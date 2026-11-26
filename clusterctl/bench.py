"""Scaling benchmark: Word Count time vs number of workers and input size.

Inputs are generated ON the master (same fixed-seed generator as the laptop),
then copied to the workers over the LAN, so no gigabytes travel through the
jump host. Worker count is chosen per run with --executor-cores C
--total-executor-cores k*C, which places exactly k executors on k workers
without stopping any service.
"""

from __future__ import annotations

import posixpath
import shlex
import statistics
from dataclasses import dataclass
from datetime import datetime

from clusterctl.cluster_ops import master_state
from clusterctl.context import Context
from clusterctl.results import write_json
from clusterctl.workload import (WorkloadError, _remote_sha256, fan_out_from_master, run_wordcount)

DEFAULT_SIZES_MB = (100, 500, 1000, 2000)
DEFAULT_REPEATS = 3
GENERATOR = "make_corpus.py"
REFERENCE_TOOLS = ("local_count.py", "wordcount.py")


@dataclass(frozen=True)
class RunPoint:
    size_mb: int
    workers: int
    cores: int
    seconds: float
    sha256: str


def corpus_name(size_mb: int) -> str:
    return f"corpus-{size_mb}mb.txt"


def _upload_tools(ctx: Context, session) -> str:
    jobs_remote = posixpath.join(ctx.inventory.cluster.data_dir, "jobs")
    for name in (GENERATOR, *REFERENCE_TOOLS):
        session.put_bytes((ctx.paths.jobs_dir / name).read_bytes(), posixpath.join(jobs_remote, name))
    return jobs_remote


def generate_on_cluster(ctx: Context, size_mb: int, *, with_reference: bool = True) -> dict:
    """Create corpus-<N>mb.txt on the master, copy it to every worker, optionally count it locally."""
    data = ctx.inventory.cluster.data_dir
    name = corpus_name(size_mb)
    remote = posixpath.join(data, "input", name)
    reference = posixpath.join(data, "output", f"reference-{name}.tsv")
    master = ctx.inventory.master
    with ctx.session(master) as s:
        jobs = _upload_tools(ctx, s)
        q_remote = shlex.quote(remote)
        expected = size_mb * 1024 * 1024
        have = int(s.run(f"stat -c %s {q_remote} 2>/dev/null || echo 0", check=False).out.strip() or 0)
        if have < expected:
            ctx.reporter.log(master.name, f"generating {name} on the master ({size_mb} MB)")
            s.run(f"python3 {shlex.quote(jobs + '/' + GENERATOR)} --out {q_remote} --size-mb {size_mb}",
                  timeout=3600)
        sha = _remote_sha256(s, remote)
        ref_sha = ""
        if with_reference:
            q_ref = shlex.quote(reference)
            if not s.run(f"test -s {q_ref}", check=False).ok:
                ctx.reporter.log(master.name, f"single-machine reference count of {name} (one core)")
                s.run(f"python3 {shlex.quote(jobs + '/local_count.py')} {q_remote} > {q_ref}.tmp && "
                      f"mv {q_ref}.tmp {q_ref}", timeout=7200)
            ref_sha = _remote_sha256(s, reference)
    fan_out_from_master(ctx, remote, sha)
    ctx.reporter.log("-", f"{name} ready on every node (sha256 {sha[:12]})", "ok")
    return {"name": name, "size_mb": size_mb, "sha256": sha, "reference_sha256": ref_sha}


def clean_generated(ctx: Context) -> None:
    """Remove generated corpora and reference counts (lab machines are shared)."""
    from clusterctl.runner import run_parallel

    data = shlex.quote(ctx.inventory.cluster.data_dir)

    def clean(host) -> str:
        with ctx.session(host) as s:
            s.run(f"rm -f {data}/input/corpus-*mb.txt {data}/output/reference-corpus-*mb.txt.tsv "
                  f"{data}/output/bench-*.tsv", check=False)
        return "generated data removed"

    run_parallel(ctx.inventory.hosts, clean, ctx.reporter, "data clean")


def speedup_table(points: list[RunPoint]) -> list[dict]:
    """Median time per (size, workers), speedup vs 1 worker, efficiency, throughput."""
    rows = []
    for size in sorted({p.size_mb for p in points}):
        by_k = {}
        for k in sorted({p.workers for p in points if p.size_mb == size}):
            times = [p.seconds for p in points if p.size_mb == size and p.workers == k]
            by_k[k] = statistics.median(times)
        base = by_k.get(min(by_k)) if by_k else None
        base_k = min(by_k) if by_k else 1
        for k, t in by_k.items():
            speedup = (base / t) * base_k if base and t else 0.0
            rows.append({"size_mb": size, "workers": k, "median_s": round(t, 2),
                         "speedup": round(speedup, 2), "efficiency": round(speedup / k, 2) if k else 0.0,
                         "throughput_mb_s": round(size / t, 1) if t else 0.0})
    return rows


def run_benchmark(ctx: Context, *, sizes_mb: tuple[int, ...] = DEFAULT_SIZES_MB,
                  repeats: int = DEFAULT_REPEATS, max_workers: int | None = None) -> dict:
    state = master_state(ctx)
    alive = [w for w in state.get("workers", []) if w.get("state") == "ALIVE"]
    if not alive:
        raise WorkloadError("no ALIVE workers")
    cores_per_worker = min(int(w.get("cores") or 1) for w in alive)
    worker_counts = list(range(1, min(len(alive), max_workers or len(alive)) + 1))
    stamp = f"{datetime.now():%Y%m%d-%H%M%S}"
    inputs = {size: generate_on_cluster(ctx, size) for size in sizes_mb}
    points: list[RunPoint] = []
    correctness: dict[int, bool] = {}
    total = len(sizes_mb) * len(worker_counts) * repeats
    n = 0
    for size in sizes_mb:
        partitions = max(3 * cores_per_worker * len(worker_counts), size // 32)
        for k in worker_counts:
            for rep in range(1, repeats + 1):
                n += 1
                ctx.reporter.info(f"[{n}/{total}] {size} MB on {k} worker(s), run {rep}/{repeats}")
                tag = f"bench-{stamp}-{size}mb-w{k}-r{rep}"
                result = run_wordcount(
                    ctx, inputs[size]["name"], tag=tag, partitions=partitions, top=5,
                    submit_opts=("--executor-cores", str(cores_per_worker),
                                 "--total-executor-cores", str(k * cores_per_worker)))
                run = _load_run(ctx, tag)
                points.append(RunPoint(size, k, k * cores_per_worker, float(run["summary"]["elapsed_s"]),
                                       run["result_sha256"]))
                ref = inputs[size]["reference_sha256"]
                if ref:
                    correctness[size] = correctness.get(size, True) and run["result_sha256"] == ref
                result.unlink(missing_ok=True)  # the counts are identical across runs; keep the JSON only
    report = {
        "stamp": stamp, "cores_per_worker": cores_per_worker, "worker_counts": worker_counts,
        "repeats": repeats, "inputs": list(inputs.values()),
        "runs": [p.__dict__ for p in points], "table": speedup_table(points),
        "correct_vs_reference": {str(k): v for k, v in correctness.items()},
    }
    out = write_json(ctx.paths.results_dir / f"bench-{stamp}.json", report)
    ctx.reporter.log("-", f"benchmark saved to {out}", "ok")
    return report


def _load_run(ctx: Context, tag: str) -> dict:
    import json

    return json.loads((ctx.paths.results_dir / f"{tag}.json").read_text(encoding="utf-8"))

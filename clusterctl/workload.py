"""Data distribution, Word Count runs, and the failure-recovery demo."""

from __future__ import annotations

import hashlib
import posixpath
import re
import shlex
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from clusterctl.cluster_ops import alive_worker_ips, fetch_json, master_state, verify
from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.results import parse_driver_log, reference_counts, write_json
from clusterctl.runner import all_ok, failed_names, run_parallel
from clusterctl.ssh import HostSession, RemoteError

JOB_FILE = "wordcount.py"
SUMMARY_RE = re.compile(r"^(WORDCOUNT_DONE|input=|total_words=|output=|top \d+:|\s+\d+\s+\S+$)")
RECOVERY_RE = re.compile(
    r"(Lost executor|lost executor|Executor lost|ExecutorLost|Removed worker|Lost task|"
    r"Resubmitted|Resubmitting|FetchFailed|has been removed|Remote RPC client disassociated|"
    r"Worker .* lost|Executor added|Granted executor|Executor updated)",
)


class WorkloadError(RuntimeError):
    """A job or the demo did not complete as expected."""


def _remote_paths(ctx: Context, input_name: str, tag: str) -> tuple[str, str, str]:
    data = ctx.inventory.cluster.data_dir
    return (
        posixpath.join(data, "jobs", JOB_FILE),
        posixpath.join(data, "input", input_name),
        posixpath.join(data, "output", f"{tag}.tsv"),
    )


def _remote_sha256(s: HostSession, path: str) -> str:
    return s.run(f"sha256sum {shlex.quote(path)} 2>/dev/null | cut -d' ' -f1", check=False).out.strip()


def push_data(ctx: Context, local_file: Path) -> str:
    """Copy an input file to the same path on every node (no HDFS needed).

    The laptop uploads once, to the master. The master then copies it to the
    workers over the cluster LAN using its own node key (trusted on workers at
    bootstrap). Any worker the master cannot reach gets a direct upload.
    """
    if not local_file.is_file():
        raise WorkloadError(f"{local_file} not found")
    remote = posixpath.join(ctx.inventory.cluster.data_dir, "input", local_file.name)
    size_mb = local_file.stat().st_size / 1024 / 1024
    local_sha = hashlib.sha256(local_file.read_bytes()).hexdigest()
    master = ctx.inventory.master

    with ctx.session(master) as s:
        if _remote_sha256(s, remote) != local_sha:
            ctx.reporter.log(master.name, f"uploading {local_file.name} ({size_mb:.1f} MB)")
            s.put_file(local_file, remote)
            if _remote_sha256(s, remote) != local_sha:
                raise WorkloadError("upload to master failed checksum")
        ctx.reporter.log(master.name, "data push: present on master", "ok")

    def fan_out(host: Host) -> str:
        with ctx.session(host) as w:
            if _remote_sha256(w, remote) == local_sha:
                return f"{local_file.name} already present"
        with ctx.session(master) as m:
            q = shlex.quote(remote)
            copied = m.run(f"scp -q -o BatchMode=yes -o ConnectTimeout=10 {q} "
                           f"{shlex.quote(host.user)}@{shlex.quote(host.ip)}:{q}",
                           check=False, timeout=3600).ok
        with ctx.session(host) as w:
            if copied and _remote_sha256(w, remote) == local_sha:
                return f"copied from master over LAN ({size_mb:.1f} MB)"
            w.put_file(local_file, remote)  # fallback: direct from laptop
            if _remote_sha256(w, remote) != local_sha:
                raise WorkloadError("checksum mismatch after upload")
        return f"uploaded directly ({size_mb:.1f} MB)"

    others = tuple(h for h in ctx.inventory.hosts if h != master)
    results = run_parallel(others, fan_out, ctx.reporter, "data push")
    if not all_ok(results):
        raise WorkloadError(f"data push failed on: {', '.join(failed_names(results))}")
    return remote


def _upload_job(ctx: Context, session: HostSession) -> str:
    job_remote, _, _ = _remote_paths(ctx, "x", "x")
    session.put_bytes((ctx.paths.jobs_dir / JOB_FILE).read_bytes(), job_remote)
    return job_remote


def submit_command(ctx: Context, input_name: str, tag: str, *, partitions: int = 0,
                   top: int = 20, delay_ms: int = 0) -> str:
    cfg = ctx.inventory.cluster
    job, remote_input, remote_output = _remote_paths(ctx, input_name, tag)
    master = f"spark://{ctx.inventory.master.ip}:{cfg.master_port}"
    args = [
        f"{cfg.spark_home}/bin/spark-submit", "--master", master, "--name", f"WordCount-{tag}",
        job, "--input", f"file://{remote_input}", "--output", remote_output,
        "--partitions", str(partitions), "--top", str(top), "--delay-ms", str(delay_ms),
        "--app-name", f"WordCount-{tag}",
    ]
    return " ".join(shlex.quote(a) for a in args)


def run_wordcount(ctx: Context, input_name: str, *, tag: str | None = None, partitions: int = 0,
                  top: int = 20, delay_ms: int = 0) -> Path:
    """Run Word Count with the driver on the master; stream its log; fetch the result."""
    tag = tag or f"wordcount-{datetime.now():%Y%m%d-%H%M%S}"
    _, _, remote_output = _remote_paths(ctx, input_name, tag)
    master = ctx.inventory.master
    with ctx.session(master) as s:
        _upload_job(ctx, s)
        cmd = submit_command(ctx, input_name, tag, partitions=partitions, top=top, delay_ms=delay_ms)
        ctx.reporter.log(master.name, f"$ {cmd}")
        captured: list[str] = []
        started = datetime.now()

        def on_line(line: str) -> None:
            captured.append(line)
            level = "ok" if SUMMARY_RE.match(line) else ("warn" if RECOVERY_RE.search(line) else "info")
            if level != "info" or " ERROR " in line or "Exception" in line:
                ctx.reporter.log(master.name, line, level)

        code = s.stream(cmd + " 2>&1", on_line)
        if code != 0:
            raise WorkloadError(f"spark-submit exited with {code}")
        local = ctx.paths.results_dir / f"{tag}.tsv"
        s.get_file(remote_output, local)
    ctx.reporter.log("-", f"result saved to {local}", "ok")
    _save_run_json(ctx, tag, input_name, started, captured, local)
    return local


def _save_run_json(ctx: Context, tag: str, input_name: str, started: datetime,
                   lines: list[str], result: Path) -> None:
    """Write results/<tag>.json for the dashboard and evidence report."""
    parsed = parse_driver_log(lines)
    data = {"tag": tag, "input": input_name, "started": started.isoformat(timespec="seconds"),
            "result_file": result.name, "result_sha256": file_digest(result), **parsed}
    reference = reference_counts(ctx.paths, input_name)
    if reference is not None:
        data["reference_file"] = reference.name
        data["matches_local_reference"] = file_digest(reference) == data["result_sha256"]
        verdict = "IDENTICAL to" if data["matches_local_reference"] else "DIFFERENT from"
        ctx.reporter.log("-", f"cluster result {verdict} single-machine count",
                         "ok" if data["matches_local_reference"] else "error")
    write_json(ctx.paths.results_dir / f"{tag}.json", data)


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ------------------------------------------------------------------ failure demo

@dataclass
class DemoTimeline:
    started: float = field(default_factory=time.time)
    events: list[tuple[float, str]] = field(default_factory=list)

    def add(self, ctx: Context, message: str, level: str = "info") -> None:
        self.events.append((time.time() - self.started, message))
        ctx.reporter.log("demo", message, level)

    def markdown(self) -> str:
        return "\n".join(f"| +{t:6.1f}s | {m} |" for t, m in self.events)


def _active_app_executors(ctx: Context, s: HostSession) -> list[dict]:
    base = f"http://{ctx.inventory.master.ip}:4040/api/v1/applications"
    apps = fetch_json(s, base)
    if not apps:
        return []
    return fetch_json(s, f"{base}/{apps[0]['id']}/executors")


def _pick_victim(ctx: Context, executors: list[dict], requested: str | None) -> Host | None:
    busy_ips = {e["hostPort"].rsplit(":", 1)[0] for e in executors
                if e.get("id") != "driver" and e.get("activeTasks", 0) > 0}
    candidates = [h for h in ctx.inventory.workers if h.ip in busy_ips and not h.is_master]
    if requested:
        candidates = [h for h in candidates if h.name == requested]
    return candidates[0] if candidates else None


def _kill(ctx: Context, victim: Host, mode: str) -> str:
    with ctx.session(victim, need_sudo=(mode == "worker")) as s:
        if mode == "worker":
            s.sudo("systemctl stop spark-worker")
            return f"stopped spark-worker (worker JVM + its executors) on {victim.name} ({victim.ip})"
        s.run("pkill -9 -f CoarseGrainedExecutorBackend", check=False)
        return f"kill -9 executor JVM on {victim.name} ({victim.ip})"


def demo_failure(ctx: Context, input_name: str, *, mode: str = "worker", victim_name: str | None = None,
                 waves: int = 4, delay_ms: int = 12000) -> Path:
    """Run Word Count, kill a busy worker mid-job, prove the job still returns the right answer."""
    if mode not in ("worker", "executor"):
        raise WorkloadError("mode must be 'worker' or 'executor'")
    tl = DemoTimeline()
    state = verify(ctx)
    total_cores = int(state.get("cores") or 1)
    partitions = max(total_cores * waves, 8)
    stamp = f"{datetime.now():%Y%m%d-%H%M%S}"

    tl.add(ctx, f"baseline: clean run on {len(alive_worker_ips(state))} workers / {total_cores} cores")
    baseline = run_wordcount(ctx, input_name, tag=f"baseline-{stamp}", partitions=partitions)

    master = ctx.inventory.master
    data = ctx.inventory.cluster.data_dir
    log_path = posixpath.join(data, "logs", f"demo-{stamp}.log")
    tag = f"failure-{stamp}"
    with ctx.session(master) as s:
        _upload_job(ctx, s)
        cmd = submit_command(ctx, input_name, tag, partitions=partitions, delay_ms=delay_ms)
        pid = s.run(f"setsid nohup {cmd} > {shlex.quote(log_path)} 2>&1 < /dev/null & echo $!").out.strip()
        tl.add(ctx, f"job started (pid {pid}, {partitions} partitions, {delay_ms} ms/partition)")

        victim = None
        deadline = time.time() + 180
        while victim is None and time.time() < deadline:
            time.sleep(2)
            try:
                victim = _pick_victim(ctx, _active_app_executors(ctx, s), victim_name)
            except (RemoteError, KeyError, IndexError, ValueError):
                continue
        if victim is None:
            raise WorkloadError("no worker started running tasks within 180s")
        tl.add(ctx, f"tasks running on {victim.name}; injecting failure")
        tl.add(ctx, _kill(ctx, victim, mode), "warn")

        seen_dead = False
        while s.run(f"kill -0 {shlex.quote(pid)}", check=False).ok:
            time.sleep(3)
            if not seen_dead and mode == "worker":
                try:
                    dead = victim.ip not in alive_worker_ips(master_state(ctx))
                except RemoteError:
                    dead = False
                if dead:
                    seen_dead = True
                    tl.add(ctx, f"master reports {victim.name} no longer ALIVE; job continues on survivors")
        log_text = s.read_text(log_path)
        recovered = ctx.paths.results_dir / f"{tag}.tsv"
        _, _, remote_output = _remote_paths(ctx, input_name, tag)
        if "WORDCOUNT_DONE" not in log_text:
            raise WorkloadError(f"job did not finish; see {log_path} on {master.name}")
        s.get_file(remote_output, recovered)
        tl.add(ctx, "job finished despite the failure", "ok")

    recovery_lines = [ln for ln in log_text.splitlines() if RECOVERY_RE.search(ln)]
    same = file_digest(baseline) == file_digest(recovered)
    tl.add(ctx, f"result identical to baseline: {same}", "ok" if same else "error")

    if mode == "worker":
        with ctx.session(victim, need_sudo=True) as v:
            v.sudo("systemctl start spark-worker")
        tl.add(ctx, f"restarted spark-worker on {victim.name}")
        verify(ctx)
        tl.add(ctx, f"{victim.name} re-registered with the master", "ok")

    local_log = ctx.paths.results_dir / f"demo-{stamp}.log"
    local_log.write_text(log_text, encoding="utf-8")
    report = ctx.paths.results_dir / f"demo-{stamp}.md"
    report.write_text(_demo_report(stamp, mode, victim, partitions, delay_ms, tl, recovery_lines,
                                   baseline, recovered, same, local_log), encoding="utf-8")
    parsed = parse_driver_log(log_text.splitlines())
    write_json(ctx.paths.results_dir / f"demo-{stamp}.json", {
        "stamp": stamp, "input": input_name, "mode": mode,
        "victim": {"name": victim.name, "ip": victim.ip},
        "partitions": partitions, "delay_ms": delay_ms, "identical": same,
        "baseline": {"file": baseline.name, "sha256": file_digest(baseline)},
        "recovered": {"file": recovered.name, "sha256": file_digest(recovered)},
        "timeline": [{"t": round(t, 1), "event": e} for t, e in tl.events],
        "lost_executors": parsed["lost_executors"], "lost_tasks": parsed["lost_tasks"],
        "retried_tasks": parsed["retried_tasks"], "executors": parsed["executors"],
        "stages": parsed["stages"], "summary": parsed["summary"],
        "key_log_lines": _key_lines(log_text), "report_file": report.name, "log_file": local_log.name,
    })
    ctx.reporter.log("-", f"demo report: {report}", "ok")
    if not same:
        raise WorkloadError("recovered result differs from baseline")
    return report


KEY_LINE_PATTERNS = (
    ("ERROR TaskSchedulerImpl: Lost executor", 2),
    ("WARN TaskSetManager: Lost task", 2),
    ("DAGScheduler: Executor lost", 2),
    ("got disassociated", 2),
    (re.compile(r"Starting task \d+\.1 in stage"), 3),
    (re.compile(r"Finished task \d+\.1 in stage"), 2),
    (re.compile(r"ShuffleMapStage 0 \(.*\) finished"), 1),
    ("Job 0 finished", 1),
    ("WORDCOUNT_DONE", 1),
    ("total_words=", 1),
)


def _key_lines(log_text: str) -> list[str]:
    """A short, ordered excerpt that tells the recovery story (for slides and the dashboard)."""
    picked: list[tuple[int, str]] = []
    lines = log_text.splitlines()
    for pattern, limit in KEY_LINE_PATTERNS:
        hits = [(i, ln) for i, ln in enumerate(lines)
                if (pattern.search(ln) if isinstance(pattern, re.Pattern) else pattern in ln)]
        picked.extend(hits[:limit])
    return [ln.strip() for _, ln in sorted(set(picked))]


def _demo_report(stamp: str, mode: str, victim: Host, partitions: int, delay_ms: int, tl: DemoTimeline,
                 recovery_lines: list[str], baseline: Path, recovered: Path, same: bool, log: Path) -> str:
    excerpt = "\n".join(recovery_lines[:60]) or "(no recovery lines matched)"
    return f"""# Failure-recovery demo {stamp}

- Failure injected: **{mode}** on **{victim.name} ({victim.ip})**
- Partitions: {partitions}, artificial delay per partition: {delay_ms} ms
- Baseline result: `{baseline.name}`  sha256 `{file_digest(baseline)[:16]}`
- Result with failure: `{recovered.name}`  sha256 `{file_digest(recovered)[:16]}`
- **Identical: {same}**
- Full driver log: `{log.name}`

## Timeline

| time | event |
|------|-------|
{tl.markdown()}

## Driver log lines showing detection and recovery

```
{excerpt}
```
"""

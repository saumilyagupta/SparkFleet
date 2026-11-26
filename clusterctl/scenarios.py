"""Advanced fault-tolerance scenarios (beyond the basic 'kill a worker' demo).

shuffle    kill a worker while the REDUCE side reads shuffle output -> its map outputs
           are gone, reduce tasks hit FetchFailed and Spark RESUBMITS the earlier
           map stage for just the missing partitions (lineage across a stage boundary).
straggler  throttle one worker to 1 CPU core (real cgroup CPU quota, not a sleep) with
           speculative execution on -> Spark launches backup copies of its slow tasks
           on healthy workers and keeps whichever finishes first.
cascade    kill one busy worker, then a second one 20 s later -> the job still finishes
           on the last surviving worker.

Every scenario checks the answer against a single-machine reference count, puts the
cluster back the way it was, and writes results/demo-<scenario>-<stamp>.{json,md}.
"""

from __future__ import annotations

import posixpath
import shlex
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from clusterctl.bench import corpus_name, generate_on_cluster
from clusterctl.cluster_ops import alive_worker_ips, fetch_json, master_state, verify
from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.results import parse_driver_log, reference_counts, sha256, write_json
from clusterctl.ssh import HostSession, RemoteError
from clusterctl.workload import (DemoTimeline, WorkloadError, _key_lines, _pick_victim, _remote_paths,
                                 _remote_sha256, _upload_job, file_digest, submit_command)

POLL_S = 2
START_TIMEOUT_S = 180
CASCADE_GAP_S = 20
SCENARIOS = ("shuffle", "straggler", "cascade")
SPECULATION_CONF = ("spark.speculation=true", "spark.speculation.multiplier=1.5",
                    "spark.speculation.quantile=0.5", "spark.speculation.interval=1s")
EXTRA_KEY_PATTERNS = ("FetchFailed", "Resubmitting", "Resubmitted", "speculatable", "Killing attempt")


@dataclass(frozen=True)
class Plan:
    """What to run and how to break it."""
    input_name: str
    partitions: int
    delay_ms: int = 0
    reduce_delay_ms: int = 0
    submit_opts: tuple[str, ...] = ()


def plan_for(scenario: str, cores: int) -> Plan:
    if scenario == "shuffle":
        return Plan("corpus.txt", partitions=2 * cores, reduce_delay_ms=4000)
    if scenario == "straggler":
        conf = tuple(x for kv in SPECULATION_CONF for x in ("--conf", kv))
        return Plan(corpus_name(2000), partitions=6 * cores, submit_opts=conf)
    if scenario == "cascade":
        return Plan("corpus.txt", partitions=4 * cores, delay_ms=8000)
    raise WorkloadError(f"unknown scenario '{scenario}'; choose one of {', '.join(SCENARIOS)}")


# ------------------------------------------------------------------ driver REST helpers

def _app_base(ctx: Context) -> str:
    return f"http://{ctx.inventory.master.ip}:4040/api/v1/applications"


def _app_id(ctx: Context, s: HostSession) -> str | None:
    apps = fetch_json(s, _app_base(ctx), timeout=15)
    return apps[0]["id"] if apps else None


def _wait_for(ctx: Context, s: HostSession, what: str, check: Callable[[str], object]) -> object:
    deadline = time.time() + START_TIMEOUT_S
    while time.time() < deadline:
        time.sleep(POLL_S)
        try:
            app = _app_id(ctx, s)
            found = check(app) if app else None
        except (RemoteError, KeyError, IndexError, ValueError):
            continue
        if found:
            return found
    raise WorkloadError(f"timed out waiting for {what}")


def _busy_victim(ctx: Context, s: HostSession, exclude: tuple[str, ...] = ()) -> Host:
    def check(app: str) -> Host | None:
        executors = fetch_json(s, f"{_app_base(ctx)}/{app}/executors", timeout=15)
        victim = _pick_victim(ctx, executors, None)
        if victim and victim.name in exclude:
            busy = [h for h in ctx.inventory.workers if h.name not in exclude and not h.is_master
                    and any(e.get("hostPort", "").startswith(h.ip + ":") and e.get("activeTasks", 0) > 0
                            for e in executors)]
            return busy[0] if busy else None
        return victim

    return _wait_for(ctx, s, "a worker running tasks", check)


def _reduce_stage_running(ctx: Context, s: HostSession) -> int:
    def check(app: str) -> int | None:
        stages = fetch_json(s, f"{_app_base(ctx)}/{app}/stages?status=active", timeout=15)
        live = [st for st in stages if st.get("stageId", 0) >= 1 and st.get("numActiveTasks", 0) > 0]
        return live[0]["stageId"] if live else None

    return _wait_for(ctx, s, "the reduce (post-shuffle) stage", check)


# ------------------------------------------------------------------ fault injection

def _stop_worker(ctx: Context, host: Host) -> None:
    with ctx.session(host, need_sudo=True) as v:
        v.sudo("systemctl stop spark-worker")


def _start_worker(ctx: Context, host: Host) -> None:
    with ctx.session(host, need_sudo=True) as v:
        v.sudo("systemctl start spark-worker")


def _throttle(ctx: Context, host: Host, quota: str) -> None:
    """Real CPU slowdown via the worker's cgroup ('' removes it). --runtime: gone after reboot."""
    with ctx.session(host, need_sudo=True) as v:
        v.sudo(f"systemctl set-property --runtime spark-worker CPUQuota={shlex.quote(quota)}"
               if quota else "systemctl set-property --runtime spark-worker CPUQuota=")


# ------------------------------------------------------------------ runner

def _ensure_input(ctx: Context, plan: Plan) -> str:
    """Return the sha256 the result must match (single-machine reference)."""
    local_ref = reference_counts(ctx.paths, plan.input_name)
    if local_ref is not None:
        return sha256(local_ref)
    info = generate_on_cluster(ctx, int(plan.input_name.split("-")[1].removesuffix("mb.txt")))
    return info["reference_sha256"]


def run_scenario(ctx: Context, scenario: str) -> dict:
    state = verify(ctx)
    cores = int(state.get("cores") or 1)
    plan = plan_for(scenario, cores)
    expected_sha = _ensure_input(ctx, plan)
    stamp = f"{datetime.now():%Y%m%d-%H%M%S}"
    tag = f"{scenario}-{stamp}"
    tl = DemoTimeline()
    victims: list[Host] = []
    throttled: list[Host] = []
    master = ctx.inventory.master
    log_path = posixpath.join(ctx.inventory.cluster.data_dir, "logs", f"demo-{tag}.log")
    job_opts = ("--reduce-delay-ms", str(plan.reduce_delay_ms)) if plan.reduce_delay_ms else ()
    tl.add(ctx, f"scenario '{scenario}': {len(alive_worker_ips(state))} workers / {cores} cores, "
                f"input {plan.input_name}, {plan.partitions} partitions")
    try:
        with ctx.session(master) as s:
            _upload_job(ctx, s)
            cmd = submit_command(ctx, plan.input_name, tag, partitions=plan.partitions, delay_ms=plan.delay_ms,
                                 submit_opts=plan.submit_opts, job_opts=job_opts)
            pid = s.run(f"setsid nohup {cmd} > {shlex.quote(log_path)} 2>&1 < /dev/null & echo $!").out.strip()
            tl.add(ctx, f"job started (pid {pid})")
            _inject(ctx, s, scenario, tl, victims, throttled)
            while s.run(f"kill -0 {shlex.quote(pid)}", check=False).ok:
                time.sleep(3)
            log_text = s.read_text(log_path)
            if "WORDCOUNT_DONE" not in log_text:
                raise WorkloadError(f"job did not finish; see {log_path} on {master.name}")
            _, _, remote_output = _remote_paths(ctx, plan.input_name, tag)
            result_sha = _remote_sha256(s, remote_output)
            tl.add(ctx, "job finished despite the fault", "ok")
    finally:
        for host in throttled:
            _throttle(ctx, host, "")
            tl.add(ctx, f"CPU throttle removed from {host.name}")
        for host in victims:
            _start_worker(ctx, host)
            tl.add(ctx, f"restarted spark-worker on {host.name}")
    verify(ctx)
    tl.add(ctx, "all workers ALIVE again", "ok")
    identical = result_sha == expected_sha
    tl.add(ctx, f"result identical to single-machine count: {identical}", "ok" if identical else "error")
    return _save(ctx, scenario, stamp, plan, tl, victims or throttled, log_text, result_sha, expected_sha)


def _inject(ctx: Context, s: HostSession, scenario: str, tl: DemoTimeline,
            victims: list[Host], throttled: list[Host]) -> None:
    if scenario == "shuffle":
        stage = _reduce_stage_running(ctx, s)
        victim = _busy_victim(ctx, s)
        tl.add(ctx, f"map stage done, reduce stage {stage} reading shuffle data; killing {victim.name}")
        _stop_worker(ctx, victim)
        victims.append(victim)
        tl.add(ctx, f"stopped spark-worker on {victim.name}: its map outputs are now lost", "warn")
    elif scenario == "straggler":
        victim = _busy_victim(ctx, s)
        _throttle(ctx, victim, "100%")
        throttled.append(victim)
        tl.add(ctx, f"{victim.name} throttled to 1 CPU core (cgroup CPUQuota=100%): it is now a straggler",
               "warn")
    elif scenario == "cascade":
        first = _busy_victim(ctx, s)
        _stop_worker(ctx, first)
        victims.append(first)
        tl.add(ctx, f"stopped spark-worker on {first.name} (first failure)", "warn")
        time.sleep(CASCADE_GAP_S)
        second = _busy_victim(ctx, s, exclude=(first.name,))
        _stop_worker(ctx, second)
        victims.append(second)
        tl.add(ctx, f"stopped spark-worker on {second.name} (second failure, {CASCADE_GAP_S}s later)", "warn")


def _save(ctx: Context, scenario: str, stamp: str, plan: Plan, tl: DemoTimeline, affected: list[Host],
          log_text: str, result_sha: str, expected_sha: str) -> dict:
    parsed = parse_driver_log(log_text.splitlines())
    key_lines = _key_lines(log_text)
    extra = [ln.strip() for ln in log_text.splitlines() if any(p in ln for p in EXTRA_KEY_PATTERNS)][:12]
    base = f"demo-{scenario}-{stamp}"
    log_file = ctx.paths.results_dir / f"{base}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text(log_text, encoding="utf-8")
    victim = affected[0] if affected else None
    data = {
        "scenario": scenario, "stamp": stamp, "input": plan.input_name, "mode": scenario,
        "victim": {"name": victim.name, "ip": victim.ip} if victim else {},
        "affected": [{"name": h.name, "ip": h.ip} for h in affected],
        "partitions": plan.partitions, "delay_ms": plan.delay_ms, "reduce_delay_ms": plan.reduce_delay_ms,
        "identical": result_sha == expected_sha,
        "baseline": {"file": "single-machine reference", "sha256": expected_sha},
        "recovered": {"file": f"{scenario}-{stamp}.tsv (on master)", "sha256": result_sha},
        "timeline": [{"t": round(t, 1), "event": e} for t, e in tl.events],
        **{k: parsed[k] for k in ("lost_executors", "lost_tasks", "retried_tasks", "executors", "stages",
                                  "summary", "fetch_failed", "resubmitted", "resubmit_count",
                                  "speculatable", "speculative_wins")},
        "key_log_lines": sorted(set(key_lines + extra), key=lambda ln: ln[:17])[:30],
        "report_file": f"{base}.md", "log_file": log_file.name,
    }
    write_json(ctx.paths.results_dir / f"{base}.json", data)
    (ctx.paths.results_dir / f"{base}.md").write_text(_markdown(data), encoding="utf-8")
    ctx.reporter.log("-", f"scenario report: results/{base}.md", "ok")
    if not data["identical"]:
        raise WorkloadError("result differs from the single-machine reference")
    return data


def _markdown(d: dict) -> str:
    rows = "\n".join(f"| +{t['t']:6.1f}s | {t['event']} |" for t in d["timeline"])
    lines = "\n".join(d["key_log_lines"])
    return f"""# Fault scenario: {d['scenario']} ({d['stamp']})

- Affected: {', '.join(f"{a['name']} ({a['ip']})" for a in d['affected']) or '-'}
- Input: {d['input']}, {d['partitions']} partitions
- Lost tasks: {d['lost_tasks']}, retried: {d['retried_tasks']}, FetchFailed: {d['fetch_failed']},
  stages resubmitted: {d['resubmit_count']}, speculatable: {d['speculatable']},
  speculative copies that won: {d['speculative_wins']}
- **Result identical to single-machine count: {d['identical']}** (sha256 {d['recovered']['sha256'][:16]})

| time | event |
|------|-------|
{rows}

```
{lines}
```
"""

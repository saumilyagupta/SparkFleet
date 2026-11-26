"""Spark master high availability: ZooKeeper ensemble + standby master + failover demo.

Off unless cluster.ha_enabled is true in inventory.yaml. Turning it off again and
re-running provision removes ZooKeeper and the standby master (provision removes
managed services a host no longer needs).
"""

from __future__ import annotations

import posixpath
import shlex
import threading
import time
from datetime import datetime

from clusterctl.cluster_ops import fetch_json, verify
from clusterctl.context import Context
from clusterctl.inventory import ClusterConfig, Host, Inventory
from clusterctl.provision import _node_download, _remote_sha512, render_values
from clusterctl.render import render
from clusterctl.results import parse_driver_log, reference_counts, sha256, write_json
from clusterctl.spark_dist import parse_sha512, _fetch
from clusterctl.ssh import HostSession, RemoteError
from clusterctl.workload import DemoTimeline, WorkloadError, _key_lines, _remote_paths, _remote_sha256, \
    _upload_job, run_wordcount, submit_command

ZK_MIRRORS = ("https://downloads.apache.org/zookeeper", "https://archive.apache.org/dist/zookeeper")
FAILOVER_TIMEOUT_S = 240
_release_lock = threading.Lock()
_release_cache: dict[str, tuple[list[str], str]] = {}


# ------------------------------------------------------------------ install

def zk_tarball(cfg: ClusterConfig) -> str:
    return f"apache-zookeeper-{cfg.zookeeper_version}-bin.tar.gz"


def zk_release(cfg: ClusterConfig) -> tuple[list[str], str]:
    """(download URLs, official sha512), fetched once per process."""
    with _release_lock:
        if cfg.zookeeper_version in _release_cache:
            return _release_cache[cfg.zookeeper_version]
        name = zk_tarball(cfg)
        urls = [f"{m}/zookeeper-{cfg.zookeeper_version}/{name}" for m in ZK_MIRRORS]
        errors = []
        for url in urls:
            try:
                sha = parse_sha512(_fetch(url + ".sha512").decode("utf-8", "replace"), name)
            except Exception as exc:  # noqa: BLE001 - try the next mirror
                errors.append(f"{url}: {exc}")
                continue
            _release_cache[cfg.zookeeper_version] = (urls, sha)
            return urls, sha
        raise RemoteError("could not fetch the ZooKeeper checksum: " + "; ".join(errors))


def myid_for(inventory: Inventory, host: Host) -> int:
    for i, z in enumerate(inventory.zookeepers, start=1):
        if z.name == host.name:
            return i
    raise ValueError(f"{host.name} is not a ZooKeeper host")


def install_zookeeper(session: HostSession, inventory: Inventory) -> str:
    """Download (on the node), verify SHA-512, unpack, write zoo.cfg + myid. Idempotent."""
    cfg = inventory.cluster
    urls, sha = zk_release(cfg)
    target = f"{cfg.install_dir}/apache-zookeeper-{cfg.zookeeper_version}-bin"
    home = f"{cfg.install_dir}/zookeeper"
    data = f"{cfg.data_dir}/zookeeper"
    user = shlex.quote(session.host.user)
    status = "zookeeper already installed"
    if not session.run(f"test -x {shlex.quote(target)}/bin/zkServer.sh", check=False).ok:
        tgz = f"/tmp/{zk_tarball(cfg)}"
        if _remote_sha512(session, tgz) != sha:
            if not _node_download(session, urls, tgz) or _remote_sha512(session, tgz) != sha:
                raise RemoteError("ZooKeeper download failed or SHA-512 mismatch")
        session.sudo(f"tar -xzf {shlex.quote(tgz)} -C {shlex.quote(cfg.install_dir)} && rm -f {shlex.quote(tgz)}")
        status = f"installed ZooKeeper {cfg.zookeeper_version} (SHA-512 ok)"
    session.sudo(f"ln -sfn {shlex.quote(target)} {shlex.quote(home)} && chown -R {user}: {shlex.quote(target)} "
                 f"&& mkdir -p {shlex.quote(data)}/logs && chown -R {user}: {shlex.quote(data)}")
    values = render_values(inventory, session.host)
    changed = session.sudo_write(f"{home}/conf/zoo.cfg", render("zoo.cfg", values), owner=session.host.user)
    changed |= session.sudo_write(f"{data}/myid", f"{myid_for(inventory, session.host)}\n",
                                  owner=session.host.user)
    return status + ("; config updated" if changed else "")


# ------------------------------------------------------------------ state

def master_states(ctx: Context) -> list[dict]:
    """Status of every master: [{name, ip, status: ALIVE|STANDBY|RECOVERING|DOWN, ...}]."""
    out = []
    for host in ctx.inventory.masters:
        url = f"http://{host.ip}:{ctx.inventory.cluster.master_ui_port}/json/"
        try:
            with ctx.session(host) as s:
                state = fetch_json(s, url, timeout=15)
            out.append({"name": host.name, "ip": host.ip, "status": state.get("status", "?"),
                        "alive_workers": sum(1 for w in state.get("workers", []) if w.get("state") == "ALIVE"),
                        "active_apps": [a.get("name") for a in state.get("activeapps", [])]})
        except Exception as exc:  # noqa: BLE001 - a dead master is a valid answer
            out.append({"name": host.name, "ip": host.ip, "status": "DOWN", "error": str(exc)[:160]})
    return out


def zookeeper_status(ctx: Context) -> list[dict]:
    out = []
    for host in ctx.inventory.zookeepers:
        try:
            with ctx.session(host) as s:
                home = f"{ctx.inventory.cluster.install_dir}/zookeeper"
                mode = s.run(f"{home}/bin/zkServer.sh status {home}/conf/zoo.cfg 2>&1 | grep -i '^mode'",
                             check=False, timeout=20).out.strip()
            out.append({"name": host.name, "ip": host.ip, "mode": mode.split(":")[-1].strip() or "unknown"})
        except Exception as exc:  # noqa: BLE001
            out.append({"name": host.name, "ip": host.ip, "mode": "DOWN", "error": str(exc)[:160]})
    return out


# ------------------------------------------------------------------ failover demo

def _wait_status(ctx: Context, host: Host, wanted: str, timeout_s: int) -> float:
    start = time.time()
    while time.time() - start < timeout_s:
        state = next((m for m in master_states(ctx) if m["name"] == host.name), {})
        if state.get("status") == wanted:
            return time.time() - start
        time.sleep(3)
    raise WorkloadError(f"{host.name} did not become {wanted} within {timeout_s}s")


def demo_master_failover(ctx: Context, input_name: str = "corpus.txt") -> dict:
    inv = ctx.inventory
    if not inv.cluster.ha_enabled or not inv.standbys:
        raise WorkloadError("master HA is not enabled (cluster.ha_enabled: true + provision first)")
    tl = DemoTimeline()
    states = {m["name"]: m["status"] for m in master_states(ctx)}
    alive = [h for h in inv.masters if states.get(h.name) == "ALIVE"]
    waiting = [h for h in inv.masters if states.get(h.name) == "STANDBY"]
    if len(alive) != 1 or not waiting:
        raise WorkloadError(f"need one ALIVE and one STANDBY master before the demo (now: {states})")
    # Kill whichever master ZooKeeper elected; the other one must take over. (Names kept from
    # the original API: 'primary' = the master we kill, 'standby' = the one that takes over.)
    primary, standby = alive[0], waiting[0]
    tl.add(ctx, f"masters: {primary.name}=ALIVE (will be killed), {standby.name}=STANDBY")
    reference = reference_counts(ctx.paths, input_name)
    if reference is None:
        raise WorkloadError(f"{input_name} must exist locally for the reference count")
    stamp = f"{datetime.now():%Y%m%d-%H%M%S}"
    tag = f"master-failover-{stamp}"
    cores = int(verify(ctx).get("cores") or 1)
    log_path = posixpath.join(inv.cluster.data_dir, "logs", f"demo-{tag}.log")
    expected = sha256(reference)
    # 12 waves x 10 s = 2+ minutes: longer than the ~40-90 s ZooKeeper takes to elect the standby,
    # so the job is genuinely still running when the new master takes over.
    partitions, delay_ms = 12 * cores, 10_000
    stopped_primary, pid, finished = False, "", False
    from clusterctl.scenarios import _kill_job, _no_other_app_running

    _no_other_app_running(ctx)
    try:
        with ctx.session(inv.master) as s:  # driver always on system-01
            _upload_job(ctx, s)
            cmd = submit_command(ctx, input_name, tag, partitions=partitions, delay_ms=delay_ms)
            pid = s.run(f"setsid nohup {cmd} > {shlex.quote(log_path)} 2>&1 < /dev/null & echo $!").out.strip()
            tl.add(ctx, f"job started with master URL {inv.spark_master_url} (pid {pid}, {partitions} partitions)")
            time.sleep(15)
            stopped_primary = True
            with ctx.session(primary, need_sudo=True) as p:
                p.sudo("systemctl stop spark-master")
            tl.add(ctx, f"stopped spark-master on {primary.name}: the active master is DOWN", "warn")
            took = _wait_status(ctx, standby, "ALIVE", FAILOVER_TIMEOUT_S)
            running = s.run(f"kill -0 {shlex.quote(pid)}", check=False).ok
            tl.add(ctx, f"ZooKeeper elected {standby.name}: standby master ALIVE after {took:.0f}s"
                        + ("; the job is still running" if running else "; WARNING: the job had already ended"),
                   "ok" if running else "warn")
            deadline = time.time() + FAILOVER_TIMEOUT_S * 5
            while s.run(f"kill -0 {shlex.quote(pid)}", check=False).ok:
                if time.time() > deadline:
                    s.run(f"pkill -TERM -s {shlex.quote(pid)} || kill {shlex.quote(pid)}", check=False)
                    raise WorkloadError("failover job did not finish in time; stopped it")
                time.sleep(3)
            finished = True
            log_text = s.read_text(log_path)
            if "WORDCOUNT_DONE" not in log_text:
                raise WorkloadError(f"job did not finish; see {log_path} on {inv.master.name}")
            _, _, remote_output = _remote_paths(ctx, input_name, tag)
            result_sha = _remote_sha256(s, remote_output)
            if running:
                tl.add(ctx, "the running job finished under the new master", "ok")
        # Strongest proof the new master works: a brand-new application is accepted by it.
        after = run_wordcount(ctx, input_name, tag=f"after-failover-{stamp}", partitions=cores)
        new_app_ok = sha256(after) == expected
        tl.add(ctx, f"new application submitted after failover ran on {standby.name}: "
                    f"result identical={new_app_ok}", "ok" if new_app_ok else "error")
    finally:
        if pid and not finished:
            _kill_job(ctx, pid)  # never leave the demo driver holding the cluster's cores
        if stopped_primary:  # never leave the primary master stopped, whatever happened above
            try:
                with ctx.session(primary, need_sudo=True) as p:
                    p.sudo("systemctl start spark-master")
            except Exception as exc:  # noqa: BLE001
                ctx.reporter.log("demo", f"RESTORE FAILED: could not restart {primary.name}: {exc}", "error")
    try:
        _wait_status(ctx, primary, "STANDBY", 120)
        tl.add(ctx, f"{primary.name} restarted and rejoined as STANDBY", "ok")
    except WorkloadError as exc:  # restarted already; do not lose the report over a slow rejoin
        tl.add(ctx, f"{primary.name} restarted but not yet STANDBY: {exc}", "warn")
    identical = result_sha == expected and new_app_ok
    tl.add(ctx, f"results identical to single-machine count: {identical}", "ok" if identical else "error")
    return _save(ctx, stamp, tag, primary, standby, tl, log_text, result_sha, expected, input_name)


def _save(ctx: Context, stamp: str, tag: str, primary: Host, standby: Host, tl: DemoTimeline, log_text: str,
          result_sha: str, expected: str, input_name: str) -> dict:
    parsed = parse_driver_log(log_text.splitlines())
    master_lines = [ln.strip() for ln in log_text.splitlines()
                    if any(k in ln for k in ("Master has changed", "Connecting to master", "master",
                                             "Master removed", "disconnected from"))][:12]
    base = f"demo-master-failover-{stamp}"
    (ctx.paths.results_dir / f"{base}.log").write_text(log_text, encoding="utf-8")
    data = {
        "scenario": "master-failover", "stamp": stamp, "input": input_name, "mode": "master-failover",
        "victim": {"name": primary.name, "ip": primary.ip}, "new_master": {"name": standby.name, "ip": standby.ip},
        "affected": [{"name": primary.name, "ip": primary.ip}], "identical": result_sha == expected,
        "baseline": {"file": "single-machine reference", "sha256": expected},
        "recovered": {"file": f"{tag}.tsv (on master)", "sha256": result_sha},
        "timeline": [{"t": round(t, 1), "event": e} for t, e in tl.events],
        **{k: parsed[k] for k in ("lost_executors", "lost_tasks", "retried_tasks", "executors", "stages",
                                  "summary")},
        "key_log_lines": (master_lines + _key_lines(log_text))[:30],
        "report_file": f"{base}.md", "log_file": f"{base}.log",
    }
    write_json(ctx.paths.results_dir / f"{base}.json", data)
    rows = "\n".join(f"| +{t['t']:6.1f}s | {t['event']} |" for t in data["timeline"])
    (ctx.paths.results_dir / f"{base}.md").write_text(
        f"# Master failover ({stamp})\n\n- Killed: active master {primary.name}; new master: {standby.name}\n"
        f"- **Result identical: {data['identical']}**\n\n| time | event |\n|---|---|\n{rows}\n\n```\n"
        + "\n".join(data["key_log_lines"]) + "\n```\n", encoding="utf-8")
    ctx.reporter.log("-", f"failover report: results/{base}.md", "ok")
    if not data["identical"]:
        raise WorkloadError("result differs from the single-machine reference")
    return data

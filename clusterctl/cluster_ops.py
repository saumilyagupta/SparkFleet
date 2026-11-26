"""Start/stop services and read cluster state."""

from __future__ import annotations

import json
import shlex
import time
from dataclasses import dataclass

from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.provision import java_version, services_for
from clusterctl.runner import HostResult, run_parallel
from clusterctl.ssh import HostSession, RemoteError

VERIFY_TIMEOUT_S = 120
POLL_S = 3

# Executed on the master with its own python3, so the laptop needs no route to port 8080.
_FETCH_JSON = (
    "import json,sys,urllib.request\n"
    "url=sys.argv[1]\n"
    "try:\n"
    "    print(urllib.request.urlopen(url, timeout=5).read().decode())\n"
    "except Exception as e:\n"
    "    print(json.dumps({'_error': str(e)}))\n"
)


def fetch_json(session: HostSession, url: str) -> dict | list:
    out = session.run(f"python3 -c {shlex.quote(_FETCH_JSON)} {shlex.quote(url)}").out
    data = json.loads(out or "{}")
    if isinstance(data, dict) and "_error" in data:
        raise RemoteError(f"GET {url}: {data['_error']}")
    return data


def master_url(ctx: Context) -> str:
    cfg = ctx.inventory.cluster
    return f"http://{ctx.inventory.master.ip}:{cfg.master_ui_port}/json/"


def master_state(ctx: Context) -> dict:
    with ctx.session(ctx.inventory.master) as s:
        return fetch_json(s, master_url(ctx))


def _systemctl(ctx: Context, hosts: tuple[Host, ...], action: str, label: str) -> list[HostResult]:
    def act(host: Host) -> str:
        services = services_for(ctx.inventory, host)
        with ctx.session(host, need_sudo=True) as s:
            s.sudo(" && ".join(f"systemctl {action} {svc}" for svc in services))
        return f"{action} {', '.join(services)}"

    return run_parallel(hosts, act, ctx.reporter, label)


def start(ctx: Context, hosts: tuple[Host, ...]) -> list[HostResult]:
    """Master first, then workers, so workers register immediately."""
    master = ctx.inventory.master
    results = []
    if master in hosts:
        results += _systemctl(ctx, (master,), "start", "start")
        time.sleep(POLL_S)
    others = tuple(h for h in hosts if h != master)
    return results + _systemctl(ctx, others, "start", "start")


def stop(ctx: Context, hosts: tuple[Host, ...]) -> list[HostResult]:
    master = ctx.inventory.master
    others = tuple(h for h in hosts if h != master)
    results = _systemctl(ctx, others, "stop", "stop")
    if master in hosts:
        results += _systemctl(ctx, (master,), "stop", "stop")
    return results


def restart(ctx: Context, hosts: tuple[Host, ...]) -> list[HostResult]:
    return stop(ctx, hosts) + start(ctx, hosts)


def alive_worker_ips(state: dict) -> set[str]:
    return {w.get("host") for w in state.get("workers", []) if w.get("state") == "ALIVE"}


def verify(ctx: Context, timeout_s: int = VERIFY_TIMEOUT_S) -> dict:
    """Wait until every expected worker is ALIVE on the master."""
    expected = {h.ip for h in ctx.inventory.workers}
    deadline = time.time() + timeout_s
    last_err = ""
    while time.time() < deadline:
        try:
            state = master_state(ctx)
            missing = expected - alive_worker_ips(state)
            if not missing:
                ctx.reporter.log("-", f"master ALIVE, {len(expected)}/{len(expected)} workers ALIVE "
                                      f"({state.get('cores')} cores, {state.get('memory')} MB)", "ok")
                return state
            last_err = f"waiting for workers: {', '.join(sorted(missing))}"
        except RemoteError as exc:
            last_err = str(exc)
        ctx.reporter.info(last_err)
        time.sleep(POLL_S)
    raise RemoteError(f"cluster not healthy after {timeout_s}s: {last_err}")


@dataclass(frozen=True)
class HostStatus:
    name: str
    ip: str
    role: str
    reachable: bool
    java: str = "-"
    spark: str = "-"
    services: dict | None = None
    error: str = ""

    def to_dict(self) -> dict:
        return {**self.__dict__, "services": self.services or {}}


def host_status(ctx: Context, host: Host) -> HostStatus:
    cfg = ctx.inventory.cluster
    try:
        with ctx.session(host) as s:
            java = java_version(s)
            spark = s.run(f"head -1 {shlex.quote(cfg.spark_home)}/RELEASE 2>/dev/null || echo missing",
                          check=False).out.strip()
            svc = {name: s.run(f"systemctl is-active {name}", check=False).out.strip() or "unknown"
                   for name in services_for(ctx.inventory, host)}
        return HostStatus(host.name, host.ip, host.role, True, java, spark, svc)
    except Exception as exc:  # noqa: BLE001 - report unreachable hosts instead of failing
        return HostStatus(host.name, host.ip, host.role, False, error=str(exc))


def status(ctx: Context) -> dict:
    results = run_parallel(ctx.inventory.hosts, lambda h: host_status(ctx, h), _Silent(), "status")
    hosts = [r.value.to_dict() for r in results if r.ok]
    try:
        state = master_state(ctx)
        alive = alive_worker_ips(state)
        master = {"status": state.get("status"), "url": state.get("url"),
                  "alive_workers": sorted(alive), "cores": state.get("cores"),
                  "memory_mb": state.get("memory"),
                  "active_apps": [a.get("name") for a in state.get("activeapps", [])],
                  "completed_apps": len(state.get("completedapps", []))}
    except Exception as exc:  # noqa: BLE001
        alive, master = set(), {"status": "UNREACHABLE", "error": str(exc)}
    for h in hosts:
        h["worker_alive"] = h["ip"] in alive
    return {"hosts": hosts, "master": master}


class _Silent:
    def log(self, *args, **kwargs) -> None:  # noqa: D401 - status is printed by the caller
        pass

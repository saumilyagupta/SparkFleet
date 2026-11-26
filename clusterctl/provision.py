"""Idempotent provisioning steps. Each step is safe to re-run."""

from __future__ import annotations

import shlex
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from clusterctl.context import Context
from clusterctl.inventory import ClusterConfig, Host, Inventory
from clusterctl.render import render
from clusterctl.runner import HostResult, all_ok, failed_names, run_parallel
from clusterctl.spark_dist import ensure_tarball, release_info, tarball_name
from clusterctl.ssh import HostSession, RemoteError

MIN_DISK_KB = 2 * 1024 * 1024
HOSTS_BEGIN = "# BEGIN clusterctl managed block"
HOSTS_END = "# END clusterctl managed block"
DATA_SUBDIRS = ("input", "jobs", "output", "events", "logs", "work")
APT = "DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 -y -qq"


class ProvisionError(RuntimeError):
    """A provisioning phase failed on one or more hosts."""


@dataclass(frozen=True)
class Facts:
    os_id: str
    os_version: str
    arch: str
    python: str


# ---------------------------------------------------------------- preflight

def preflight(session: HostSession, inventory: Inventory) -> Facts:
    session.sudo("true")  # proves the vault password works for sudo
    os_line = session.run(". /etc/os-release && echo \"$ID|$VERSION_ID|$ID_LIKE\"").out.strip()
    os_id, os_version, os_like = (os_line.split("|") + ["", "", ""])[:3]
    if "debian" not in f"{os_id} {os_like}" and os_id != "debian":
        raise RemoteError(f"unsupported OS '{os_id}': clusterctl targets Ubuntu/Debian (apt)")
    arch = session.run("uname -m").out.strip()
    free_kb = int(session.run("df -Pk / | awk 'NR==2 {print $4}'").out.strip() or 0)
    if free_kb < MIN_DISK_KB:
        raise RemoteError(f"only {free_kb // 1024} MB free on /; need at least 2 GB")
    peers = [h.ip for h in inventory.hosts if h.ip != session.host.ip]
    if peers:
        probe = " ".join(shlex.quote(ip) for ip in peers)
        out = session.run(
            f"for ip in {probe}; do timeout 3 bash -c \"</dev/tcp/$ip/22\" 2>/dev/null "
            "&& echo \"$ip ok\" || echo \"$ip FAIL\"; done"
        ).out
        bad = [line.split()[0] for line in out.splitlines() if line.endswith("FAIL")]
        if bad:
            raise RemoteError(f"cannot reach peers on the LAN: {', '.join(bad)}")
    python = session.run("python3 -c 'import sys; print(\"%d.%d\" % sys.version_info[:2])'",
                         check=False).out.strip() or "missing"
    return Facts(os_id, os_version, arch, python)


# ---------------------------------------------------------------- packages

def install_packages(session: HostSession, cfg: ClusterConfig) -> str:
    java_ok = session.run(
        "command -v java >/dev/null && java -version 2>&1 | grep -Eq 'version \"(11|17|21)'",
        check=False,
    ).ok
    have_py = session.run(f"command -v {shlex.quote(cfg.python_bin)}", check=False).ok
    if java_ok and have_py:
        return "java + python already installed"
    session.sudo(f"{APT} update", timeout=1800)
    candidates = " ".join(shlex.quote(p) for p in cfg.java_packages)
    pick = session.run(
        f"for p in {candidates}; do c=$(apt-cache policy \"$p\" 2>/dev/null | awk '/Candidate:/ {{print $2}}'); "
        "if [ -n \"$c\" ] && [ \"$c\" != '(none)' ]; then echo \"$p\"; break; fi; done"
    ).out.strip()
    if not pick:
        raise RemoteError(f"none of {cfg.java_packages} available from apt")
    session.sudo(f"{APT} install {shlex.quote(pick)} python3 procps", timeout=1800)
    return f"installed {pick} + python3"


def java_version(session: HostSession) -> str:
    out = session.run("java -version 2>&1 | head -1", check=False).out.strip()
    return out or "missing"


def check_python_consistency(facts: dict[str, Facts]) -> str:
    """PySpark requires the same Python minor version on driver and executors."""
    versions = {name: f.python for name, f in facts.items()}
    distinct = set(versions.values())
    if "missing" in distinct:
        missing = [n for n, v in versions.items() if v == "missing"]
        raise ProvisionError(f"python3 missing on: {', '.join(missing)}")
    if len(distinct) > 1:
        detail = ", ".join(f"{n}={v}" for n, v in sorted(versions.items()))
        raise ProvisionError(
            f"Python versions differ ({detail}). PySpark needs one minor version everywhere: install the "
            "same python3.X on all nodes and set cluster.python_bin in inventory.yaml (e.g. python3.10)."
        )
    major_minor = tuple(int(x) for x in distinct.pop().split("."))
    if major_minor < (3, 8):
        raise ProvisionError("Spark 3.5 needs Python >= 3.8")
    return ".".join(map(str, major_minor))


# ---------------------------------------------------------------- /etc/hosts

def hosts_block(inventory: Inventory) -> str:
    lines = [HOSTS_BEGIN]
    lines += [f"{h.ip}\t{h.name}" + ("\tspark-master" if h.is_master else "") for h in inventory.hosts]
    lines.append(HOSTS_END)
    return "\n".join(lines)


def merge_hosts_file(current: str, block: str) -> str:
    out, skipping = [], False
    for line in current.splitlines():
        if line.strip() == HOSTS_BEGIN:
            skipping = True
            continue
        if line.strip() == HOSTS_END:
            skipping = False
            continue
        if not skipping:
            out.append(line)
    while out and not out[-1].strip():
        out.pop()
    return "\n".join(out + ["", block]) + "\n"


def update_hosts_file(session: HostSession, inventory: Inventory) -> str:
    current = session.read_text("/etc/hosts")
    changed = session.sudo_write("/etc/hosts", merge_hosts_file(current, hosts_block(inventory)))
    return "/etc/hosts updated" if changed else "/etc/hosts unchanged"


# ---------------------------------------------------------------- spark install

def _remote_sha512(session: HostSession, path: str) -> str:
    return session.run(f"sha512sum {shlex.quote(path)} 2>/dev/null | cut -d' ' -f1", check=False).out.strip()


def _node_download(session: HostSession, urls: list[str], dest: str) -> bool:
    """Let the node fetch Spark itself (much faster than pushing through the laptop)."""
    for url in urls:
        q_url, q_dest = shlex.quote(url), shlex.quote(dest)
        cmd = (f"if command -v curl >/dev/null; then curl -fsSL --retry 2 -o {q_dest} {q_url}; "
               f"elif command -v wget >/dev/null; then wget -q -O {q_dest} {q_url}; "
               f"else python3 -c 'import sys,urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])' "
               f"{q_url} {q_dest}; fi")
        if session.run(cmd, check=False, timeout=3600).ok:
            return True
    return False


def install_spark(session: HostSession, cfg: ClusterConfig, urls: list[str], sha512: str,
                  local_tarball: Callable[[], Path]) -> str:
    """Install Spark. Node downloads it directly; falls back to an upload from the laptop.

    Either way the archive must match the official SHA-512 before it is unpacked.
    """
    target = f"{cfg.install_dir}/{cfg.spark_dirname}"
    user = shlex.quote(session.host.user)
    data_dirs = " ".join(shlex.quote(f"{cfg.data_dir}/{d}") for d in DATA_SUBDIRS)
    installed = session.run(f"test -f {shlex.quote(target)}/RELEASE", check=False).ok
    status = "spark already installed"
    if not installed:
        remote_tgz = f"/tmp/{tarball_name(cfg)}"
        source = "cached"
        if _remote_sha512(session, remote_tgz) != sha512:
            source = "downloaded on node"
            if not (_node_download(session, urls, remote_tgz) and _remote_sha512(session, remote_tgz) == sha512):
                source = "pushed from laptop"
                session.put_file(local_tarball(), remote_tgz)
            if _remote_sha512(session, remote_tgz) != sha512:
                raise RemoteError("Spark tarball failed SHA-512 check")
        session.sudo(f"tar -xzf {shlex.quote(remote_tgz)} -C {shlex.quote(cfg.install_dir)} "
                     f"&& rm -f {shlex.quote(remote_tgz)}")
        status = f"installed {cfg.spark_dirname} ({source}, SHA-512 ok)"
    session.sudo(
        f"ln -sfn {shlex.quote(target)} {shlex.quote(cfg.spark_home)} && "
        f"chown -R {user}: {shlex.quote(target)} && mkdir -p {data_dirs} && "
        f"chown -R {user}: {shlex.quote(cfg.data_dir)}"
    )
    return status


def render_values(inventory: Inventory, host: Host) -> dict[str, object]:
    cfg = inventory.cluster
    resources = []
    if host.cores:
        resources.append(f"export SPARK_WORKER_CORES={host.cores}")
    if host.memory:
        resources.append(f"export SPARK_WORKER_MEMORY={host.memory}")
    return {
        "host_name": host.name, "host_ip": host.ip, "user": host.user,
        "master_ip": inventory.master.ip, "spark_home": cfg.spark_home,
        "data_dir": cfg.data_dir, "python_bin": cfg.python_bin,
        "master_port": cfg.master_port, "master_ui_port": cfg.master_ui_port,
        "worker_ui_port": cfg.worker_ui_port, "driver_port": cfg.driver_port,
        "block_manager_port": cfg.block_manager_port,
        "worker_resources": "\n".join(resources),
    }


def configure_spark(session: HostSession, inventory: Inventory) -> bool:
    values = render_values(inventory, session.host)
    conf = f"{inventory.cluster.spark_home}/conf"
    owner = session.host.user
    changed = session.sudo_write(f"{conf}/spark-env.sh", render("spark-env.sh", values), mode="755", owner=owner)
    changed |= session.sudo_write(f"{conf}/spark-defaults.conf", render("spark-defaults.conf", values), owner=owner)
    return changed


# ---------------------------------------------------------------- systemd + firewall

def services_for(inventory: Inventory, host: Host) -> tuple[str, ...]:
    names = []
    if host.is_master:
        names.append("spark-master")
    if host in inventory.workers:
        names.append("spark-worker")
    return tuple(names)


def install_services(session: HostSession, inventory: Inventory, config_changed: bool) -> str:
    values = render_values(inventory, session.host)
    services = services_for(inventory, session.host)
    changed = config_changed
    for svc in services:
        changed |= session.sudo_write(f"/etc/systemd/system/{svc}.service", render(f"{svc}.service", values))
    session.sudo("systemctl daemon-reload && systemctl enable " + " ".join(services) + " >/dev/null 2>&1")
    if changed:
        session.sudo(" ; ".join(f"systemctl try-restart {s}" for s in services))
    return f"services {', '.join(services)} enabled" + (" (restarted)" if changed else "")


def configure_firewall(session: HostSession, inventory: Inventory) -> str:
    active = session.sudo("ufw status 2>/dev/null | grep -q 'Status: active' && echo yes || echo no",
                          check=False).out.strip()
    if active != "yes":
        return "ufw inactive; nothing to open"
    peers = [h.ip for h in inventory.hosts if h.ip != session.host.ip]
    cmds = [f"ufw allow from {shlex.quote(ip)} >/dev/null" for ip in peers]
    # Admin access to the web UIs from wherever we SSH in from (laptop or jump host).
    cmds.append("src=${SSH_CLIENT%% *}; [ -n \"$src\" ] && for p in 8080 8081 4040:4049; do "
                "ufw allow proto tcp from \"$src\" to any port $p >/dev/null; done")
    session.sudo(" ; ".join(cmds), check=False)
    return f"ufw: allowed {len(peers)} peers + admin UI ports"


# ---------------------------------------------------------------- orchestration

def _phase(ctx: Context, hosts: tuple[Host, ...], label: str, fn) -> list[HostResult]:
    results = run_parallel(hosts, fn, ctx.reporter, label)
    if not all_ok(results):
        raise ProvisionError(f"{label} failed on: {', '.join(failed_names(results))}")
    return results


def provision(ctx: Context, hosts: tuple[Host, ...]) -> None:
    inventory = ctx.inventory
    cfg = inventory.cluster
    inventory.master  # fail fast if no master

    def sudo_session(host: Host) -> HostSession:
        return ctx.session(host, need_sudo=True)

    def do_preflight(host: Host) -> Facts:
        with sudo_session(host) as s:
            return preflight(s, inventory)

    _phase(ctx, hosts, "preflight", do_preflight)

    def do_packages(host: Host) -> str:
        with sudo_session(host) as s:
            return install_packages(s, cfg)

    _phase(ctx, hosts, "packages", do_packages)

    # Python must match across the WHOLE cluster, not only the hosts being provisioned.
    def python_of(host: Host) -> Facts:
        with ctx.session(host) as s:
            v = s.run(f"{shlex.quote(cfg.python_bin)} -c 'import sys; print(\"%d.%d\" % sys.version_info[:2])'",
                      check=False).out.strip()
            return Facts("", "", "", v or "missing")

    facts = _phase(ctx, inventory.hosts, "python check", python_of)
    version = check_python_consistency({r.host: r.value for r in facts})
    ctx.reporter.info(f"python {version} on every node")

    urls, sha512 = release_info(cfg)
    ctx.reporter.info(f"Spark {cfg.spark_version} SHA-512 {sha512[:16]}... (from apache.org)")
    tarball_lock = threading.Lock()

    def local_tarball() -> Path:
        with tarball_lock:  # download once on the laptop, only if some node needs it
            return ensure_tarball(cfg, ctx.paths.cache_dir, ctx.reporter)[0]

    def do_install(host: Host) -> str:
        with sudo_session(host) as s:
            spark = install_spark(s, cfg, urls, sha512, local_tarball)
            changed = configure_spark(s, inventory)
            svc = install_services(s, inventory, changed)
            return f"{spark}; {svc}"

    _phase(ctx, hosts, "spark install", do_install)
    refresh_shared_config(ctx)


def refresh_shared_config(ctx: Context) -> None:
    """/etc/hosts and firewall reference every node, so refresh them cluster-wide."""

    def do_refresh(host: Host) -> str:
        with ctx.session(host, need_sudo=True) as s:
            return f"{update_hosts_file(s, ctx.inventory)}; {configure_firewall(s, ctx.inventory)}"

    _phase(ctx, ctx.inventory.hosts, "hosts+firewall", do_refresh)

"""First contact with each machine: pin host key, install control key, create node key.

After bootstrap every connection uses the control key; passwords are only
needed again for sudo.
"""

from __future__ import annotations

import shlex

from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.keys import fingerprint, generate_control_key
from clusterctl.runner import HostResult, run_parallel
from clusterctl.ssh import HostSession

NODE_KEY_PATH = "~/.ssh/id_ed25519"


def authorize_key(session: HostSession, public_key: str) -> bool:
    """Append a public key to ~/.ssh/authorized_keys if absent. Returns True if added."""
    q = shlex.quote(public_key.strip())
    result = session.run(
        "umask 077; mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys && "
        f"if grep -qxF {q} ~/.ssh/authorized_keys; then echo present; "
        f"else printf '%s\\n' {q} >> ~/.ssh/authorized_keys && echo added; fi"
    )
    return result.out.strip() == "added"


def remove_key(session: HostSession, public_key: str) -> bool:
    """Remove every authorized_keys line containing this key blob."""
    blob = public_key.split()[1]
    q = shlex.quote(blob)
    result = session.run(
        "f=~/.ssh/authorized_keys; [ -f \"$f\" ] || { echo absent; exit 0; }; "
        f"if grep -qF {q} \"$f\"; then grep -vF {q} \"$f\" > \"$f.tmp\"; "
        "chmod 600 \"$f.tmp\"; mv \"$f.tmp\" \"$f\"; echo removed; else echo absent; fi"
    )
    return result.out.strip() == "removed"


def ensure_node_key(session: HostSession) -> str:
    """Create ~/.ssh/id_ed25519 on the node if missing (private key never leaves it)."""
    session.run(
        f"umask 077; mkdir -p ~/.ssh; test -f {NODE_KEY_PATH} || "
        f"ssh-keygen -q -t ed25519 -N '' -C \"clusterctl-node@$(hostname)\" -f {NODE_KEY_PATH}"
    )
    return session.run(f"cat {NODE_KEY_PATH}.pub").out.strip()


def _bootstrap_one(ctx: Context, host: Host, control_pub: str) -> str:
    registry = ctx.registry
    control_fp = fingerprint(control_pub)
    with ctx.password_session(host) as session:
        added = authorize_key(session, control_pub)
        registry.mark_installed(control_fp, host.name)
        node_pub = ensure_node_key(session)
    registry.register(public_key=node_pub, kind="node", owner=host.name, user=host.user, ip=host.ip)
    # Prove key auth works before declaring success.
    with ctx.session(host) as session:
        session.run("true")
    return "control key " + ("installed" if added else "already present") + ", key login verified"


def _trust_master_on_workers(ctx: Context, hosts: tuple[Host, ...]) -> list[HostResult]:
    """Let the master SSH to workers (needed only for Spark's own start-all.sh)."""
    master = ctx.inventory.master
    master_rec = next((r for r in ctx.registry.records()
                       if r.owner == master.name and r.kind == "node" and r.status == "active"), None)
    if master_rec is None:
        return []
    targets = [h for h in hosts if h.name != master.name]

    def install(host: Host) -> str:
        with ctx.session(host) as session:
            authorize_key(session, master_rec.public_key)
        ctx.registry.mark_installed(master_rec.fingerprint, host.name)
        return "master key authorized"

    results = run_parallel(targets, install, ctx.reporter, "master->worker trust")
    worker_ips = " ".join(shlex.quote(h.ip) for h in ctx.inventory.hosts if h.name != master.name)
    if worker_ips:
        with ctx.session(master) as session:
            session.run(
                "mkdir -p ~/.ssh && touch ~/.ssh/known_hosts && "
                f"for ip in {worker_ips}; do ssh-keygen -F \"$ip\" >/dev/null || "
                "ssh-keyscan -T 5 -t ed25519 \"$ip\" >> ~/.ssh/known_hosts 2>/dev/null; done",
                check=False,
            )
    return results


def bootstrap(ctx: Context, hosts: tuple[Host, ...]) -> list[HostResult]:
    control_pub = generate_control_key(ctx.paths.control_key, ctx.paths.control_pub)
    ctx.registry.register(public_key=control_pub, kind="control", owner="laptop", user="-", ip="-")
    results = run_parallel(hosts, lambda h: _bootstrap_one(ctx, h, control_pub), ctx.reporter, "bootstrap")
    ok_hosts = tuple(h for h, r in zip(hosts, results) if r.ok)
    try:
        ctx.inventory.master
    except Exception:  # noqa: BLE001 - no master yet is fine during partial bootstrap
        return results
    if ok_hosts:
        _trust_master_on_workers(ctx, ok_hosts)
    return results


def revoke(ctx: Context, fp: str) -> list[str]:
    """Remove a key from every host it was installed on and mark it revoked."""
    record = ctx.registry.find(fp)
    if record is None:
        raise ValueError(f"no key with fingerprint {fp}")
    if record.kind == "control":
        raise ValueError("refusing to revoke the control key: you would lose access to every host")
    targets = tuple(h for h in ctx.inventory.hosts if h.name in record.installed_on)
    results = run_parallel(
        targets,
        lambda h: _remove_on(ctx, h, record.public_key),
        ctx.reporter, "revoke",
    )
    removed = [r.host for r in results if r.ok]
    ctx.registry.mark_revoked(fp, removed)
    return removed


def _remove_on(ctx: Context, host: Host, public_key: str) -> str:
    with ctx.session(host) as session:
        return "removed" if remove_key(session, public_key) else "was not present"

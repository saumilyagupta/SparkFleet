"""Local port forwarding so the laptop browser can open the Spark web UIs.

Equivalent to `ssh -L` but through clusterctl's own connection (and jump host):
    localhost:8080 -> master UI,  localhost:4040 -> running job UI,
    localhost:8081, 8082, ... -> each worker UI
"""

from __future__ import annotations

import select
import socket
import socketserver
import threading
from dataclasses import dataclass

import paramiko

from clusterctl.context import Context
from clusterctl.inventory import Host

BUFFER = 32768


@dataclass(frozen=True)
class Forward:
    local_port: int
    remote_ip: str
    remote_port: int
    label: str


def plan_forwards(ctx: Context, base_worker_port: int = 8081) -> list[Forward]:
    cfg = ctx.inventory.cluster
    master = ctx.inventory.master
    forwards = [
        Forward(cfg.master_ui_port, master.ip, cfg.master_ui_port, f"master UI ({master.name})"),
        Forward(4040, master.ip, 4040, "running application UI (driver on master)"),
    ]
    for i, worker in enumerate(ctx.inventory.workers):
        forwards.append(Forward(base_worker_port + i, worker.ip, cfg.worker_ui_port, f"worker UI ({worker.name})"))
    return forwards


def _pipe(sock: socket.socket, chan: paramiko.Channel) -> None:
    try:
        while True:
            readable, _, _ = select.select([sock, chan], [], [], 60)
            if sock in readable:
                data = sock.recv(BUFFER)
                if not data:
                    break
                chan.sendall(data)
            if chan in readable:
                data = chan.recv(BUFFER)
                if not data:
                    break
                sock.sendall(data)
    finally:
        chan.close()
        sock.close()


def _handler_for(transport_for, fwd: Forward):
    class Handler(socketserver.BaseRequestHandler):
        def handle(self) -> None:
            try:
                transport = transport_for()
                chan = transport.open_channel("direct-tcpip", (fwd.remote_ip, fwd.remote_port),
                                              self.request.getpeername())
            except Exception:  # noqa: BLE001 - target not listening (e.g. no job running on 4040)
                self.request.close()
                return
            _pipe(self.request, chan)

    return Handler


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def open_tunnels(ctx: Context, master: Host | None = None) -> list[tuple[Forward, _Server]]:
    """Start forwarding servers on 127.0.0.1. Caller keeps the process alive."""
    master = master or ctx.inventory.master
    lock = threading.Lock()
    holder: dict[str, object] = {}

    def transport_for() -> paramiko.Transport:
        with lock:
            session = holder.get("s")
            transport = session.client.get_transport() if session else None
            if transport is None or not transport.is_active():
                session = ctx.session(master)
                holder["s"] = session
                transport = session.client.get_transport()
            return transport

    servers = []
    for fwd in plan_forwards(ctx):
        try:
            server = _Server(("127.0.0.1", fwd.local_port), _handler_for(transport_for, fwd))
        except OSError as exc:
            ctx.reporter.log("-", f"skip localhost:{fwd.local_port} ({fwd.label}): {exc}", "warn")
            continue
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append((fwd, server))
        ctx.reporter.log("-", f"http://localhost:{fwd.local_port}  ->  {fwd.label}", "ok")
    return servers

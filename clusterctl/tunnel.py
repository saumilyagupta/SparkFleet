"""Local port forwarding so the laptop browser can open the Spark web UIs.

Equivalent to `ssh -L` but through clusterctl's own connection (and jump host).
Local ports are offset by 10000 so they don't collide with common local
services (e.g. a Postgres/Apache install already listening on 8080):
    localhost:18080 -> master UI,  localhost:14040 -> running job UI,
    localhost:18081, 18082, ... -> each worker UI
If a local port is busy, the next free one is used and printed.
"""

from __future__ import annotations

import logging
import select
import socket
import socketserver
import threading
from dataclasses import dataclass

import paramiko

from clusterctl.context import Context
from clusterctl.inventory import Host

BUFFER = 32768
LOCAL_OFFSET = 10000
PORT_SEARCH = 20
JOB_UI_PORT = 4040

_UNAVAILABLE = (
    "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n\r\n"
    "<html><body style='font-family:sans-serif;padding:2em'>"
    "<h2>{label} is not available right now</h2>"
    "<p>The tunnel works, but nothing is listening on {target}.</p>"
    "<p>{hint}</p></body></html>"
)


@dataclass(frozen=True)
class Forward:
    local_port: int
    remote_ip: str
    remote_port: int
    label: str
    hint: str = "Check the service with: python -m clusterctl status"


def plan_forwards(ctx: Context) -> list[Forward]:
    cfg = ctx.inventory.cluster
    master = ctx.inventory.master
    forwards = [
        Forward(cfg.master_ui_port + LOCAL_OFFSET, master.ip, cfg.master_ui_port, f"master UI ({master.name})"),
        Forward(JOB_UI_PORT + LOCAL_OFFSET, master.ip, JOB_UI_PORT, "running job UI (driver on master)",
                "This page only exists while a Spark job is running. Start one with "
                "'python -m clusterctl run --input corpus.txt' or the failure demo, then refresh."),
    ]
    for i, worker in enumerate(ctx.inventory.workers):
        forwards.append(Forward(cfg.worker_ui_port + LOCAL_OFFSET + i, worker.ip, cfg.worker_ui_port,
                                f"worker UI ({worker.name})"))
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
            except Exception:  # noqa: BLE001 - target not listening: explain instead of a blank error
                page = _UNAVAILABLE.format(label=fwd.label, target=f"{fwd.remote_ip}:{fwd.remote_port}",
                                           hint=fwd.hint)
                try:
                    self.request.settimeout(2)
                    self.request.recv(BUFFER)  # consume the request so the reply isn't reset
                    self.request.sendall(page.encode("utf-8"))
                    self.request.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                finally:
                    self.request.close()
                return
            _pipe(self.request, chan)

    return Handler


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = False  # never share a port another program already owns


def _bind(port: int, handler) -> _Server | None:
    for candidate in range(port, port + PORT_SEARCH):
        try:
            return _Server(("127.0.0.1", candidate), handler)
        except OSError:
            continue
    return None


def open_tunnels(ctx: Context, master: Host | None = None) -> list[tuple[Forward, _Server]]:
    """Start forwarding servers on 127.0.0.1. Caller keeps the process alive."""
    logging.getLogger("paramiko").setLevel(logging.CRITICAL)  # failures are shown as a web page
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

    transport_for()  # fail fast if the master is unreachable
    servers = []
    for fwd in plan_forwards(ctx):
        server = _bind(fwd.local_port, _handler_for(transport_for, fwd))
        if server is None:
            ctx.reporter.log("-", f"no free local port near {fwd.local_port} for {fwd.label}", "warn")
            continue
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append((fwd, server))
        moved = f"  ({fwd.local_port} was busy)" if port != fwd.local_port else ""
        ctx.reporter.log("-", f"http://localhost:{port:<6} ->  {fwd.label}{moved}", "ok")
    return servers

"""FastAPI app for the local clusterctl dashboard.

Security model: binds 127.0.0.1 only; every /api route needs the per-launch
random token (X-Token header; the SSE stream alone may pass it as ?token=
because EventSource cannot set headers). Passwords are write-only: they go
into the encrypted vault and no endpoint ever returns one.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import secrets
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from clusterctl import bootstrap as boot
from clusterctl import cluster_ops
from clusterctl import inventory as inv
from clusterctl.context import Context, VaultLockedError
from clusterctl.dashboard.state import DashboardState
from clusterctl.dashboard.actions import Action, ActionFailed, default_actions
from clusterctl.dashboard.jobs import JobBusyError, sse_events
from clusterctl.paths import ProjectPaths
from clusterctl.runner import QueueReporter
from clusterctl.systems_import import parse_systems
from clusterctl.vault import Vault, VaultError

log = logging.getLogger("clusterctl.dashboard")
STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_UPLOAD_BYTES = 1024 * 1024
JUMP_ALIAS = r"^[A-Za-z0-9._-]{1,64}$"
SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'self'; img-src 'self' data:; object-src 'none'; "
                               "base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}


# ------------------------------------------------------------------ request models

class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PasswordIn(_Body):
    password: str = Field(min_length=1, max_length=1024)


class UnlockIn(PasswordIn):
    create: bool = False


class HostIn(_Body):
    name: str = Field(max_length=63)
    ip: str = Field(max_length=64)
    user: str = Field(max_length=32)
    role: str = "worker"
    password: str | None = Field(default=None, max_length=1024)


class RevokeIn(_Body):
    fingerprint: str = Field(pattern=r"^SHA256:[A-Za-z0-9+/]{20,64}$")


# ------------------------------------------------------------------ errors

def _install_error_handlers(app: FastAPI) -> None:
    def err(status: int, message: str) -> JSONResponse:
        return JSONResponse({"error": message}, status_code=status)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_req: Request, exc: StarletteHTTPException) -> JSONResponse:
        return err(exc.status_code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def validation_error(_req: Request, exc: RequestValidationError) -> JSONResponse:
        # Only field locations + messages: pydantic's "input" could echo a password.
        parts = [".".join(str(x) for x in e.get("loc", ()) if x != "body") + ": " + e.get("msg", "invalid")
                 for e in exc.errors()]
        return err(422, "; ".join(parts) or "invalid request")

    handlers = {inv.InventoryError: 400, VaultError: 400, ValueError: 400,
                VaultLockedError: 409, JobBusyError: 409}
    for exc_type, status in handlers.items():
        app.add_exception_handler(exc_type, lambda _r, e, s=status: err(s, str(e)))

    @app.exception_handler(Exception)
    async def internal_error(_req: Request, exc: Exception) -> JSONResponse:
        log.error("unhandled %s: %s", type(exc).__name__, exc)
        return err(500, "internal error")


# ------------------------------------------------------------------ routes: vault + hosts

def _vault_routes(r: APIRouter, st: DashboardState) -> None:
    @r.get("/vault")
    def vault_info() -> dict:
        vault = st.vault
        return {"exists": st.paths.vault.exists(), "unlocked": vault is not None,
                "names": list(vault.names()) if vault else []}

    @r.post("/vault/unlock")
    def vault_unlock(body: UnlockIn) -> dict:
        if not st.paths.vault.exists():
            if not body.create:
                raise HTTPException(404, "no vault yet; unlock with create=true to make one")
            st.set_vault(Vault.create(st.paths.vault, body.password))
        else:
            try:
                st.set_vault(Vault.open(st.paths.vault, body.password))
            except VaultError as exc:
                raise HTTPException(400, str(exc)) from None
        return vault_info()

    @r.post("/vault/lock")
    def vault_lock() -> dict:
        st.set_vault(None)
        return vault_info()


def _host_routes(r: APIRouter, st: DashboardState) -> None:
    @r.get("/inventory")
    def get_inventory() -> dict:
        inventory = st.load_inventory()
        vault = st.vault
        return {
            "exists": st.paths.inventory.exists(),
            "jump_host": inventory.cluster.jump_host,
            "cluster": inventory.cluster.to_dict(),
            "hosts": [{**h.to_dict(), "has_password": (h.name in vault.names()) if vault else None}
                      for h in inventory.hosts],
        }

    @r.post("/hosts", status_code=201)
    def add_host(body: HostIn) -> dict:
        vault = st.require_vault() if body.password else None
        host = inv.Host(name=body.name.strip(), ip=body.ip.strip(), user=body.user.strip(),
                        role=body.role.strip().lower())
        inventory = st.load_inventory().with_host(host)   # validates
        inv.save(inventory, st.paths.inventory)
        if vault is not None:
            updated = vault.with_secret(host.name, body.password)
            updated.save()
            st.set_vault(updated)
        return {"host": host.to_dict(), "password_stored": vault is not None}

    @r.delete("/hosts/{name}")
    def delete_host(name: str) -> dict:
        inventory = st.load_inventory()
        try:
            inventory.get(name)
        except inv.InventoryError:
            raise HTTPException(404, f"unknown host '{name}'") from None
        inv.save(inventory.without_host(name), st.paths.inventory)
        return {"removed": name}

    @r.post("/hosts/{name}/password")
    def set_password(name: str, body: PasswordIn) -> dict:
        vault = st.require_vault()
        try:
            st.load_inventory().get(name)
        except inv.InventoryError:
            raise HTTPException(404, f"unknown host '{name}'") from None
        updated = vault.with_secret(name, body.password)
        updated.save()
        st.set_vault(updated)
        return {"name": name, "password_stored": True}

    @r.post("/import-systems")
    async def import_systems(file: UploadFile = File(...), master: str = Form(""),
                             jump: str = Form("")) -> dict:
        raw = await file.read(MAX_UPLOAD_BYTES + 1)
        if len(raw) > MAX_UPLOAD_BYTES:
            raise HTTPException(413, "systems file too large")
        try:
            data = json.loads(raw.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise HTTPException(400, "systems file is not valid JSON") from None
        return await run_in_threadpool(_import_parsed, st, data, master.strip(), jump.strip())


def _import_parsed(st: DashboardState, data: object, master: str, jump: str) -> dict:
    """Mirrors cli._import_systems, but from an in-memory upload (never written to disk)."""
    base = st.load_inventory().cluster
    if jump:
        if jump.lower() == "direct":
            base = dataclasses.replace(base, jump_host=None)
        elif not re.match(JUMP_ALIAS, jump):
            raise HTTPException(400, "jump must be an ~/.ssh/config alias (letters, digits, . _ -)")
        else:
            base = dataclasses.replace(base, jump_host=jump)
    result = parse_systems(data, master=master or None, cluster=base)
    vault = st.require_vault() if result.passwords else st.vault
    inv.save(result.inventory, st.paths.inventory)
    if result.passwords:
        for name, password in result.passwords.items():
            vault = vault.with_secret(name, password)
        vault.save()
        st.set_vault(vault)
    return {"hosts": [h.to_dict() for h in result.inventory.hosts],
            "master": result.inventory.master.name, "passwords_stored": len(result.passwords)}


# ------------------------------------------------------------------ routes: ops

def _ops_routes(r: APIRouter, st: DashboardState) -> None:
    @r.get("/status")
    def get_status() -> dict:
        ctx = st.context()
        try:
            return cluster_ops.status(ctx)
        except Exception as exc:  # noqa: BLE001 - surface as a gateway error, not a 500 traceback
            raise HTTPException(502, f"status failed: {exc}") from None

    @r.get("/keys")
    def get_keys() -> dict:
        return {"keys": [rec.to_dict() for rec in st.registry().records()]}

    @r.get("/audit")
    def get_audit(limit: int = Query(200, ge=1, le=500)) -> dict:
        return {"entries": st.registry().audit_entries(limit)}

    @r.post("/keys/revoke", status_code=202)
    def revoke_key(body: RevokeIn) -> dict:
        record = st.registry().find(body.fingerprint)
        if record is None:
            raise HTTPException(404, "no key with that fingerprint")
        if record.kind == "control":
            raise HTTPException(400, "refusing to revoke the control key")
        ctx = st.context()
        job = st.jobs.start("revoke", lambda rep: boot.revoke(ctx.with_reporter(rep), body.fingerprint))
        return {"job_id": job.id}

    @r.post("/actions/{name}", status_code=202)
    async def run_action(name: str, request: Request) -> dict:
        raw = await request.body()
        try:
            params_raw = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            raise HTTPException(400, "body must be JSON") from None
        if not isinstance(params_raw, dict):
            raise HTTPException(400, "body must be a JSON object")
        return await run_in_threadpool(_start_action, st, name, params_raw)

    @r.get("/jobs")
    def list_jobs() -> dict:
        return {"jobs": [{k: v for k, v in j.snapshot().items() if k != "history"}
                         for j in st.jobs.recent()]}

    @r.get("/jobs/{job_id}")
    def get_job(job_id: str, since: int = Query(0, ge=0)) -> dict:
        job = st.jobs.get(job_id)
        if job is None:
            raise HTTPException(404, "unknown job")
        return job.snapshot(since)

    @r.get("/tunnel")
    def tunnel_info() -> dict:
        return {"open": bool(st.tunnels),
                "forwards": [{"local_port": f.local_port, "label": f.label} for f, _ in st.tunnels]}

    @r.post("/tunnel/close")
    def tunnel_close() -> dict:
        servers, st.tunnels = st.tunnels, []
        for _fwd, server in servers:
            server.shutdown()
            server.server_close()
        return tunnel_info()


def _start_action(st: DashboardState, name: str, params_raw: dict) -> dict:
    if name == "tunnel":
        return _start_tunnel(st)
    action = st.actions.get(name)
    if action is None:
        raise HTTPException(404, f"unknown action '{name}'")
    try:
        params = action.params.model_validate(params_raw)
    except ValidationError as exc:
        msgs = "; ".join(".".join(map(str, e["loc"])) + ": " + e["msg"] for e in exc.errors())
        raise HTTPException(422, msgs) from None
    if st.jobs.running() is not None:
        raise JobBusyError(f"job '{st.jobs.running().name}' is still running")
    ctx = st.context(need_vault=action.needs_vault)
    ctx.inventory.select(getattr(params, "limit", None))     # unknown host -> 400 now, not later
    job = st.jobs.start(name, lambda rep: action.fn(ctx.with_reporter(rep), params))
    return {"job_id": job.id}


def _start_tunnel(st: DashboardState) -> dict:
    if st.tunnels:
        raise HTTPException(409, "tunnels are already open")
    opener = st.tunnel_opener
    if opener is None:
        from clusterctl.tunnel import open_tunnels as opener
    ctx = st.context()

    def run(rep: QueueReporter) -> None:
        servers = opener(ctx.with_reporter(rep))
        if not servers:
            raise ActionFailed("no tunnel could be opened (ports busy?)")
        st.tunnels = list(servers)
        rep.log("-", "tunnels stay open until you close them or stop the dashboard", "info")

    return {"job_id": st.jobs.start("tunnel", run).id}


# ------------------------------------------------------------------ app factory

def create_app(paths: ProjectPaths, token: str, *, actions: dict[str, Action] | None = None,
               auto_unlock: bool = False, tunnel_opener: Callable[[Context], list] | None = None) -> FastAPI:
    if len(token) < 16:
        raise ValueError("dashboard token too short")
    st = DashboardState(paths, actions if actions is not None else default_actions(), tunnel_opener)
    if auto_unlock:
        print(st.auto_unlock())

    def token_ok(given: str | None) -> bool:
        return bool(given) and secrets.compare_digest(given.encode(), token.encode())

    def require_token(request: Request) -> None:
        if not token_ok(request.headers.get("x-token")):
            raise HTTPException(401, "missing or invalid token")

    app = FastAPI(title="clusterctl dashboard", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.dashboard = st
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "testserver"])

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        for key, value in SECURITY_HEADERS.items():
            response.headers.setdefault(key, value)
        return response

    _install_error_handlers(app)

    @app.get("/api/jobs/{job_id}/stream")
    async def stream_job(job_id: str, request: Request, token_q: str = Query("", alias="token"),
                         since: int = Query(0, ge=0)) -> StreamingResponse:
        if not (token_ok(token_q) or token_ok(request.headers.get("x-token"))):
            raise HTTPException(401, "missing or invalid token")
        job = st.jobs.get(job_id)
        if job is None:
            raise HTTPException(404, "unknown job")
        last = request.headers.get("last-event-id", "")
        start = max(since, int(last) if last.isdigit() else 0)
        return StreamingResponse(sse_events(job, request.is_disconnected, start),
                                 media_type="text/event-stream",
                                 headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    router = APIRouter(prefix="/api", dependencies=[Depends(require_token)])
    _vault_routes(router, st)
    _host_routes(router, st)
    _ops_routes(router, st)
    app.include_router(router)

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


def serve(paths: ProjectPaths, host: str = "127.0.0.1", port: int = 8765) -> int:
    import uvicorn

    if host not in ("127.0.0.1", "localhost"):
        raise ValueError("the dashboard only binds to 127.0.0.1")
    token = secrets.token_urlsafe(32)
    app = create_app(paths, token, auto_unlock=True)
    print(f"clusterctl dashboard: http://127.0.0.1:{port}/?token={token}")
    print("Ctrl+C to stop")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
    return 0

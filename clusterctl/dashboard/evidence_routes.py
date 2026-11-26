"""Evidence view routes: node facts, saved results, job history and generated reports.

The JSON routes hang off the token-protected /api router. The two file routes
(report + raw result files) also accept ?token= so they can open in a new tab,
exactly like the SSE stream route.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

from clusterctl import cluster_ops, facts, results
from clusterctl.dashboard.state import DashboardState

MAX_LISTED = 10
REPORT_NAME = re.compile(r"^evidence-[\w.-]+\.html$")
RAW_FILE_NAME = re.compile(r"^(demo-[\w.-]+\.(md|log)|[\w.-]+\.tsv)$")
# Reports are static documents: no scripts, no network, only their own inline CSS/SVG.
REPORT_CSP = ("default-src 'none'; style-src 'unsafe-inline'; img-src data:; font-src data:; "
              "base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox allow-modals")
RAW_CSP = "default-src 'none'; frame-ancestors 'none'; sandbox"


def evidence_api_routes(r: APIRouter, st: DashboardState) -> None:
    """JSON routes; `r` is the /api router that already enforces the X-Token header."""

    @r.get("/evidence/facts")
    async def get_facts() -> dict:
        ctx = st.context()
        try:
            nodes = await run_in_threadpool(facts.collect_facts, ctx)
        except Exception as exc:  # noqa: BLE001 - SSH/jump failures surface as a gateway error
            raise HTTPException(502, f"facts failed: {exc}") from None
        return {"nodes": nodes}

    @r.get("/evidence/results")
    def get_results() -> dict:
        index = results.results_index(st.paths)
        return {**index, "wordcount": index["wordcount"][:MAX_LISTED],
                "demos": index["demos"][:MAX_LISTED], "bench": index["bench"][:MAX_LISTED]}

    @r.get("/monitor")
    def get_monitor() -> dict:
        try:
            return st.sampler().snapshot()
        except Exception as exc:  # noqa: BLE001 - e.g. inventory missing or key not bootstrapped
            raise HTTPException(502, f"monitor failed: {exc}") from None

    @r.get("/evidence/jobs")
    async def get_jobs() -> dict:
        ctx = st.context()
        try:
            return await run_in_threadpool(cluster_ops.job_history, ctx)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(502, f"job history failed: {exc}") from None


def _resolve(st: DashboardState, name: str, pattern: re.Pattern[str]) -> Path:
    if not pattern.match(name):
        raise HTTPException(404, "not found")
    try:
        return results.safe_result_file(st.paths, name)
    except (ValueError, FileNotFoundError):
        raise HTTPException(404, "not found") from None


def evidence_file_routes(app: FastAPI, st: DashboardState, token_ok: Callable[[str | None], bool]) -> None:
    """File routes that accept the token as a header OR as ?token= (new-tab links)."""

    def check(request: Request, token_q: str) -> None:
        if not (token_ok(token_q) or token_ok(request.headers.get("x-token"))):
            raise HTTPException(401, "missing or invalid token")

    @app.get("/api/evidence/report/{name}")
    def get_report(name: str, request: Request, token_q: str = Query("", alias="token")) -> FileResponse:
        check(request, token_q)
        path = _resolve(st, name, REPORT_NAME)
        return FileResponse(path, media_type="text/html; charset=utf-8",
                            headers={"Content-Security-Policy": REPORT_CSP,
                                     "Content-Disposition": "inline"})

    @app.get("/api/evidence/file/{name}")
    def get_file(name: str, request: Request, token_q: str = Query("", alias="token")) -> FileResponse:
        check(request, token_q)
        path = _resolve(st, name, RAW_FILE_NAME)
        return FileResponse(path, media_type="text/plain; charset=utf-8",
                            headers={"Content-Security-Policy": RAW_CSP,
                                     "Content-Disposition": "inline"})

"""Background jobs for the dashboard: one long-running cluster action at a time."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable

from clusterctl.runner import QueueReporter

log = logging.getLogger("clusterctl.dashboard")

MAX_KEPT_JOBS = 50
SSE_POLL_S = 0.3
SSE_HEARTBEAT_S = 15.0


class JobBusyError(RuntimeError):
    """Another job is still running."""


@dataclass
class Job:
    id: str
    name: str
    reporter: QueueReporter
    started: float = field(default_factory=time.time)
    state: str = "running"          # running | done | failed
    error: str = ""
    finished: float | None = None

    def snapshot(self, since: int = 0) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "state": self.state,
            "error": self.error,
            "started": self.started,
            "finished": self.finished,
            "history": list(self.reporter.history[since:]),
        }


class JobManager:
    """Runs a callable on a worker thread with its own QueueReporter."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}

    def running(self) -> Job | None:
        with self._lock:
            return next((j for j in self._jobs.values() if j.state == "running"), None)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def recent(self) -> list[Job]:
        with self._lock:
            return sorted(self._jobs.values(), key=lambda j: j.started, reverse=True)

    def start(self, name: str, fn: Callable[[QueueReporter], None]) -> Job:
        with self._lock:
            busy = next((j for j in self._jobs.values() if j.state == "running"), None)
            if busy is not None:
                raise JobBusyError(f"job '{busy.name}' ({busy.id}) is still running")
            job = Job(id=secrets.token_hex(6), name=name, reporter=QueueReporter())
            self._jobs[job.id] = job
            self._prune()
        threading.Thread(target=self._run, args=(job, fn), name=f"job-{name}", daemon=True).start()
        return job

    def _prune(self) -> None:
        finished = sorted((j for j in self._jobs.values() if j.state != "running"),
                          key=lambda j: j.started)
        for old in finished[:max(0, len(self._jobs) - MAX_KEPT_JOBS)]:
            del self._jobs[old.id]

    @staticmethod
    def _run(job: Job, fn: Callable[[QueueReporter], None]) -> None:
        job.reporter.log("-", f"job {job.name} started", "info")
        try:
            fn(job.reporter)
        except Exception as exc:  # noqa: BLE001 - job boundary: record and report, never crash the server
            log.warning("job %s (%s) failed: %s: %s", job.name, job.id, type(exc).__name__, exc)
            job.error = f"{type(exc).__name__}: {exc}"
            job.reporter.log("-", f"job {job.name} FAILED: {job.error}", "error")
            job.state = "failed"
        else:
            job.reporter.log("-", f"job {job.name} finished", "ok")
            job.state = "done"
        finally:
            job.finished = time.time()


async def sse_events(job: Job, is_disconnected: Callable[[], Awaitable[bool]],
                     index: int = 0) -> AsyncIterator[str]:
    """Server-Sent Events for a job's log. Reads history by index so reconnects resume cleanly."""
    idle = 0.0
    while True:
        history = job.reporter.history
        while index < len(history):
            event = history[index]
            index += 1
            idle = 0.0
            yield f"id: {index}\nevent: log\ndata: {json.dumps(event)}\n\n"
        if job.state != "running" and index >= len(job.reporter.history):
            end = {"state": job.state, "error": job.error}
            yield f"event: end\ndata: {json.dumps(end)}\n\n"
            return
        if await is_disconnected():
            return
        await asyncio.sleep(SSE_POLL_S)
        idle += SSE_POLL_S
        if idle >= SSE_HEARTBEAT_S:
            idle = 0.0
            yield ": keep-alive\n\n"

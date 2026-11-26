"""Parallel per-host execution and progress reporting."""

from __future__ import annotations

import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Iterable, TypeVar

from clusterctl.inventory import Host

T = TypeVar("T")
MAX_PARALLEL = 16


@dataclass(frozen=True)
class HostResult:
    host: str
    ok: bool
    detail: str
    value: object = None


class Reporter:
    """Base reporter. Subclasses decide where messages go."""

    def log(self, host: str, message: str, level: str = "info") -> None:
        raise NotImplementedError

    def info(self, message: str) -> None:
        self.log("-", message)

    def error(self, message: str) -> None:
        self.log("-", message, "error")


class ConsoleReporter(Reporter):
    _COLORS = {"info": "", "ok": "\033[32m", "warn": "\033[33m", "error": "\033[31m"}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._color = sys.stdout.isatty()

    def log(self, host: str, message: str, level: str = "info") -> None:
        prefix = f"[{host}] " if host != "-" else ""
        color = self._COLORS.get(level, "") if self._color else ""
        reset = "\033[0m" if color else ""
        with self._lock:
            print(f"{color}{prefix}{message}{reset}", flush=True)


class QueueReporter(Reporter):
    """Collects events for the dashboard's live log stream."""

    def __init__(self) -> None:
        self.events: "queue.Queue[dict]" = queue.Queue()
        self.history: list[dict] = []
        self._lock = threading.Lock()

    def log(self, host: str, message: str, level: str = "info") -> None:
        event = {"t": time.time(), "host": host, "level": level, "msg": message}
        with self._lock:
            self.history.append(event)
        self.events.put(event)


def run_parallel(
    hosts: Iterable[Host],
    fn: Callable[[Host], T],
    reporter: Reporter,
    label: str,
) -> list[HostResult]:
    """Run fn(host) for every host concurrently; one failure never stops the others."""
    hosts = list(hosts)

    def wrapped(host: Host) -> HostResult:
        try:
            value = fn(host)
            detail = value if isinstance(value, str) else "ok"
            reporter.log(host.name, f"{label}: {detail}", "ok")
            return HostResult(host.name, True, detail, value)
        except Exception as exc:  # noqa: BLE001 - per-host isolation is the point
            reporter.log(host.name, f"{label} FAILED: {exc}", "error")
            return HostResult(host.name, False, str(exc))

    if not hosts:
        return []
    with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL, len(hosts))) as pool:
        return list(pool.map(wrapped, hosts))


def all_ok(results: list[HostResult]) -> bool:
    return all(r.ok for r in results)


def failed_names(results: list[HostResult]) -> list[str]:
    return [r.host for r in results if not r.ok]

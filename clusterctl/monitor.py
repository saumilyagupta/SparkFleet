"""Live per-node load: CPU %, memory %, load average, and Spark cores in use.

A Sampler thread keeps one SSH session per node and reads /proc every few
seconds (one round trip per node). The master's /json/ adds how many cores
Spark is using on each worker, so the chart shows Spark work, not just load.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from clusterctl.cluster_ops import fetch_json, master_url
from clusterctl.context import Context
from clusterctl.inventory import Host
from clusterctl.ssh import HostSession

INTERVAL_S = 3.0
HISTORY_POINTS = 200   # 10 minutes at 3 s
PROBE = "head -1 /proc/stat; grep -E '^(MemTotal|MemAvailable):' /proc/meminfo; cat /proc/loadavg"


@dataclass(frozen=True)
class CpuTimes:
    busy: int
    total: int


@dataclass(frozen=True)
class Sample:
    cpu: CpuTimes
    mem_total_kb: int
    mem_available_kb: int
    load1: float


def parse_probe(text: str) -> Sample:
    """Parse the PROBE output: /proc/stat cpu line, meminfo lines, loadavg."""
    lines = [ln.split() for ln in text.strip().splitlines() if ln.strip()]
    cpu_line = next(ln for ln in lines if ln[0] == "cpu")
    values = [int(v) for v in cpu_line[1:9]]          # user nice system idle iowait irq softirq steal
    idle = values[3] + values[4]
    total = sum(values)
    mem = {ln[0].rstrip(":"): int(ln[1]) for ln in lines if ln[0] in ("MemTotal:", "MemAvailable:")}
    load = next(ln for ln in lines if ln[0] not in ("cpu", "MemTotal:", "MemAvailable:"))
    return Sample(CpuTimes(total - idle, total), mem.get("MemTotal", 0), mem.get("MemAvailable", 0),
                  float(load[0]))


def cpu_percent(prev: CpuTimes, cur: CpuTimes) -> float:
    total = cur.total - prev.total
    if total <= 0:
        return 0.0
    return round(max(0.0, min(100.0, 100.0 * (cur.busy - prev.busy) / total)), 1)


def mem_percent(sample: Sample) -> float:
    if sample.mem_total_kb <= 0:
        return 0.0
    return round(100.0 * (sample.mem_total_kb - sample.mem_available_kb) / sample.mem_total_kb, 1)


class Sampler:
    """Background sampler. snapshot() is safe to call from any thread."""

    def __init__(self, ctx: Context, interval_s: float = INTERVAL_S) -> None:
        self.ctx = ctx
        self.interval_s = interval_s
        self._sessions: dict[str, HostSession] = {}
        self._prev: dict[str, CpuTimes] = {}
        self._series: dict[str, deque] = {h.name: deque(maxlen=HISTORY_POINTS) for h in ctx.inventory.hosts}
        self._status: dict[str, str] = {h.name: "starting" for h in ctx.inventory.hosts}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "Sampler":
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="clusterctl-monitor", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        for session in list(self._sessions.values()):
            try:
                session.close()
            except Exception:  # noqa: BLE001 - closing a dead session
                pass
        self._sessions.clear()

    def _session(self, host: Host) -> HostSession:
        session = self._sessions.get(host.name)
        transport = session.client.get_transport() if session else None
        if session is None or transport is None or not transport.is_active():
            session = self.ctx.session(host)
            self._sessions[host.name] = session
        return session

    def _probe(self, host: Host) -> tuple[str, Sample | None, str]:
        try:
            return host.name, parse_probe(self._session(host).run(PROBE, timeout=10).out), ""
        except Exception as exc:  # noqa: BLE001 - a down node is data, not an error
            self._sessions.pop(host.name, None)
            return host.name, None, str(exc)

    def _spark_cores(self) -> dict[str, int]:
        try:
            state = fetch_json(self._session(self.ctx.inventory.master), master_url(self.ctx))
        except Exception:  # noqa: BLE001
            return {}
        by_ip = {w.get("host"): int(w.get("coresused") or 0)
                 for w in state.get("workers", []) if w.get("state") == "ALIVE"}
        return {h.name: by_ip[h.ip] for h in self.ctx.inventory.hosts if h.ip in by_ip}

    def tick(self) -> None:
        hosts = self.ctx.inventory.hosts
        with ThreadPoolExecutor(max_workers=min(16, len(hosts))) as pool:
            probes = list(pool.map(self._probe, hosts))
        spark = self._spark_cores()
        now = time.time()
        with self._lock:
            for name, sample, error in probes:
                if sample is None:
                    self._status[name] = f"down: {error[:120]}"
                    self._series[name].append({"t": now, "cpu": None, "mem": None, "load": None,
                                               "spark_cores": 0})
                    self._prev.pop(name, None)
                    continue
                prev = self._prev.get(name)
                self._prev[name] = sample.cpu
                self._status[name] = "up"
                if prev is None:
                    continue  # need two samples for a CPU delta
                self._series[name].append({"t": now, "cpu": cpu_percent(prev, sample.cpu),
                                           "mem": mem_percent(sample), "load": sample.load1,
                                           "spark_cores": spark.get(name, 0)})

    def _loop(self) -> None:
        while not self._stop.is_set():
            started = time.time()
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - the sampler must never die
                pass
            self._stop.wait(max(0.5, self.interval_s - (time.time() - started)))

    def snapshot(self) -> dict:
        with self._lock:
            nodes = []
            for host in self.ctx.inventory.hosts:
                series = list(self._series[host.name])
                nodes.append({"name": host.name, "ip": host.ip, "role": host.role,
                              "status": self._status[host.name],
                              "latest": series[-1] if series else None, "series": series})
        return {"interval_s": self.interval_s, "nodes": nodes}

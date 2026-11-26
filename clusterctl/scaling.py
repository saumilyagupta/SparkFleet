"""Pure helpers for presenting a scaling benchmark (results/bench-*.json).

No I/O and no HTML: the evidence report renders these, and the dashboard's scaling.js
mirrors the same rules in JavaScript.
"""

from __future__ import annotations

# Below this speedup at the largest worker count an input "barely speeds up".
BARELY_SPEEDUP = 1.5


def sizes(bench: dict) -> list[int]:
    return sorted({int(r["size_mb"]) for r in bench.get("table", [])})


def worker_counts(bench: dict) -> list[int]:
    counts = bench.get("worker_counts") or sorted({int(r["workers"]) for r in bench.get("table", [])})
    return [int(k) for k in counts]


def row(bench: dict, size_mb: int, workers: int) -> dict | None:
    return next((r for r in bench.get("table", [])
                 if int(r["size_mb"]) == size_mb and int(r["workers"]) == workers), None)


def series(bench: dict, size_mb: int) -> list[tuple[int, float]]:
    """(workers, speedup) points for one input size, ordered by workers."""
    pts = [(int(r["workers"]), float(r["speedup"])) for r in bench.get("table", [])
           if int(r["size_mb"]) == size_mb]
    return sorted(pts)


def all_correct(bench: dict) -> bool | None:
    """True if every size matched the single-machine reference, False if any did not, None if unchecked."""
    checks = bench.get("correct_vs_reference") or {}
    if not checks:
        return None
    return all(bool(v) for v in checks.values())


def failed_sizes(bench: dict) -> list[str]:
    return [k for k, v in (bench.get("correct_vs_reference") or {}).items() if not v]


def _fmt_x(speedup: float) -> str:
    return f"{speedup:.1f}×"


def _scaling_clause(pts: list[tuple[int, float]]) -> tuple[int, int, float, int]:
    base_k, top_k = pts[0][0], pts[-1][0]
    speedup = pts[-1][1]
    efficiency = round(100 * speedup / top_k) if top_k else 0
    return base_k, top_k, speedup, efficiency


def interpretation(bench: dict) -> str:
    """One sentence computed from the numbers: how the largest input scales, and the smallest."""
    all_sizes = sizes(bench)
    if not all_sizes:
        return "No benchmark measurements recorded."
    largest = all_sizes[-1]
    pts = series(bench, largest)
    base_k, top_k, speedup, eff = _scaling_clause(pts)
    if top_k == base_k:
        return f"Only {top_k} worker(s) measured for {largest} MB, so there is no scaling to compare yet."
    plural = "workers" if top_k != 1 else "worker"
    if speedup >= 1:
        head = (f"Largest input ({largest} MB): {top_k} {plural} are {_fmt_x(speedup)} faster than {base_k} "
                f"({eff}% efficiency)")
    else:
        head = (f"Largest input ({largest} MB): {top_k} {plural} are slower than {base_k} "
                f"({_fmt_x(speedup)}, {eff}% efficiency)")
    smallest = all_sizes[0]
    if smallest == largest:
        return head + "."
    s_pts = series(bench, smallest)
    s_speed = s_pts[-1][1] if s_pts else 0.0
    if s_speed < BARELY_SPEEDUP:
        tail = (f"; {smallest} MB barely speeds up ({_fmt_x(s_speed)}) because fixed start-up cost "
                "dominates the short job")
    else:
        tail = f"; {smallest} MB reaches {_fmt_x(s_speed)}"
    return head + tail + "."

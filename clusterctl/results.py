"""Machine-readable run results: parse driver logs, save JSON, index the results/ folder."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from clusterctl.paths import ProjectPaths

EXECUTOR_RE = re.compile(r"Granted executor ID (\S+) on hostPort ([\d.]+):\d+ with (\d+) core")
LOST_EXEC_RE = re.compile(r"Lost executor (\S+) on ([\d.]+): (.*)$")
LOST_TASK_RE = re.compile(r"Lost task (\d+)\.(\d+) in stage")
RETRY_RE = re.compile(r"Starting task (\d+)\.([1-9]\d*) in stage [\d.]+ \(TID \d+\) \(([\d.]+)")
STAGE_DONE_RE = re.compile(r"DAGScheduler: (\w+Stage \d+) \((\w+) at [^)]*\) finished in ([\d.]+) s")
FETCH_FAILED_RE = re.compile(r"FetchFailed|Missing an output location for shuffle")
RESUBMIT_RE = re.compile(r"Resubmitting (ShuffleMapStage \d+|ResultStage \d+)|Resubmitted ShuffleMapTask")
SPECULATABLE_RE = re.compile(r"Marking task [\d.]+ in stage [\d.]+ .*as speculatable")
SPEC_KILL_RE = re.compile(r"Killing attempt \d+ for task [\d.]+ in stage [\d.]+ .*as the attempt ([1-9]\d*) succeeded")
FETCH_EVENT_RE = re.compile(r"FetchFailed\(BlockManagerId\([^)]*\), shuffleId=(\d+), mapIndex=(-?\d+)")
TOP_RE = re.compile(r"^\s*(\d+)\s+(\S+)\s*$")
SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def parse_driver_log(lines: list[str]) -> dict:
    """Extract summary, executors, failures and retries from a wordcount.py driver log."""
    summary: dict = {}
    executors: dict[str, dict] = {}
    lost_executors, lost_tasks, retried, stages, top = [], 0, [], [], []
    fetch_failed, resubmits, speculatable, spec_killed = 0, [], 0, 0
    lost_map_outputs: set[tuple[str, str]] = set()
    in_top = False
    for raw in lines:
        line = raw.split("] ", 1)[1] if raw.startswith("[") and "] " in raw else raw
        if m := EXECUTOR_RE.search(line):
            executors[m.group(1)] = {"id": m.group(1), "ip": m.group(2), "cores": int(m.group(3))}
        if m := LOST_EXEC_RE.search(line):
            lost_executors.append({"id": m.group(1), "ip": m.group(2), "reason": m.group(3).strip()})
        if LOST_TASK_RE.search(line):
            lost_tasks += 1
        if m := RETRY_RE.search(line):
            retried.append({"partition": int(m.group(1)), "attempt": int(m.group(2)), "ip": m.group(3)})
        if FETCH_FAILED_RE.search(line):
            fetch_failed += 1
        if m := FETCH_EVENT_RE.search(line):
            lost_map_outputs.add((m.group(1), m.group(2)))
        if m := RESUBMIT_RE.search(line):
            resubmits.append(m.group(0))
        if SPECULATABLE_RE.search(line):
            speculatable += 1
        if SPEC_KILL_RE.search(line):
            spec_killed += 1
        if m := STAGE_DONE_RE.search(line):
            stages.append({"stage": m.group(1), "op": m.group(2), "seconds": float(m.group(3))})
        if line.startswith("WORDCOUNT_DONE"):
            summary.update(_kv(line[len("WORDCOUNT_DONE"):]))
        elif line.startswith(("input=", "total_words=", "output=")):
            summary.update(_kv(line))
        elif re.match(r"^top \d+:", line):
            in_top = True
            continue
        if in_top:
            if m := TOP_RE.match(line):
                top.append({"word": m.group(2), "count": int(m.group(1))})
            elif line.startswith("="):
                in_top = False
    for key in ("partitions", "executors", "total_words", "distinct_words"):
        if key in summary:
            summary[key] = int(summary[key])
    if "elapsed_s" in summary:
        summary["elapsed_s"] = float(summary["elapsed_s"])
    return {
        "summary": summary, "top": top, "executors": list(executors.values()),
        "lost_executors": lost_executors, "lost_tasks": lost_tasks,
        "retried_tasks": len(retried), "retries": retried[:50], "stages": stages,
        "fetch_failed": fetch_failed, "lost_map_outputs": len(lost_map_outputs), "resubmitted": resubmits[:20], "resubmit_count": len(resubmits),
        "speculatable": speculatable, "speculative_wins": spec_killed,
    }


def _kv(text: str) -> dict:
    return dict(part.split("=", 1) for part in text.split() if "=" in part)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reference_counts(paths: ProjectPaths, input_name: str) -> Path | None:
    """Single-machine count of a local input file (cached by content hash), or None if absent."""
    source = paths.root / input_name
    if not source.is_file():
        return None
    ref = paths.results_dir / f"reference-{input_name}-{sha256(source)[:12]}.tsv"
    if not ref.exists():
        sys.path.insert(0, str(paths.jobs_dir))
        from local_count import count_file  # noqa: PLC0415 - jobs/ is not a package

        ref.parent.mkdir(parents=True, exist_ok=True)
        with ref.open("w", encoding="utf-8", newline="\n") as fh:
            for word, count in count_file(source):
                fh.write(f"{word}\t{count}\n")
    return ref


def write_json(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


def _load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def results_index(paths: ProjectPaths) -> dict:
    """Newest-first view of saved runs, demos, reports and logs in results/."""
    folder = paths.results_dir
    if not folder.is_dir():
        return {"wordcount": [], "demos": [], "bench": [], "reports": [], "logs": []}

    def newest(pattern: str) -> list[Path]:
        return sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)

    runs = [d for p in newest("wordcount-*.json") if (d := _load(p))]
    demos = [d for p in newest("demo-*.json") if (d := _load(p))]
    bench = [d for p in newest("bench-*.json") if (d := _load(p)) and "table" in d]
    reports = [{"name": p.name, "modified": _mtime(p)} for p in newest("evidence-*.html")]
    logs = [{"name": p.name, "modified": _mtime(p), "bytes": p.stat().st_size}
            for p in newest("*.log")]
    return {"wordcount": runs, "demos": demos, "bench": bench, "reports": reports, "logs": logs}


def safe_result_file(paths: ProjectPaths, name: str) -> Path:
    """Resolve a results/ file by bare name; rejects anything that could escape the folder."""
    if not SAFE_NAME.match(name) or name.startswith("."):
        raise ValueError("invalid file name")
    path = (paths.results_dir / name).resolve()
    if path.parent != paths.results_dir.resolve() or not path.is_file():
        raise FileNotFoundError(name)
    return path


def _mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")

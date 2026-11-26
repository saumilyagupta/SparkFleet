"""Scaling benchmark view: evidence report section, interpretation sentence, results API. No SSH."""

from __future__ import annotations

import json
import os
import time

import pytest

from clusterctl import evidence, scaling


def _bench(sizes=(100, 2000), times=None, correct=None, stamp="20261127-120000") -> dict:
    times = times or {100: [9.0, 8.5, 8.2], 2000: [120.0, 64.0, 46.0]}
    table = []
    for size in sizes:
        base = times[size][0]
        for k, t in enumerate(times[size], start=1):
            sp = round(base / t, 2)
            table.append({"size_mb": size, "workers": k, "median_s": t, "speedup": sp,
                          "efficiency": round(sp / k, 2), "throughput_mb_s": round(size / t, 1)})
    return {"stamp": stamp, "cores_per_worker": 24, "worker_counts": [1, 2, 3], "repeats": 3,
            "inputs": [], "runs": [], "table": table,
            "correct_vs_reference": correct if correct is not None else {str(s): True for s in sizes}}


# ------------------------------------------------------------------ interpretation

def test_interpretation_picks_largest_size_and_efficiency():
    text = scaling.interpretation(_bench())
    # 2000 MB: 120 / 46 = 2.61x on 3 workers -> 87% efficiency
    assert text.startswith("Largest input (2000 MB): 3 workers are 2.6× faster than 1 (87% efficiency)")
    assert "100 MB barely speeds up (1.1×)" in text and text.endswith(".")


def test_interpretation_smallest_scaling_well_and_single_size():
    good = _bench(times={100: [10.0, 5.0, 4.0], 2000: [120.0, 64.0, 46.0]})
    assert "100 MB reaches 2.5×" in scaling.interpretation(good)
    only = _bench(sizes=(100,), times={100: [9.1, 10.0, 8.6]})
    assert scaling.interpretation(only) == (
        "Largest input (100 MB): 3 workers are 1.1× faster than 1 (35% efficiency).")


def test_interpretation_slower_and_empty():
    slow = _bench(sizes=(100,), times={100: [8.0, 9.0, 10.0]})
    assert "are slower than 1 (0.8×" in scaling.interpretation(slow)
    assert scaling.interpretation({"table": []}) == "No benchmark measurements recorded."


def test_all_correct_states():
    assert scaling.all_correct(_bench()) is True
    assert scaling.all_correct(_bench(correct={"100": True, "2000": False})) is False
    assert scaling.all_correct(_bench(correct={})) is None


# ------------------------------------------------------------------ evidence section

def test_scaling_section_renders_chart_table_and_verdict():
    html = evidence._scaling(_bench(sizes=(100, 500, 1000, 2000),
                                    times={100: [9.0, 8.5, 8.2], 500: [33.0, 19.0, 14.0],
                                           1000: [62.0, 34.0, 24.0], 2000: [120.0, 64.0, 46.0]}))
    assert html.count("<svg") == 1
    assert html.count("<polyline") == 4                      # one per input size
    assert html.count("class='sc-ideal'") == 2               # ideal line + its legend swatch
    assert html.count("<tr><td><b>") == 4                    # one table row per size
    assert "<th>1 worker</th><th>2 workers</th><th>3 workers</th>" in html
    assert "120.0 s" in html and "2.61× · 87% · 43.5 MB/s" in html
    assert "✓ Every run matched the single-machine count" in html
    assert "Largest input (2000 MB)" in html


def test_scaling_section_escapes_text_and_flags_failures():
    bad = _bench(correct={"100": True, "<b>2000</b>": False}, stamp="<script>x</script>")
    html = evidence._scaling(bad)
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "&lt;b&gt;2000&lt;/b&gt;" in html and "verdict bad" in html


def test_scaling_section_missing_bench():
    html = evidence._scaling(None)
    assert "not run yet" in html and "clusterctl bench" in html and "<svg" not in html


def test_scaling_section_unchecked_correctness():
    assert "Correctness not checked" in evidence._scaling(_bench(correct={}))


# ------------------------------------------------------------------ results API

def test_results_api_returns_bench_newest_first(tmp_path, monkeypatch, sample_inventory):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from clusterctl import inventory as inv
    from clusterctl.dashboard.actions import default_actions
    from clusterctl.dashboard.app import create_app
    from clusterctl.paths import ProjectPaths

    token = "test-token-0123456789abcdef"
    monkeypatch.setenv("CLUSTERCTL_HOME", str(tmp_path))
    paths = ProjectPaths(tmp_path)
    inv.save(sample_inventory, paths.inventory)
    paths.results_dir.mkdir()
    now = time.time()
    for i, stamp in enumerate(["old", "mid", "new"]):
        f = paths.results_dir / f"bench-{stamp}.json"
        f.write_text(json.dumps(_bench(stamp=stamp)), encoding="utf-8")
        os.utime(f, (now - 100 + i, now - 100 + i))
    run = paths.results_dir / "bench-new-100mb-w1-r1.json"     # per-run file: no "table", not a bench
    run.write_text(json.dumps({"tag": "bench-new-100mb-w1-r1"}), encoding="utf-8")

    client = TestClient(create_app(paths, token, actions=default_actions()))
    data = client.get("/api/evidence/results", headers={"X-Token": token}).json()
    assert [b["stamp"] for b in data["bench"]] == ["new", "mid", "old"]

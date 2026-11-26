from clusterctl import evidence

RUN = {
    "tag": "wordcount-1", "started": "2026-11-26T22:59:31", "result_sha256": "ab" * 32,
    "summary": {"app_id": "app-1", "driver": "pc1", "total_words": 1000, "distinct_words": 3,
                "partitions": 72, "executors": 3, "elapsed_s": 8.3},
    "top": [{"word": "<script>", "count": 600}, {"word": "of", "count": 300}],
    "executors": [{"id": "app-1/0", "ip": "10.0.0.2", "cores": 24}],
    "stages": [{"stage": "ShuffleMapStage 0", "op": "reduceByKey", "seconds": 6.4}],
    "matches_local_reference": True,
}
DEMO = {
    "stamp": "s1", "victim": {"name": "node2", "ip": "10.0.0.2"}, "identical": True,
    "lost_tasks": 24, "retried_tasks": 24,
    "baseline": {"sha256": "cd" * 32}, "recovered": {"sha256": "cd" * 32},
    "timeline": [{"t": 16.5, "event": "tasks running on node2; injecting failure"},
                 {"t": 102.8, "event": "job finished despite the failure"}],
    "key_log_lines": ["ERROR TaskSchedulerImpl: Lost executor 0 on 10.0.0.2"],
}


def test_wordcount_section_escapes_words_and_shows_verdict():
    html = evidence._wordcount(RUN)

    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "byte-identical" in html
    assert "1,000" in html


def test_wordcount_section_without_runs():
    assert "No Word Count run recorded" in evidence._wordcount(None)


def test_demo_section_marks_kill_step_and_counts():
    html = evidence._demo(DEMO)

    assert "class='kill'" in html
    assert "<b>24</b>" in html
    assert "identical to the clean baseline" in html


def test_bars_scale_to_largest_count():
    svg = evidence._bars([{"word": "a", "count": 100}, {"word": "b", "count": 50}])

    assert "width='560'" in svg
    assert "width='280'" in svg


def test_jobs_highlight_degraded_runs():
    html = evidence._jobs({"apps": [
        {"id": "a1", "name": "full", "state": "FINISHED", "cores": 72, "duration_s": 9, "started": 0},
        {"id": "a2", "name": "failure", "state": "FINISHED", "cores": 48, "duration_s": 85, "started": 0},
    ]})

    assert html.count("class='degraded'") == 1


def test_summary_flags_missing_workers():
    st = {"master": {"status": "ALIVE", "alive_workers": ["10.0.0.2"], "cores": 24, "memory_mb": 1024}}
    nodes = [{"role": "master", "is_physical": True}, {"role": "worker", "is_physical": True},
             {"role": "worker", "is_physical": True}]

    html = evidence._summary(st, nodes)

    assert "1/2" in html
    assert "chip bad" in html

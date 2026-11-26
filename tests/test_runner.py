import threading

from clusterctl.inventory import Host
from clusterctl.runner import (
    HostResult,
    QueueReporter,
    Reporter,
    all_ok,
    failed_names,
    run_parallel,
)


class RecordingReporter(Reporter):
    def __init__(self):
        self.entries = []
        self._lock = threading.Lock()

    def log(self, host, message, level="info"):
        with self._lock:
            self.entries.append((host, message, level))


def _hosts(n):
    return tuple(Host(f"h{i}", f"10.0.0.{i + 1}", "ubuntu") for i in range(n))


def test_failure_on_one_host_does_not_affect_others():
    reporter = RecordingReporter()

    def fn(host):
        if host.name == "h1":
            raise RuntimeError("boom")
        return f"done {host.name}"

    results = run_parallel(_hosts(3), fn, reporter, "step")

    by_host = {r.host: r for r in results}
    assert by_host["h1"].ok is False and by_host["h1"].detail == "boom"
    assert by_host["h0"].ok and by_host["h0"].detail == "done h0"
    assert by_host["h2"].ok and by_host["h2"].detail == "done h2"
    assert failed_names(results) == ["h1"]
    assert not all_ok(results)


def test_results_preserve_input_order():
    results = run_parallel(_hosts(5), lambda h: h.name, RecordingReporter(), "s")
    assert [r.host for r in results] == ["h0", "h1", "h2", "h3", "h4"]


def test_non_string_value_kept_and_detail_is_ok():
    results = run_parallel(_hosts(1), lambda h: 42, RecordingReporter(), "s")
    assert results[0].value == 42 and results[0].detail == "ok"


def test_reporter_receives_ok_and_error_levels():
    reporter = RecordingReporter()
    run_parallel(_hosts(2), lambda h: (_ for _ in ()).throw(ValueError("bad")) if h.name == "h0" else "x",
                 reporter, "lbl")
    levels = {host: level for host, _, level in reporter.entries}
    assert levels == {"h0": "error", "h1": "ok"}
    assert any("lbl FAILED: bad" in m for _, m, _ in reporter.entries)


def test_empty_hosts_returns_empty_list():
    assert run_parallel([], lambda h: "x", RecordingReporter(), "s") == []


def test_all_ok_on_empty_is_true():
    assert all_ok([]) is True


def test_runs_concurrently_across_hosts():
    barrier = threading.Barrier(3, timeout=5)

    def fn(host):
        barrier.wait()  # deadlocks (BrokenBarrierError) if run serially
        return "x"

    assert all_ok(run_parallel(_hosts(3), fn, RecordingReporter(), "s"))


def test_queue_reporter_collects_history():
    reporter = QueueReporter()
    reporter.info("hello")
    reporter.error("oops")
    assert [e["level"] for e in reporter.history] == ["info", "error"]
    assert reporter.events.qsize() == 2


def test_hostresult_is_immutable():
    r = HostResult("h", True, "ok")
    try:
        r.ok = False
    except Exception:
        return
    raise AssertionError("HostResult should be frozen")

import pytest

from clusterctl.results import parse_driver_log
from clusterctl.scenarios import SCENARIOS, plan_for
from clusterctl.workload import WorkloadError

LOG = """\
26/11/27 10:00:01 WARN TaskSetManager: Lost task 3.0 in stage 1.0 (TID 300) (10.0.0.3 executor 1): FetchFailed(BlockManagerId(0, 10.0.0.2, 7079, None), shuffleId=0, mapIndex=5, mapId=5, reduceId=3, message=
26/11/27 10:00:01 INFO DAGScheduler: Resubmitting ShuffleMapStage 0 (reduceByKey at /x/wordcount.py:64) and ResultStage 1 (sortBy at /x/wordcount.py:69) due to fetch failure
26/11/27 10:00:02 INFO DAGScheduler: Resubmitted ShuffleMapTask(0, 5), so marking it as still running.
26/11/27 10:01:00 INFO TaskSetManager: Marking task 12 in stage 0.0 (on 10.0.0.2) as speculatable because it ran more than 4500 ms
26/11/27 10:01:05 INFO TaskSetManager: Killing attempt 0 for task 12.0 in stage 0.0 (TID 12) on 10.0.0.2 as the attempt 1 succeeded on 10.0.0.3
"""


def test_parser_counts_shuffle_and_speculation_signatures():
    parsed = parse_driver_log(LOG.splitlines())

    assert parsed["fetch_failed"] == 1
    assert parsed["resubmit_count"] == 2
    assert parsed["resubmitted"][0].startswith("Resubmitting ShuffleMapStage 0")
    assert parsed["speculatable"] == 1
    assert parsed["speculative_wins"] == 1
    assert parsed["lost_tasks"] == 1


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_scenario_has_a_plan(scenario):
    plan = plan_for(scenario, cores=72)

    assert plan.partitions >= 72


def test_shuffle_slows_reduce_side_and_straggler_enables_speculation():
    assert plan_for("shuffle", 72).reduce_delay_ms > 0
    straggler = plan_for("straggler", 72)
    assert "spark.speculation=true" in straggler.submit_opts
    assert straggler.delay_ms == 0  # must be CPU-bound work, or a CPU quota cannot slow it down


def test_unknown_scenario_rejected():
    with pytest.raises(WorkloadError):
        plan_for("meteor", 72)


def test_speculative_win_counts_only_when_the_copy_won():
    copy_won = ("INFO TaskSetManager: Killing attempt 0 for task 12.0 in stage 0.0 (TID 12) on 10.0.0.2 "
                "as the attempt 1 succeeded on 10.0.0.3")
    original_won = ("INFO TaskSetManager: Killing attempt 1 for task 12.1 in stage 0.0 (TID 99) on 10.0.0.3 "
                    "as the attempt 0 succeeded on 10.0.0.2")

    assert parse_driver_log([copy_won])["speculative_wins"] == 1
    assert parse_driver_log([original_won])["speculative_wins"] == 0


def test_restore_keeps_going_after_one_step_fails(monkeypatch):
    from types import SimpleNamespace

    from clusterctl import scenarios
    from clusterctl.inventory import Host
    from clusterctl.workload import DemoTimeline

    calls = []

    def throttle(ctx, host, quota):
        calls.append(("throttle", host.name))
        raise RuntimeError("ssh dropped")

    monkeypatch.setattr(scenarios, "_throttle", throttle)
    monkeypatch.setattr(scenarios, "_start_worker", lambda ctx, host: calls.append(("start", host.name)))
    ctx = SimpleNamespace(reporter=SimpleNamespace(log=lambda *a, **k: None))
    a, b = Host("a", "10.0.0.2", "u"), Host("b", "10.0.0.3", "u")

    errors = scenarios._restore(ctx, DemoTimeline(), victims=[b], throttled=[a])

    assert ("start", "b") in calls          # the failed un-throttle did not skip the restart
    assert len(errors) == 1 and "un-throttle a" in errors[0]

"""The dashboard's advanced operations: scenarios, master failover, benchmark, archive.

No SSH and no real cluster: library calls are faked, and the real code paths that
touch the cluster (ha.bench.run_benchmark) are monkeypatched at module level.
"""

from __future__ import annotations

import time

import pytest

pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from clusterctl import inventory as inv  # noqa: E402
from clusterctl.context import Context  # noqa: E402
from clusterctl.dashboard.actions import (  # noqa: E402
    Action,
    ActionFailed,
    BenchParams,
    FailoverParams,
    NoParams,
    ScenarioParams,
    default_actions,
    parse_sizes,
)
from clusterctl.dashboard.app import create_app  # noqa: E402
from clusterctl.paths import ProjectPaths  # noqa: E402
from clusterctl.runner import QueueReporter  # noqa: E402

TOKEN = "test-token-0123456789abcdef"
AUTH = {"X-Token": TOKEN}
ADVANCED = {"ha", "demo_failover", "demo_scenario", "bench", "archive"}


@pytest.fixture
def paths(tmp_path, monkeypatch, sample_inventory) -> ProjectPaths:
    monkeypatch.setenv("CLUSTERCTL_HOME", str(tmp_path))
    p = ProjectPaths(tmp_path)
    inv.save(sample_inventory, p.inventory)
    return p


def _ctx(paths: ProjectPaths, sample_inventory) -> Context:
    return Context.build(paths, sample_inventory, QueueReporter())


def _wait_job(client: TestClient, job_id: str, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/api/jobs/{job_id}", headers=AUTH).json()
        if job["state"] != "running":
            return job
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never finished")


# ------------------------------------------------------------------ action registry

def test_default_actions_expose_advanced_operations():
    actions = default_actions()
    assert ADVANCED <= actions.keys()
    # The demos inject faults with sudo; status, bench and archive do not need the vault.
    for name in ("demo_failover", "demo_scenario"):
        assert actions[name].needs_vault is True, name
    for name in ("ha", "bench", "archive"):
        assert actions[name].needs_vault is False, name


def test_advanced_action_parameters_are_strict():
    assert ScenarioParams(scenario="shuffle").scenario == "shuffle"
    with pytest.raises(ValueError):
        ScenarioParams(scenario="meteor")
    assert FailoverParams().input == "corpus.txt"
    assert BenchParams().sizes == "100,500,1000,2000"
    assert BenchParams().repeats == 3
    with pytest.raises(ValueError):
        BenchParams(extra=1)


# ------------------------------------------------------------------ sizes parsing

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("100,500", (100, 500)),
        (" 1000 , 100 ", (100, 1000)),          # whitespace and order do not matter
        ("500,500,100", (100, 500)),            # duplicates collapse
    ],
)
def test_parse_sizes_accepts_lists(raw, expected):
    assert parse_sizes(raw) == expected


@pytest.mark.parametrize("raw", ["", " , ", "abc", "100,abc", "0,100", "-5", "100,2000,3000,4000,5000",
                                 "1,2,3,4,5,6,7,8,9"])
def test_parse_sizes_rejects_bad_lists(raw):
    with pytest.raises(ValueError):
        parse_sizes(raw)


def test_bench_params_validate_sizes_and_bounds():
    with pytest.raises(ValueError):
        BenchParams(sizes="zero")
    assert BenchParams(sizes="50, 100").sizes == "50,100"   # normalized for the action
    with pytest.raises(ValueError):
        BenchParams(repeats=0)
    with pytest.raises(ValueError):
        BenchParams(max_workers=0)


# ------------------------------------------------------------------ the actions themselves

def test_archive_refuses_a_file_containing_a_vault_secret(paths, sample_inventory):
    from clusterctl.vault import Vault

    results = paths.results_dir
    results.mkdir(parents=True)
    (results / "demo-20260101-000000.md").write_text("host password: swordfish\n", encoding="utf-8")
    vault = Vault.create(paths.vault, "master-pass").with_secret("w1", "swordfish")
    vault.save()
    ctx = _ctx(paths, sample_inventory).with_vault(vault)
    with pytest.raises(ActionFailed, match="refused"):
        default_actions()["archive"].fn(ctx, NoParams())
    msgs = [e["msg"] for e in ctx.reporter.history]  # type: ignore[attr-defined]
    assert any("REFUSED demo-20260101-000000.md" in m for m in msgs)
    assert not (paths.root / "docs" / "results" / "demo-20260101-000000.md").exists()


def test_archive_reports_a_clean_copy(paths, sample_inventory):
    results = paths.results_dir
    results.mkdir(parents=True)
    (results / "wordcount-20260101-000000.json").write_text('{"stamp": "x"}\n', encoding="utf-8")
    ctx = _ctx(paths, sample_inventory)
    default_actions()["archive"].fn(ctx, NoParams())     # no exception: nothing refused
    assert (paths.root / "docs" / "results" / "wordcount-20260101-000000.json").is_file()
    assert any("1 copied" in e["msg"] for e in ctx.reporter.history)


def test_ha_logs_masters_and_zookeeper(paths, sample_inventory, monkeypatch):
    import clusterctl.ha as ha

    # actions.py imports these on use, so the patch must land on their home module.
    states = [
        {"name": "m1", "ip": "10.0.0.1", "status": "ALIVE", "alive_workers": 2},
        {"name": "m2", "ip": "10.0.0.9", "status": "STANDBY"},
    ]
    zk = [{"name": "m1", "ip": "10.0.0.1", "mode": "leader"}]
    # A late-binding closure: the module-level name is replaced after the lambda would close over it.
    monkeypatch.setattr(ha, "master_states", lambda ctx: list(states))
    monkeypatch.setattr(ha, "zookeeper_status", lambda ctx: list(zk))
    ctx = _ctx(paths, sample_inventory)
    default_actions()["ha"].fn(ctx, NoParams())
    lines = [(e["host"], e["msg"]) for e in ctx.reporter.history]
    assert any(host == "m1" and "ALIVE" in msg and "workers=2" in msg for host, msg in lines)
    assert any(host == "m2" and "STANDBY" in msg for host, msg in lines)
    assert any(host == "m1" and "zookeeper" in msg and "leader" in msg for host, msg in lines)


def test_bench_logs_the_speedup_table(paths, sample_inventory, monkeypatch):
    import clusterctl.bench as bench

    report = {"stamp": "20260101-000000", "correct_vs_reference": {"100": True},
              "table": [{"size_mb": 100, "workers": 1, "median_s": 9.0, "speedup": 1.0,
                         "efficiency": 1.0, "throughput_mb_s": 11.1}]}
    calls = {}

    def fake(ctx, *, sizes_mb, repeats, max_workers):
        calls.update(sizes=sizes_mb, repeats=repeats, max_workers=max_workers)
        return report

    monkeypatch.setattr(bench, "run_benchmark", fake)
    ctx = _ctx(paths, sample_inventory)
    default_actions()["bench"].fn(ctx, BenchParams(sizes="100", repeats=2, max_workers=3))
    assert calls == {"sizes": (100,), "repeats": 2, "max_workers": 3}
    msgs = [e["msg"] for e in ctx.reporter.history]
    assert any("100 MB" in m and "1 worker" in m for m in msgs)
    assert any("correct vs single-machine" in m for m in msgs)


# ------------------------------------------------------------------ HTTP surface

def test_bad_bench_parameters_are_422(paths):
    c = TestClient(create_app(paths, TOKEN))       # real default_actions
    r = c.post("/api/actions/bench", json={"sizes": "zero"}, headers=AUTH)
    assert r.status_code == 422 and "sizes" in r.json()["error"]


def test_failover_demo_is_409_while_the_vault_is_locked(paths):
    c = TestClient(create_app(paths, TOKEN))
    r = c.post("/api/actions/demo_failover", json={}, headers=AUTH)
    assert r.status_code == 409 and "vault" in r.json()["error"]


def test_advanced_actions_are_reachable_by_name(paths):
    """Every button's endpoint is wired: the fakes stand in for the cluster, so nothing is touched."""

    def make(marker: str, cls: type[NoParams] = NoParams):
        def fn(ctx, params):
            ctx.reporter.log("-", f"{marker} ran", "ok")

        return Action(fn, cls)

    actions = default_actions()
    actions.update({
        "ha": make("ha"),
        "bench": make("bench", BenchParams),
        "archive": make("archive"),
        "demo_scenario": make("demo_scenario", ScenarioParams),
        "demo_failover": make("demo_failover", FailoverParams),
    })
    c = TestClient(create_app(paths, TOKEN, actions=actions))
    cases = {
        "ha": {},
        "bench": {},
        "archive": {},
        "demo_scenario": {"scenario": "cascade"},
        "demo_failover": {},
    }
    for name, body in cases.items():
        r = c.post(f"/api/actions/{name}", json=body, headers=AUTH)
        assert r.status_code in (202, 409), (name, r.status_code, r.text)
        if r.status_code == 202:
            job = _wait_job(c, r.json()["job_id"])
            assert job["state"] == "done"
            assert any(f"{name} ran" in e["msg"] for e in job["history"])

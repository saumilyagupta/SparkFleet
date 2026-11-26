"""History Server wiring and live-sampler resilience (review follow-ups)."""

from dataclasses import replace
from types import SimpleNamespace

from clusterctl.cluster_ops import service_command
from clusterctl.inventory import Inventory
from clusterctl.monitor import Sampler
from clusterctl.provision import services_for
from clusterctl.render import render
from clusterctl.provision import render_values
from clusterctl.tunnel import HISTORY_LOCAL_PORT, plan_forwards

PROBE_OK = "cpu  {busy} 0 0 {idle} 0 0 0 0 0 0\nMemTotal: 1000 kB\nMemAvailable: 500 kB\n0.5 0.4 0.3 1/2 3\n"


def test_history_service_only_on_master_and_can_be_disabled(sample_inventory):
    assert "spark-history" in services_for(sample_inventory, sample_inventory.master)
    assert "spark-history" not in services_for(sample_inventory, sample_inventory.get("w1"))
    off = replace(sample_inventory, cluster=replace(sample_inventory.cluster, history_server=False))
    assert "spark-history" not in services_for(off, off.master)


def test_history_unit_renders_without_placeholders(sample_inventory):
    unit = render("spark-history.service", render_values(sample_inventory, sample_inventory.master))

    assert "{{" not in unit
    assert "-Dspark.history.ui.port=18080" in unit
    assert "file:///opt/spark-data/events" in unit


def test_service_command_keeps_history_non_fatal_and_ordered():
    start = service_command(("spark-master", "spark-history"), "start")
    stop = service_command(("spark-master", "spark-history"), "stop")

    assert start == "systemctl start spark-master && (systemctl start spark-history || true)"
    assert stop == "(systemctl stop spark-history || true) && systemctl stop spark-master"


def test_tunnel_history_forward_follows_flag(sample_inventory):
    ctx = SimpleNamespace(inventory=sample_inventory)
    assert any(f.local_port == HISTORY_LOCAL_PORT for f in plan_forwards(ctx))
    off = replace(sample_inventory, cluster=replace(sample_inventory.cluster, history_server=False))
    assert not any(f.local_port == HISTORY_LOCAL_PORT for f in plan_forwards(SimpleNamespace(inventory=off)))


class _FakeSession:
    def __init__(self, outputs: list, closed: list) -> None:
        self.outputs, self.closed = outputs, closed
        self.client = SimpleNamespace(get_transport=lambda: SimpleNamespace(is_active=lambda: True))

    def run(self, command, timeout=None):
        out = self.outputs.pop(0)
        if isinstance(out, Exception):
            raise out
        return SimpleNamespace(out=out)

    def close(self) -> None:
        self.closed.append(self)


def _sampler(inventory: Inventory, script: dict[str, list]) -> tuple[Sampler, list]:
    closed: list = []

    def session(host, **_):
        return _FakeSession(script[host.name], closed)

    ctx = SimpleNamespace(inventory=inventory, session=session, jump=None)
    sampler = Sampler(ctx, interval_s=0.01)
    sampler._spark_cores = lambda: {"w1": 4}
    return sampler, closed


def test_sampler_closes_failed_sessions_and_recovers(sample_inventory):
    inv = replace(sample_inventory, hosts=(sample_inventory.get("w1"),))
    ok = lambda busy, idle: PROBE_OK.format(busy=busy, idle=idle)  # noqa: E731
    sampler, closed = _sampler(inv, {"w1": [ok(100, 900), ok(150, 950), RuntimeError("boom")]})

    sampler.tick()                       # first sample only primes the CPU counters
    assert sampler.snapshot()["nodes"][0]["status"] == "starting"
    sampler.tick()                       # second sample -> 50 % CPU
    node = sampler.snapshot()["nodes"][0]
    assert node["status"] == "up" and node["latest"]["cpu"] == 50.0 and node["latest"]["spark_cores"] == 4
    sampler.tick()                       # failure -> session is CLOSED, not just forgotten
    node = sampler.snapshot()["nodes"][0]
    assert node["status"].startswith("down") and node["latest"]["cpu"] is None
    assert len(closed) == 1


def test_sampler_backs_off_reconnects_to_down_node(sample_inventory):
    inv = replace(sample_inventory, hosts=(sample_inventory.get("w1"),))
    attempts = []

    def session(host, **_):
        attempts.append(host.name)
        raise OSError("unreachable")

    sampler = Sampler(SimpleNamespace(inventory=inv, session=session, jump=None), interval_s=0.01)
    sampler._spark_cores = lambda: None
    for _ in range(5):
        sampler.tick()

    assert len(attempts) == 1           # ticks 2-5 skip the reconnect (RETRY_DOWN_EVERY = 5)

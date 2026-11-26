"""Master high availability: config model, rendering, service placement, active-master choice."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from clusterctl import cluster_ops
from clusterctl.inventory import Host, Inventory, InventoryError, validate
from clusterctl.provision import daemon_opts, render_values, services_for
from clusterctl.render import render


@pytest.fixture
def ha_inventory(sample_inventory) -> Inventory:
    extra = Host(name="w3", ip="10.0.0.4", user="ubuntu")
    inv = replace(sample_inventory, hosts=(*sample_inventory.hosts, extra))
    return replace(inv, cluster=replace(inv.cluster, ha_enabled=True, standby_masters=("w1",),
                                        zookeeper_hosts=("w1", "w2", "w3")))


def test_ha_off_keeps_single_master_everywhere(sample_inventory):
    assert sample_inventory.masters == (sample_inventory.master,)
    assert sample_inventory.spark_master_url == "spark://10.0.0.1:7077"
    assert sample_inventory.zookeepers == ()
    assert daemon_opts(sample_inventory) == ""
    worker = render("spark-worker.service", render_values(sample_inventory, sample_inventory.get("w1")))
    assert worker.rstrip().splitlines()[10].endswith("spark://10.0.0.1:7077")


def test_ha_master_url_lists_primary_then_standby(ha_inventory):
    assert ha_inventory.spark_master_url == "spark://10.0.0.1:7077,10.0.0.2:7077"
    assert [h.name for h in ha_inventory.masters] == ["m1", "w1"]


def test_ha_services_placement(ha_inventory):
    assert services_for(ha_inventory, ha_inventory.master) == ("spark-master", "spark-history")
    assert services_for(ha_inventory, ha_inventory.get("w1")) == ("zookeeper", "spark-master", "spark-worker")
    assert services_for(ha_inventory, ha_inventory.get("w2")) == ("zookeeper", "spark-worker")


def test_ha_renders_zookeeper_recovery_and_ensemble(ha_inventory):
    env = render("spark-env.sh", render_values(ha_inventory, ha_inventory.master))
    assert "spark.deploy.recoveryMode=ZOOKEEPER" in env
    assert "spark.deploy.zookeeper.url=10.0.0.2:2181,10.0.0.3:2181,10.0.0.4:2181" in env
    zoo = render("zoo.cfg", render_values(ha_inventory, ha_inventory.get("w2")))
    assert "server.1=10.0.0.2:2888:3888" in zoo and "server.3=10.0.0.4:2888:3888" in zoo
    assert "admin.enableServer=false" in zoo          # its default port 8080 would clash with the master UI
    assert "clientPortAddress=10.0.0.3" in zoo
    unit = render("zookeeper.service", render_values(ha_inventory, ha_inventory.get("w2")))
    assert "{{" not in unit and "start-foreground" in unit


def test_myid_matches_ensemble_order(ha_inventory):
    from clusterctl.ha import myid_for

    assert [myid_for(ha_inventory, ha_inventory.get(n)) for n in ("w1", "w2", "w3")] == [1, 2, 3]


@pytest.mark.parametrize("change, message", [
    ({"standby_masters": ()}, "standby"),
    ({"standby_masters": ("m1",)}, "primary"),
    ({"zookeeper_hosts": ("w1", "w2")}, "odd"),
    ({"standby_masters": ("ghost",)}, "unknown"),
])
def test_ha_validation(ha_inventory, change, message):
    bad = replace(ha_inventory, cluster=replace(ha_inventory.cluster, **change))
    with pytest.raises(InventoryError, match=message):
        validate(bad)


def test_master_state_prefers_alive_over_standby(ha_inventory, monkeypatch):
    states = {"10.0.0.1": {"status": "STANDBY"}, "10.0.0.2": {"status": "ALIVE", "workers": []}}

    class _S:
        def __init__(self, host):
            self.host = host

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    ctx = SimpleNamespace(inventory=ha_inventory, session=lambda h, **_: _S(h))
    monkeypatch.setattr(cluster_ops, "fetch_json", lambda s, url, timeout=60: states[s.host.ip])

    assert cluster_ops.master_state(ctx)["status"] == "ALIVE"


def test_master_state_survives_dead_primary(ha_inventory, monkeypatch):
    class _S:
        def __init__(self, host):
            if host.ip == "10.0.0.1":
                raise OSError("connection refused")
            self.host = host

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    ctx = SimpleNamespace(inventory=ha_inventory, session=lambda h, **_: _S(h))
    monkeypatch.setattr(cluster_ops, "fetch_json", lambda s, url, timeout=60: {"status": "ALIVE"})

    assert cluster_ops.master_state(ctx)["status"] == "ALIVE"


def test_remove_stale_services_only_touches_unwanted_units():
    from clusterctl.provision import remove_stale_services

    present = {"/etc/systemd/system/zookeeper.service", "/etc/systemd/system/spark-worker.service"}
    sudo_calls = []
    session = SimpleNamespace(
        run=lambda cmd, check=True: SimpleNamespace(ok=cmd.split()[-1] in present),
        sudo=lambda cmd: sudo_calls.append(cmd))

    removed = remove_stale_services(session, ("spark-worker",))

    assert removed == ["zookeeper"]
    assert len(sudo_calls) == 1 and "disable --now zookeeper" in sudo_calls[0]

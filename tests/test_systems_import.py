import json

import pytest

from clusterctl import systems_import
from clusterctl.inventory import InventoryError
from clusterctl.systems_import import parse_systems


@pytest.fixture(autouse=True)
def fake_local_ips(monkeypatch):
    monkeypatch.setattr(systems_import, "local_ips", lambda: {"127.0.0.1", "192.168.99.99"})


@pytest.fixture
def data():
    return {"systems": [
        {"system": "System 01", "ip_address": "10.0.0.1", "username": "u1", "password": "p1"},
        {"system": "System 02", "ip_address": "10.0.0.2", "username": "u2", "password": "p2"},
        {"system": "System 03", "ip_address": "10.0.0.3", "username": "u3", "password": "p3"},
    ]}


def test_default_master_is_first(data):
    result = parse_systems(data)
    assert result.inventory.master.name == "system-01"
    assert [h.name for h in result.inventory.workers] == ["system-02", "system-03"]


def test_hosts_fields_mapped(data):
    host = parse_systems(data).inventory.get("system-02")
    assert (host.ip, host.user, host.role) == ("10.0.0.2", "u2", "worker")


@pytest.mark.parametrize("selector", ["system-02", "10.0.0.2", "System 02"])
def test_master_flag_by_name_ip_or_label(data, selector):
    assert parse_systems(data, master=selector).inventory.master.name == "system-02"


def test_role_master_in_entry(data):
    data["systems"][2]["role"] = "master"
    assert parse_systems(data).inventory.master.name == "system-03"


def test_master_flag_overrides_role_in_entry(data):
    data["systems"][2]["role"] = "master"
    assert parse_systems(data, master="10.0.0.2").inventory.master.name == "system-02"


def test_unknown_master_raises(data):
    with pytest.raises(InventoryError, match="does not match"):
        parse_systems(data, master="nope")


def test_master_equal_to_local_ip_rejected(data):
    data["systems"][0]["ip_address"] = "192.168.99.99"
    with pytest.raises(InventoryError, match="control machine"):
        parse_systems(data)


def test_worker_with_local_ip_is_allowed(data):
    data["systems"][1]["ip_address"] = "192.168.99.99"
    assert len(parse_systems(data).inventory.hosts) == 3


def test_passwords_mapped_by_slug(data):
    assert parse_systems(data).passwords == {
        "system-01": "p1", "system-02": "p2", "system-03": "p3"}


def test_entry_without_password_is_omitted(data):
    del data["systems"][1]["password"]
    assert "system-02" not in parse_systems(data).passwords


@pytest.mark.parametrize("bad", [None, {}, {"systems": []}, {"systems": "x"}, []])
def test_missing_or_empty_systems_rejected(bad):
    with pytest.raises(InventoryError, match="non-empty"):
        parse_systems(bad)


def test_entry_missing_username_rejected(data):
    del data["systems"][0]["username"]
    with pytest.raises(InventoryError, match=r"systems\[0\] missing username"):
        parse_systems(data)


def test_label_without_leading_letter_gets_fallback_name(data):
    data["systems"][1]["system"] = "123"
    assert parse_systems(data).inventory.hosts[1].name == "node-02"


def test_missing_label_gets_fallback_name(data):
    del data["systems"][1]["system"]
    assert parse_systems(data).inventory.hosts[1].name == "node-02"


def test_duplicate_ip_rejected(data):
    data["systems"][1]["ip_address"] = "10.0.0.1"
    with pytest.raises(InventoryError, match="duplicate"):
        parse_systems(data)


def test_load_systems_reads_file(tmp_path, data):
    p = tmp_path / "s.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    assert systems_import.load_systems(p).inventory.master.ip == "10.0.0.1"

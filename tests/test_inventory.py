import pytest

from clusterctl import inventory as inv
from clusterctl.inventory import ClusterConfig, Host, Inventory, InventoryError


def _h(name="w1", ip="10.0.0.2", **kw):
    return {"name": name, "ip": ip, "user": "ubuntu", **kw}


class TestParse:
    def test_valid_inventory_parses(self):
        result = inv.parse({"hosts": [_h("m1", "10.0.0.1", role="master"), _h()]})
        assert [h.name for h in result.hosts] == ["m1", "w1"]
        assert result.master.name == "m1"

    def test_none_and_empty_give_empty_inventory(self):
        assert inv.parse(None).hosts == ()
        assert inv.parse({}).hosts == ()

    def test_hosts_not_a_list_rejected(self):
        with pytest.raises(InventoryError, match="must be a list"):
            inv.parse({"hosts": {"a": 1}})

    def test_host_not_mapping_rejected(self):
        with pytest.raises(InventoryError, match="must be a mapping"):
            inv.parse({"hosts": ["nope"]})

    @pytest.mark.parametrize("ip", ["10.0.0.256", "abc", "10.0.0", "1.2.3.4.5"])
    def test_bad_ip_rejected(self, ip):
        with pytest.raises(InventoryError, match="invalid IP"):
            inv.parse({"hosts": [_h(ip=ip)]})

    @pytest.mark.parametrize("name", ["W1", "1node", "has space", "under_score", "a" * 64])
    def test_bad_name_rejected(self, name):
        with pytest.raises(InventoryError, match="invalid"):
            inv.parse({"hosts": [_h(name=name)]})

    def test_duplicate_name_rejected(self):
        with pytest.raises(InventoryError, match="duplicate host name"):
            inv.parse({"hosts": [_h("a", "10.0.0.2"), _h("a", "10.0.0.3")]})

    def test_duplicate_ip_rejected(self):
        with pytest.raises(InventoryError, match="duplicate host ip"):
            inv.parse({"hosts": [_h("a", "10.0.0.2"), _h("b", "10.0.0.2")]})

    def test_two_masters_rejected(self):
        with pytest.raises(InventoryError, match="only one master"):
            inv.parse({"hosts": [_h("a", "10.0.0.2", role="master"),
                                 _h("b", "10.0.0.3", role="master")]})

    def test_unknown_host_key_rejected(self):
        with pytest.raises(InventoryError, match="unknown keys: foo"):
            inv.parse({"hosts": [_h(foo=1)]})

    def test_unknown_cluster_key_rejected(self):
        with pytest.raises(InventoryError, match="cluster has unknown keys"):
            inv.parse({"cluster": {"bogus": 1}, "hosts": []})

    def test_missing_required_field_rejected(self):
        with pytest.raises(InventoryError, match="missing user"):
            inv.parse({"hosts": [{"name": "a", "ip": "10.0.0.2"}]})

    @pytest.mark.parametrize("extra", [
        {"role": "boss"}, {"port": 0}, {"port": 70000}, {"cores": 0}, {"memory": "lots"},
    ])
    def test_bad_role_port_cores_memory_rejected(self, extra):
        with pytest.raises(InventoryError):
            inv.parse({"hosts": [_h(**extra)]})

    def test_role_is_normalised_to_lowercase(self):
        assert inv.parse({"hosts": [_h(role=" MASTER ")]}).hosts[0].is_master

    def test_cluster_overrides_merge_with_defaults(self):
        result = inv.parse({"cluster": {"master_port": 9999, "java_packages": ["x"]}})
        assert result.cluster.master_port == 9999
        assert result.cluster.java_packages == ("x",)
        assert result.cluster.worker_ui_port == 8081


class TestMasterAndWorkers:
    def test_master_returns_the_master(self, sample_inventory):
        assert sample_inventory.master.name == "m1"

    def test_master_missing_raises(self, worker_hosts):
        with pytest.raises(InventoryError, match="exactly one master"):
            Inventory(hosts=worker_hosts).master

    def test_workers_exclude_master_by_default(self, sample_inventory):
        assert [h.name for h in sample_inventory.workers] == ["w1", "w2"]

    def test_workers_include_master_when_configured(self, master_host, worker_hosts):
        i = Inventory(cluster=ClusterConfig(master_runs_worker=True),
                      hosts=(master_host, *worker_hosts))
        assert [h.name for h in i.workers] == ["m1", "w1", "w2"]

    def test_get_and_select(self, sample_inventory):
        assert sample_inventory.get("w1").ip == "10.0.0.2"
        assert sample_inventory.select(None) == sample_inventory.hosts
        assert [h.name for h in sample_inventory.select(["w2", "m1"])] == ["w2", "m1"]
        with pytest.raises(InventoryError, match="unknown host"):
            sample_inventory.get("nope")


class TestImmutability:
    def test_with_host_returns_new_and_keeps_original(self, sample_inventory):
        new = Host(name="w3", ip="10.0.0.4", user="ubuntu")
        updated = sample_inventory.with_host(new)
        assert updated is not sample_inventory
        assert len(sample_inventory.hosts) == 3
        assert updated.get("w3") == new

    def test_with_host_replaces_same_name(self, sample_inventory):
        updated = sample_inventory.with_host(Host("w1", "10.0.0.9", "ubuntu"))
        assert updated.get("w1").ip == "10.0.0.9"
        assert sample_inventory.get("w1").ip == "10.0.0.2"
        assert len(updated.hosts) == 3

    def test_with_host_validates(self, sample_inventory):
        with pytest.raises(InventoryError):
            sample_inventory.with_host(Host("w3", "10.0.0.2", "ubuntu"))

    def test_without_host_returns_new_and_keeps_original(self, sample_inventory):
        updated = sample_inventory.without_host("w1")
        assert [h.name for h in updated.hosts] == ["m1", "w2"]
        assert len(sample_inventory.hosts) == 3

    def test_without_unknown_host_raises(self, sample_inventory):
        with pytest.raises(InventoryError):
            sample_inventory.without_host("ghost")

    def test_host_is_frozen(self, master_host):
        with pytest.raises(Exception):
            master_host.name = "x"


class TestSaveLoad:
    def test_round_trip(self, tmp_path, sample_inventory):
        path = tmp_path / "inventory.yaml"
        inv.save(sample_inventory, path)
        assert inv.load(path) == sample_inventory

    def test_round_trip_preserves_cluster_config(self, tmp_path, sample_inventory):
        custom = Inventory(cluster=ClusterConfig(master_runs_worker=True, jump_host="bastion"),
                           hosts=sample_inventory.hosts)
        path = tmp_path / "i.yaml"
        inv.save(custom, path)
        loaded = inv.load(path)
        assert loaded.cluster.master_runs_worker is True
        assert loaded.cluster.jump_host == "bastion"

    def test_load_missing_file(self, tmp_path):
        with pytest.raises(InventoryError, match="not found"):
            inv.load(tmp_path / "nope.yaml")

    def test_save_rejects_invalid_and_leaves_no_file(self, tmp_path):
        bad = Inventory(hosts=(Host("a", "bad", "u"),))
        with pytest.raises(InventoryError):
            inv.save(bad, tmp_path / "i.yaml")
        assert not (tmp_path / "i.yaml").exists()


class TestReadCsv:
    def test_with_password_column(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("name,ip,user,role,password,port\n"
                     "m1,10.0.0.1,ubuntu,master,pw1,2222\n"
                     "w1,10.0.0.2,ubuntu,,pw2,\n", encoding="utf-8")
        rows = inv.read_csv(f)
        assert rows[0].host.is_master and rows[0].host.port == 2222
        assert rows[0].password == "pw1"
        assert rows[1].host.role == "worker" and rows[1].host.port == 22

    def test_without_password_column(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("name,ip,user\nw1,10.0.0.2,ubuntu\n", encoding="utf-8")
        rows = inv.read_csv(f)
        assert rows[0].password is None
        assert rows[0].host.name == "w1"

    def test_empty_password_is_none(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("name,ip,user,password\nw1,10.0.0.2,ubuntu,\n", encoding="utf-8")
        assert inv.read_csv(f)[0].password is None

    def test_bom_header_accepted(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_bytes("﻿name,ip,user\nw1,10.0.0.2,ubuntu\n".encode("utf-8"))
        assert len(inv.read_csv(f)) == 1

    def test_bad_header_rejected(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("name,address,user\nw1,10.0.0.2,ubuntu\n", encoding="utf-8")
        with pytest.raises(InventoryError, match="CSV header"):
            inv.read_csv(f)

    def test_empty_file_rejected(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("", encoding="utf-8")
        with pytest.raises(InventoryError, match="CSV header"):
            inv.read_csv(f)

    def test_bad_row_reports_line_number(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("name,ip,user\nw1,10.0.0.2,ubuntu\nw2,not-an-ip,ubuntu\n", encoding="utf-8")
        with pytest.raises(InventoryError, match=r"CSV line 3"):
            inv.read_csv(f)

    def test_non_numeric_port_reports_line_number(self, tmp_path):
        f = tmp_path / "h.csv"
        f.write_text("name,ip,user,port\nw1,10.0.0.2,ubuntu,abc\n", encoding="utf-8")
        with pytest.raises(InventoryError, match=r"CSV line 2"):
            inv.read_csv(f)

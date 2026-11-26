import pytest

from clusterctl.inventory import ClusterConfig, Inventory
from clusterctl.provision import (
    HOSTS_BEGIN,
    HOSTS_END,
    Facts,
    ProvisionError,
    check_python_consistency,
    hosts_block,
    merge_hosts_file,
    services_for,
)


def _facts(**versions: str) -> dict[str, Facts]:
    return {n: Facts("ubuntu", "22.04", "x86_64", v) for n, v in versions.items()}


class TestHostsBlock:
    def test_block_lists_every_host_and_marks_master(self, sample_inventory):
        lines = hosts_block(sample_inventory).split("\n")
        assert lines[0] == HOSTS_BEGIN and lines[-1] == HOSTS_END
        assert "10.0.0.1\tm1\tspark-master" in lines
        assert "10.0.0.2\tw1" in lines
        assert sum("spark-master" in line for line in lines) == 1


class TestMergeHostsFile:
    BASE = "127.0.0.1 localhost\n::1 ip6-localhost\n"

    def test_appends_block_and_preserves_other_lines(self, sample_inventory):
        block = hosts_block(sample_inventory)
        merged = merge_hosts_file(self.BASE, block)
        assert merged.startswith(self.BASE)
        assert block in merged
        assert merged.endswith("\n")

    def test_replaces_existing_managed_block(self, sample_inventory):
        old = f"{self.BASE}\n{HOSTS_BEGIN}\n10.9.9.9\told\n{HOSTS_END}\n"
        merged = merge_hosts_file(old, hosts_block(sample_inventory))
        assert "10.9.9.9" not in merged
        assert merged.count(HOSTS_BEGIN) == 1
        assert "127.0.0.1 localhost" in merged

    def test_idempotent_when_applied_twice(self, sample_inventory):
        block = hosts_block(sample_inventory)
        once = merge_hosts_file(self.BASE, block)
        assert merge_hosts_file(once, block) == once

    def test_lines_after_block_are_kept(self, sample_inventory):
        old = f"a\n{HOSTS_BEGIN}\nx\n{HOSTS_END}\nafter\n"
        merged = merge_hosts_file(old, hosts_block(sample_inventory))
        assert "after" in merged.splitlines()

    def test_empty_current_file(self, sample_inventory):
        block = hosts_block(sample_inventory)
        assert merge_hosts_file("", block) == "\n" + block + "\n"


class TestPythonConsistency:
    def test_ok_returns_version(self):
        assert check_python_consistency(_facts(a="3.10", b="3.10")) == "3.10"

    def test_mismatch_raises_listing_hosts(self):
        with pytest.raises(ProvisionError, match=r"differ.*a=3\.10.*b=3\.12"):
            check_python_consistency(_facts(a="3.10", b="3.12"))

    def test_missing_raises_naming_host(self):
        with pytest.raises(ProvisionError, match="missing on: b"):
            check_python_consistency(_facts(a="3.10", b="missing"))

    def test_below_3_8_raises(self):
        with pytest.raises(ProvisionError, match="3.8"):
            check_python_consistency(_facts(a="3.7", b="3.7"))

    def test_exactly_3_8_ok(self):
        assert check_python_consistency(_facts(a="3.8")) == "3.8"

    def test_numeric_not_lexicographic_comparison(self):
        # "3.10" < "3.8" as strings; must be treated as newer.
        assert check_python_consistency(_facts(a="3.10")) == "3.10"


class TestServicesFor:
    def test_master_only_runs_master(self, sample_inventory):
        assert services_for(sample_inventory, sample_inventory.master) == ("spark-master", "spark-history")

    def test_worker_runs_worker(self, sample_inventory):
        assert services_for(sample_inventory, sample_inventory.get("w1")) == ("spark-worker",)

    def test_master_runs_worker_gets_both(self, sample_inventory):
        inv = Inventory(cluster=ClusterConfig(master_runs_worker=True), hosts=sample_inventory.hosts)
        assert services_for(inv, inv.master) == ("spark-master", "spark-worker", "spark-history")

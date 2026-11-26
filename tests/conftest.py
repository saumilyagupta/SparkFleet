"""Shared fixtures. All IPs/passwords are fake."""

import pytest

from clusterctl.inventory import ClusterConfig, Host, Inventory


@pytest.fixture
def master_host() -> Host:
    return Host(name="m1", ip="10.0.0.1", user="ubuntu", role="master")


@pytest.fixture
def worker_hosts() -> tuple[Host, Host]:
    return (
        Host(name="w1", ip="10.0.0.2", user="ubuntu", cores=4, memory="4g"),
        Host(name="w2", ip="10.0.0.3", user="ubuntu"),
    )


@pytest.fixture
def sample_inventory(master_host, worker_hosts) -> Inventory:
    return Inventory(cluster=ClusterConfig(), hosts=(master_host, *worker_hosts))

"""Inventory: the non-secret list of machines (inventory.yaml).

Passwords never live here; they go in the encrypted vault (see vault.py).
All functions return new objects; nothing mutates an Inventory in place.
"""

from __future__ import annotations

import csv
import ipaddress
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Iterable

import yaml

ROLES = ("master", "worker")
NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
USER_RE = re.compile(r"^[a-z_][a-z0-9_.-]{0,31}$", re.IGNORECASE)
MEMORY_RE = re.compile(r"^\d+[mg]$", re.IGNORECASE)


class InventoryError(ValueError):
    """Raised when inventory content is invalid."""


@dataclass(frozen=True)
class Host:
    name: str
    ip: str
    user: str
    role: str = "worker"
    port: int = 22
    cores: int | None = None
    memory: str | None = None

    @property
    def is_master(self) -> bool:
        return self.role == "master"

    def to_dict(self) -> dict:
        data = {"name": self.name, "ip": self.ip, "user": self.user, "role": self.role}
        if self.port != 22:
            data["port"] = self.port
        if self.cores is not None:
            data["cores"] = self.cores
        if self.memory is not None:
            data["memory"] = self.memory
        return data


@dataclass(frozen=True)
class ClusterConfig:
    spark_version: str = "3.5.10"
    hadoop_profile: str = "hadoop3"
    java_packages: tuple[str, ...] = (
        "openjdk-17-jdk-headless",
        "openjdk-21-jdk-headless",
        "openjdk-11-jdk-headless",
    )
    python_bin: str = "python3"
    install_dir: str = "/opt"
    data_dir: str = "/opt/spark-data"
    master_port: int = 7077
    master_ui_port: int = 8080
    worker_ui_port: int = 8081
    driver_port: int = 7078
    block_manager_port: int = 7079
    master_runs_worker: bool = False
    # ~/.ssh/config alias of a bastion used to reach the cluster (None = direct).
    jump_host: str | None = None

    @property
    def spark_home(self) -> str:
        return f"{self.install_dir}/spark"

    @property
    def spark_dirname(self) -> str:
        return f"spark-{self.spark_version}-bin-{self.hadoop_profile}"

    def to_dict(self) -> dict:
        return {
            "spark_version": self.spark_version,
            "hadoop_profile": self.hadoop_profile,
            "java_packages": list(self.java_packages),
            "python_bin": self.python_bin,
            "install_dir": self.install_dir,
            "data_dir": self.data_dir,
            "master_port": self.master_port,
            "master_ui_port": self.master_ui_port,
            "worker_ui_port": self.worker_ui_port,
            "driver_port": self.driver_port,
            "block_manager_port": self.block_manager_port,
            "master_runs_worker": self.master_runs_worker,
            "jump_host": self.jump_host,
        }


@dataclass(frozen=True)
class Inventory:
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    hosts: tuple[Host, ...] = ()

    @property
    def master(self) -> Host:
        masters = [h for h in self.hosts if h.is_master]
        if len(masters) != 1:
            raise InventoryError(f"expected exactly one master, found {len(masters)}")
        return masters[0]

    @property
    def workers(self) -> tuple[Host, ...]:
        """Hosts that run a Spark worker daemon."""
        return tuple(
            h for h in self.hosts
            if not h.is_master or self.cluster.master_runs_worker
        )

    def get(self, name: str) -> Host:
        for host in self.hosts:
            if host.name == name:
                return host
        raise InventoryError(f"unknown host '{name}'")

    def select(self, names: Iterable[str] | None) -> tuple[Host, ...]:
        if not names:
            return self.hosts
        return tuple(self.get(n) for n in names)

    def with_host(self, host: Host) -> "Inventory":
        others = tuple(h for h in self.hosts if h.name != host.name)
        updated = replace(self, hosts=others + (host,))
        validate(updated)
        return updated

    def without_host(self, name: str) -> "Inventory":
        self.get(name)
        return replace(self, hosts=tuple(h for h in self.hosts if h.name != name))

    def to_dict(self) -> dict:
        return {"cluster": self.cluster.to_dict(), "hosts": [h.to_dict() for h in self.hosts]}


def _parse_host(raw: dict, index: int) -> Host:
    if not isinstance(raw, dict):
        raise InventoryError(f"hosts[{index}] must be a mapping")
    missing = [k for k in ("name", "ip", "user") if not raw.get(k)]
    if missing:
        raise InventoryError(f"hosts[{index}] missing {', '.join(missing)}")
    unknown = set(raw) - {"name", "ip", "user", "role", "port", "cores", "memory"}
    if unknown:
        raise InventoryError(f"hosts[{index}] has unknown keys: {', '.join(sorted(unknown))}")
    return Host(
        name=str(raw["name"]).strip(),
        ip=str(raw["ip"]).strip(),
        user=str(raw["user"]).strip(),
        role=str(raw.get("role", "worker")).strip().lower(),
        port=int(raw.get("port", 22)),
        cores=int(raw["cores"]) if raw.get("cores") is not None else None,
        memory=str(raw["memory"]).strip() if raw.get("memory") is not None else None,
    )


def _parse_cluster(raw: dict | None) -> ClusterConfig:
    raw = raw or {}
    defaults = ClusterConfig()
    known = set(defaults.to_dict())
    unknown = set(raw) - known
    if unknown:
        raise InventoryError(f"cluster has unknown keys: {', '.join(sorted(unknown))}")
    merged = {**defaults.to_dict(), **raw}
    merged["java_packages"] = tuple(merged["java_packages"])
    return ClusterConfig(**merged)


def validate_host(host: Host) -> None:
    if not NAME_RE.match(host.name):
        raise InventoryError(
            f"host name '{host.name}' invalid: lowercase letters, digits, '-', start with a letter"
        )
    try:
        ipaddress.ip_address(host.ip)
    except ValueError as exc:
        raise InventoryError(f"host '{host.name}': invalid IP '{host.ip}'") from exc
    if not USER_RE.match(host.user):
        raise InventoryError(f"host '{host.name}': invalid user '{host.user}'")
    if host.role not in ROLES:
        raise InventoryError(f"host '{host.name}': role must be one of {ROLES}")
    if not 1 <= host.port <= 65535:
        raise InventoryError(f"host '{host.name}': invalid port {host.port}")
    if host.cores is not None and host.cores < 1:
        raise InventoryError(f"host '{host.name}': cores must be >= 1")
    if host.memory is not None and not MEMORY_RE.match(host.memory):
        raise InventoryError(f"host '{host.name}': memory must look like 4g or 2048m")


def validate(inventory: Inventory) -> None:
    for host in inventory.hosts:
        validate_host(host)
    for attr in ("name", "ip"):
        values = [getattr(h, attr) for h in inventory.hosts]
        dupes = sorted({v for v in values if values.count(v) > 1})
        if dupes:
            raise InventoryError(f"duplicate host {attr}: {', '.join(dupes)}")
    masters = [h.name for h in inventory.hosts if h.is_master]
    if len(masters) > 1:
        raise InventoryError(f"only one master allowed, found: {', '.join(masters)}")


def parse(data: dict | None) -> Inventory:
    data = data or {}
    hosts_raw = data.get("hosts") or []
    if not isinstance(hosts_raw, list):
        raise InventoryError("'hosts' must be a list")
    inventory = Inventory(
        cluster=_parse_cluster(data.get("cluster")),
        hosts=tuple(_parse_host(raw, i) for i, raw in enumerate(hosts_raw)),
    )
    validate(inventory)
    return inventory


def load(path: Path) -> Inventory:
    if not path.exists():
        raise InventoryError(f"{path} not found; run 'clusterctl init' first")
    with path.open(encoding="utf-8") as fh:
        return parse(yaml.safe_load(fh))


def save(inventory: Inventory, path: Path) -> None:
    validate(inventory)
    header = (
        "# clusterctl inventory: machines only, NO passwords (those live in secrets.vault).\n"
        "# Add a machine by adding an entry under 'hosts', then run: clusterctl up --limit <name>\n"
    )
    body = yaml.safe_dump(inventory.to_dict(), sort_keys=False, default_flow_style=False)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(header + body, encoding="utf-8", newline="\n")
    tmp.replace(path)


@dataclass(frozen=True)
class CsvRow:
    host: Host
    password: str | None


def read_csv(path: Path) -> tuple[CsvRow, ...]:
    """Read hosts from CSV with header: name,ip,user[,role][,password][,port]."""
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or not {"name", "ip", "user"} <= set(reader.fieldnames):
            raise InventoryError("CSV header must include name,ip,user (optional role,password,port)")
        rows = []
        for line_no, raw in enumerate(reader, start=2):
            cleaned = {k: (v or "").strip() for k, v in raw.items() if k}
            password = cleaned.pop("password", "") or None
            host_raw = {k: v for k, v in cleaned.items() if v}
            try:
                host = _parse_host(host_raw, line_no)
                validate_host(host)
            except (InventoryError, ValueError) as exc:
                raise InventoryError(f"CSV line {line_no}: {exc}") from exc
            rows.append(CsvRow(host=host, password=password))
    return tuple(rows)

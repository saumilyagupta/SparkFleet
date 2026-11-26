"""Import a systems.json credentials file into inventory.yaml + the encrypted vault.

Expected shape (extra keys ignored):
    {"systems": [{"system": "System 01", "ip_address": "...", "username": "...",
                  "password": "...", "role": "master"   # optional
                 }, ...]}

Master selection order: --master flag, else an entry with role=master /
master=true, else the FIRST system. The control machine (this laptop) can
never be the master: the master must be one of the listed systems, and an IP
belonging to this machine is rejected.
"""

from __future__ import annotations

import json
import re
import socket
from dataclasses import dataclass
from pathlib import Path

from clusterctl.inventory import ClusterConfig, Host, Inventory, InventoryError, validate


@dataclass(frozen=True)
class ImportResult:
    inventory: Inventory
    passwords: dict[str, str]


def _slug(label: str, index: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
    if not slug or not slug[0].isalpha():
        slug = f"node-{index + 1:02d}"
    return slug[:63]


def local_ips() -> set[str]:
    ips = {"127.0.0.1"}
    try:
        ips.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass
    return ips


def _pick_master(entries: list[dict], names: list[str], master: str | None) -> int:
    if master:
        for i, (entry, name) in enumerate(zip(entries, names)):
            if master in (name, entry.get("system"), entry.get("ip_address"), entry.get("username")):
                return i
        raise InventoryError(f"--master '{master}' does not match any system name, IP, or username")
    for i, entry in enumerate(entries):
        if str(entry.get("role", "")).lower() == "master" or entry.get("master") is True:
            return i
    return 0


def parse_systems(data: dict, *, master: str | None = None,
                  cluster: ClusterConfig | None = None) -> ImportResult:
    entries = data.get("systems") if isinstance(data, dict) else None
    if not isinstance(entries, list) or not entries:
        raise InventoryError("systems file must contain a non-empty 'systems' list")
    for i, entry in enumerate(entries):
        missing = [k for k in ("ip_address", "username") if not entry.get(k)]
        if missing:
            raise InventoryError(f"systems[{i}] missing {', '.join(missing)}")
    names = [_slug(str(e.get("system") or f"node {i + 1}"), i) for i, e in enumerate(entries)]
    master_index = _pick_master(entries, names, master)
    master_ip = entries[master_index]["ip_address"]
    if master_ip in local_ips():
        raise InventoryError(
            f"master {master_ip} is this control machine; the master must be a cluster system"
        )
    hosts = tuple(
        Host(name=names[i], ip=str(e["ip_address"]).strip(), user=str(e["username"]).strip(),
             role="master" if i == master_index else "worker")
        for i, e in enumerate(entries)
    )
    inventory = Inventory(cluster=cluster or ClusterConfig(), hosts=hosts)
    validate(inventory)
    passwords = {names[i]: str(e["password"]) for i, e in enumerate(entries) if e.get("password")}
    return ImportResult(inventory, passwords)


def load_systems(path: Path, **kwargs) -> ImportResult:
    with path.open(encoding="utf-8") as fh:
        return parse_systems(json.load(fh), **kwargs)

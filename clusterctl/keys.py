"""SSH key management: control key, key registry, and append-only audit log.

registry.json  - every public key the tool has placed or seen, with fingerprint and status
audit.log      - one JSON line per event (generated, installed, revoked); never rewritten
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

import paramiko
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

CONTROL_COMMENT = "clusterctl-control"
# Hosts are bootstrapped in parallel threads; serialize registry read-modify-write.
_REGISTRY_LOCK = threading.RLock()


class KeyError_(Exception):
    """Key file or registry problem."""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fingerprint(public_key_line: str) -> str:
    """OpenSSH-style SHA256 fingerprint of an 'ssh-xxx AAAA... comment' line."""
    parts = public_key_line.strip().split()
    if len(parts) < 2:
        raise KeyError_(f"not an OpenSSH public key: {public_key_line[:40]!r}")
    blob = base64.b64decode(parts[1])
    digest = base64.b64encode(hashlib.sha256(blob).digest()).decode("ascii").rstrip("=")
    return f"SHA256:{digest}"


def generate_control_key(private_path: Path, public_path: Path) -> str:
    """Create the laptop's ed25519 control key if missing. Returns the public key line."""
    if private_path.exists() and public_path.exists():
        return public_path.read_text(encoding="utf-8").strip()
    private_path.parent.mkdir(parents=True, exist_ok=True)
    key = Ed25519PrivateKey.generate()
    private_bytes = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.OpenSSH,
        serialization.NoEncryption(),
    )
    public_line = key.public_key().public_bytes(
        serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
    ).decode("ascii") + f" {CONTROL_COMMENT}"
    private_path.write_bytes(private_bytes)
    try:
        os.chmod(private_path, 0o600)
    except OSError:
        pass  # Windows ignores POSIX modes; the keys/ dir is gitignored.
    public_path.write_text(public_line + "\n", encoding="utf-8", newline="\n")
    return public_line


def load_control_key(private_path: Path) -> paramiko.PKey:
    if not private_path.exists():
        raise KeyError_(f"{private_path} missing; run 'clusterctl init'")
    return paramiko.Ed25519Key.from_private_key_file(str(private_path))


@dataclass(frozen=True)
class KeyRecord:
    fingerprint: str
    kind: str          # "control" (laptop) or "node" (generated on a cluster machine)
    owner: str         # "laptop" or host name
    user: str
    ip: str
    key_type: str
    public_key: str
    added_at: str
    status: str = "active"   # active | revoked
    installed_on: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        data = asdict(self)
        data["installed_on"] = list(self.installed_on)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "KeyRecord":
        return cls(**{**data, "installed_on": tuple(data.get("installed_on", ()))})


class KeyRegistry:
    """File-backed registry. Each mutating call rewrites registry.json and appends to audit.log."""

    def __init__(self, registry_path: Path, audit_path: Path) -> None:
        self.registry_path = registry_path
        self.audit_path = audit_path

    def records(self) -> tuple[KeyRecord, ...]:
        if not self.registry_path.exists():
            return ()
        data = json.loads(self.registry_path.read_text(encoding="utf-8"))
        return tuple(KeyRecord.from_dict(r) for r in data.get("keys", []))

    def find(self, fp: str) -> KeyRecord | None:
        return next((r for r in self.records() if r.fingerprint == fp), None)

    def _write(self, records: tuple[KeyRecord, ...]) -> None:
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        doc = {"updated_at": now_iso(), "keys": [r.to_dict() for r in records]}
        tmp = self.registry_path.with_name(f".registry-{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        tmp.replace(self.registry_path)

    def audit(self, event: str, **fields: object) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"at": now_iso(), "event": event, **fields})
        with self.audit_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def audit_entries(self, limit: int = 500) -> list[dict]:
        if not self.audit_path.exists():
            return []
        lines = self.audit_path.read_text(encoding="utf-8").splitlines()[-limit:]
        return [json.loads(line) for line in lines if line.strip()]

    def register(self, *, public_key: str, kind: str, owner: str, user: str, ip: str) -> KeyRecord:
        """Add a key if new; returns the (possibly existing) record."""
        with _REGISTRY_LOCK:
            return self._register(public_key=public_key, kind=kind, owner=owner, user=user, ip=ip)

    def _register(self, *, public_key: str, kind: str, owner: str, user: str, ip: str) -> KeyRecord:
        fp = fingerprint(public_key)
        existing = self.find(fp)
        if existing:
            return existing
        record = KeyRecord(
            fingerprint=fp, kind=kind, owner=owner, user=user, ip=ip,
            key_type=public_key.split()[0], public_key=public_key.strip(), added_at=now_iso(),
        )
        self._write(self.records() + (record,))
        self.audit("key_registered", fingerprint=fp, kind=kind, owner=owner, user=user, ip=ip)
        return record

    def mark_installed(self, fp: str, host_name: str) -> None:
        with _REGISTRY_LOCK:
            self._mark_installed(fp, host_name)

    def _mark_installed(self, fp: str, host_name: str) -> None:
        records = self.records()
        target = next((r for r in records if r.fingerprint == fp), None)
        if target is None or host_name in target.installed_on:
            return
        updated = replace(target, installed_on=target.installed_on + (host_name,))
        self._write(tuple(updated if r.fingerprint == fp else r for r in records))
        self.audit("key_installed", fingerprint=fp, host=host_name)

    def mark_revoked(self, fp: str, removed_from: list[str]) -> KeyRecord:
        with _REGISTRY_LOCK:
            return self._mark_revoked(fp, removed_from)

    def _mark_revoked(self, fp: str, removed_from: list[str]) -> KeyRecord:
        records = self.records()
        target = next((r for r in records if r.fingerprint == fp), None)
        if target is None:
            raise KeyError_(f"no key with fingerprint {fp}")
        remaining = tuple(h for h in target.installed_on if h not in removed_from)
        updated = replace(target, status="revoked", installed_on=remaining)
        self._write(tuple(updated if r.fingerprint == fp else r for r in records))
        self.audit("key_revoked", fingerprint=fp, removed_from=removed_from)
        return updated

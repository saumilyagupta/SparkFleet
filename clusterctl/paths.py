"""Project file locations. Everything lives under one project root (cwd or CLUSTERCTL_HOME)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @classmethod
    def discover(cls) -> "ProjectPaths":
        return cls(Path(os.environ.get("CLUSTERCTL_HOME", Path.cwd())).resolve())

    @property
    def inventory(self) -> Path:
        return self.root / "inventory.yaml"

    @property
    def vault(self) -> Path:
        return self.root / "secrets.vault"

    @property
    def keys_dir(self) -> Path:
        return self.root / "keys"

    @property
    def control_key(self) -> Path:
        return self.keys_dir / "control_ed25519"

    @property
    def control_pub(self) -> Path:
        return self.keys_dir / "control_ed25519.pub"

    @property
    def known_hosts(self) -> Path:
        return self.keys_dir / "known_hosts"

    @property
    def key_registry(self) -> Path:
        return self.keys_dir / "registry.json"

    @property
    def audit_log(self) -> Path:
        return self.keys_dir / "audit.log"

    @property
    def cache_dir(self) -> Path:
        return self.root / "cache"

    @property
    def results_dir(self) -> Path:
        return self.root / "results"

    @property
    def jobs_dir(self) -> Path:
        return self.root / "jobs"

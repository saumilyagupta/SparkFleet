"""In-memory server state for the dashboard: unlocked vault, jobs, open tunnels."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Callable

from clusterctl import inventory as inv
from clusterctl.cli import ENV_PASS, ENV_PASS_FILE
from clusterctl.context import Context, VaultLockedError
from clusterctl.dashboard.actions import Action
from clusterctl.dashboard.jobs import JobManager
from clusterctl.keys import KeyRegistry
from clusterctl.paths import ProjectPaths
from clusterctl.runner import QueueReporter
from clusterctl.vault import Vault, VaultError


def stored_master_password(paths: ProjectPaths) -> str | None:
    """Non-interactive half of cli.master_password: env var, then password file. Never prompts."""
    if os.environ.get(ENV_PASS):
        return os.environ[ENV_PASS]
    pass_file = Path(os.environ.get(ENV_PASS_FILE) or paths.root / ".vault-pass")
    if pass_file.is_file():
        return pass_file.read_text(encoding="utf-8").strip() or None
    return None


class DashboardState:
    def __init__(self, paths: ProjectPaths, actions: dict[str, Action],
                 tunnel_opener: Callable[[Context], list] | None) -> None:
        self.paths = paths
        self.actions = actions
        self.tunnel_opener = tunnel_opener
        self.jobs = JobManager()
        self.tunnels: list = []
        self._vault: Vault | None = None
        self._lock = threading.Lock()
        self._sampler = None
        self._sampler_lock = threading.Lock()
        self.sampler_factory: Callable[[Context], object] | None = None

    @property
    def vault(self) -> Vault | None:
        with self._lock:
            return self._vault

    def set_vault(self, vault: Vault | None) -> None:
        with self._lock:
            self._vault = vault

    def require_vault(self) -> Vault:
        vault = self.vault
        if vault is None:
            raise VaultLockedError("vault is locked; unlock it first")
        return vault

    def load_inventory(self) -> inv.Inventory:
        if not self.paths.inventory.exists():
            return inv.Inventory()
        return inv.load(self.paths.inventory)

    def context(self, *, need_vault: bool = False) -> Context:
        vault = self.require_vault() if need_vault else self.vault
        return Context.build(self.paths, inv.load(self.paths.inventory), QueueReporter(), vault)

    def sampler(self):
        """Live-load sampler, started on first use and kept for the dashboard's lifetime."""
        if self._sampler is not None:
            return self._sampler
        # Build the context BEFORE taking the lock: context() reads self.vault, which takes
        # the same (non-reentrant) lock.
        ctx = self.context()
        with self._sampler_lock:
            if self._sampler is None:
                if self.sampler_factory is not None:
                    self._sampler = self.sampler_factory(ctx)
                else:
                    from clusterctl.monitor import Sampler

                    self._sampler = Sampler(ctx).start()
            return self._sampler

    def registry(self) -> KeyRegistry:
        return KeyRegistry(self.paths.key_registry, self.paths.audit_log)

    def auto_unlock(self) -> str:
        """Unlock from env var / password file if present. Returns a human-readable outcome."""
        password = stored_master_password(self.paths)
        if not password or not self.paths.vault.exists():
            return "vault locked (unlock it from the dashboard)"
        try:
            self.set_vault(Vault.open(self.paths.vault, password))
        except VaultError as exc:
            return f"vault auto-unlock failed ({exc}); unlock it from the dashboard"
        return "vault unlocked from environment / password file"

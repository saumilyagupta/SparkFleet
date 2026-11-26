"""Shared runtime context passed to every operation (CLI and dashboard)."""

from __future__ import annotations

from dataclasses import dataclass, replace

from clusterctl import inventory as inv
from clusterctl.jump import JumpConnection, resolve
from clusterctl.keys import KeyRegistry, load_control_key
from clusterctl.paths import ProjectPaths
from clusterctl.runner import Reporter
from clusterctl.ssh import HostSession, connect


class VaultLockedError(RuntimeError):
    """An operation needs sudo/login passwords but the vault is not unlocked."""


@dataclass(frozen=True)
class Context:
    paths: ProjectPaths
    inventory: inv.Inventory
    reporter: Reporter
    vault: object | None = None          # clusterctl.vault.Vault
    jump: JumpConnection | None = None

    @classmethod
    def build(cls, paths: ProjectPaths, inventory: inv.Inventory, reporter: Reporter,
              vault: object | None = None) -> "Context":
        jump_alias = inventory.cluster.jump_host
        jump = JumpConnection(resolve(jump_alias)) if jump_alias else None
        return cls(paths, inventory, reporter, vault, jump)

    @property
    def registry(self) -> KeyRegistry:
        return KeyRegistry(self.paths.key_registry, self.paths.audit_log)

    def with_vault(self, vault: object) -> "Context":
        return replace(self, vault=vault)

    def with_reporter(self, reporter: Reporter) -> "Context":
        return replace(self, reporter=reporter)

    def reload_inventory(self) -> "Context":
        return replace(self, inventory=inv.load(self.paths.inventory))

    def password(self, host: inv.Host) -> str:
        if self.vault is None:
            raise VaultLockedError("vault is locked; unlock it to use host passwords")
        secret = self.vault.get(host.name)
        if not secret:
            raise VaultLockedError(
                f"no password stored for '{host.name}'; run 'clusterctl vault set {host.name}'"
            )
        return secret

    def _sock(self, host: inv.Host):
        return self.jump.open_channel(host.ip, host.port) if self.jump else None

    def session(self, host: inv.Host, *, need_sudo: bool = False) -> HostSession:
        """Key-authenticated session. Sudo password comes from the vault when needed."""
        sudo_password = self.password(host) if need_sudo else None
        client = connect(host, self.paths.known_hosts,
                         pkey=load_control_key(self.paths.control_key), sock=self._sock(host))
        return HostSession(client, host, sudo_password)

    def password_session(self, host: inv.Host) -> HostSession:
        """First-contact session using the vault password (bootstrap only)."""
        password = self.password(host)
        client = connect(host, self.paths.known_hosts, password=password,
                         trust_new_host_key=True, sock=self._sock(host))
        return HostSession(client, host, password)

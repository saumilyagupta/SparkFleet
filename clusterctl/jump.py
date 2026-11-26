"""Optional SSH jump host (bastion).

When the cluster machines are not reachable from the control laptop, every
connection is tunnelled through one jump host using SSH 'direct-tcpip'
channels (same as `ssh -J`). The inner SSH session is end-to-end encrypted
between the laptop and the node, so node passwords are never visible on the
jump host. The jump host is configured by alias from ~/.ssh/config.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

import paramiko

from clusterctl.ssh import CONNECT_TIMEOUT, RemoteError

SSH_CONFIG = Path.home() / ".ssh" / "config"
USER_KNOWN_HOSTS = Path.home() / ".ssh" / "known_hosts"


@dataclass(frozen=True)
class JumpSpec:
    alias: str
    hostname: str
    user: str
    port: int
    identity_files: tuple[str, ...]


def resolve(alias: str, config_path: Path = SSH_CONFIG) -> JumpSpec:
    """Resolve an alias (e.g. QWERTY-GPU-clg) or user@host[:port] into connection details."""
    if "@" in alias and not config_path.exists():
        user, _, rest = alias.partition("@")
        host, _, port = rest.partition(":")
        return JumpSpec(alias, host, user, int(port or 22), ())
    cfg = paramiko.SSHConfig.from_path(str(config_path)) if config_path.exists() else paramiko.SSHConfig()
    entry = cfg.lookup(alias)
    if "proxycommand" in entry:
        raise RemoteError(f"jump host '{alias}' uses ProxyCommand, which clusterctl does not support")
    return JumpSpec(
        alias=alias,
        hostname=entry.get("hostname", alias),
        user=entry.get("user", ""),
        port=int(entry.get("port", 22)),
        identity_files=tuple(entry.get("identityfile", [])),
    )


class JumpConnection:
    """One shared SSH connection to the jump host; thread-safe channel opening."""

    def __init__(self, spec: JumpSpec) -> None:
        self.spec = spec
        self._client: paramiko.SSHClient | None = None
        self._lock = threading.Lock()

    def _connect(self) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        if USER_KNOWN_HOSTS.exists():
            client.load_host_keys(str(USER_KNOWN_HOSTS))
        # The jump host must already be trusted by the user's own ssh (known_hosts).
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            client.connect(
                hostname=self.spec.hostname,
                port=self.spec.port,
                username=self.spec.user or None,
                key_filename=list(self.spec.identity_files) or None,
                allow_agent=True,
                look_for_keys=not self.spec.identity_files,
                timeout=CONNECT_TIMEOUT,
                banner_timeout=CONNECT_TIMEOUT,
                auth_timeout=CONNECT_TIMEOUT,
            )
        except paramiko.SSHException as exc:
            client.close()
            raise RemoteError(
                f"jump host {self.spec.alias}: {exc}. Run 'ssh {self.spec.alias}' once manually to trust it."
            ) from exc
        except OSError as exc:
            client.close()
            raise RemoteError(f"cannot reach jump host {self.spec.alias}: {exc}") from exc
        transport = client.get_transport()
        if transport is not None:
            transport.set_keepalive(30)
        return client

    def open_channel(self, ip: str, port: int) -> paramiko.Channel:
        with self._lock:
            transport = self._client.get_transport() if self._client else None
            if transport is None or not transport.is_active():
                self._client = self._connect()
                transport = self._client.get_transport()
            assert transport is not None
            try:
                return transport.open_channel("direct-tcpip", (ip, port), ("127.0.0.1", 0),
                                              timeout=CONNECT_TIMEOUT)
            except paramiko.ChannelException as exc:
                raise RemoteError(f"jump host could not reach {ip}:{port}: {exc}") from exc

    def run(self, command: str, timeout: int = 60) -> str:
        """Run a command on the jump host itself (used for connectivity checks)."""
        with self._lock:
            if self._client is None or not (self._client.get_transport() or None):
                self._client = self._connect()
            client = self._client
        _, stdout, _ = client.exec_command(command, timeout=timeout)
        return stdout.read().decode("utf-8", "replace")

    def close(self) -> None:
        with self._lock:
            if self._client:
                self._client.close()
            self._client = None

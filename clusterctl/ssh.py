"""Thin Paramiko wrapper: run commands, sudo, and file transfer on one host."""

from __future__ import annotations

import io
import posixpath
import shlex
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import paramiko

from clusterctl.inventory import Host

CONNECT_TIMEOUT = 10
DEFAULT_CMD_TIMEOUT = 900


class RemoteError(RuntimeError):
    """A remote command failed or a connection could not be made."""


@dataclass(frozen=True)
class CommandResult:
    code: int
    out: str
    err: str

    @property
    def ok(self) -> bool:
        return self.code == 0


def _ensure_known_hosts(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.touch()


def connect(
    host: Host,
    known_hosts: Path,
    *,
    pkey: paramiko.PKey | None = None,
    password: str | None = None,
    trust_new_host_key: bool = False,
    sock: paramiko.Channel | None = None,
) -> paramiko.SSHClient:
    """Open an SSH connection.

    Host keys are pinned in the project's known_hosts. A new host is only
    accepted when trust_new_host_key is set (first bootstrap). A changed key
    is always rejected.
    """
    _ensure_known_hosts(known_hosts)
    client = paramiko.SSHClient()
    client.load_host_keys(str(known_hosts))
    policy = paramiko.AutoAddPolicy() if trust_new_host_key else paramiko.RejectPolicy()
    client.set_missing_host_key_policy(policy)
    try:
        client.connect(
            hostname=host.ip,
            port=host.port,
            username=host.user,
            pkey=pkey,
            password=password if pkey is None else None,
            look_for_keys=False,
            allow_agent=False,
            timeout=CONNECT_TIMEOUT,
            banner_timeout=CONNECT_TIMEOUT,
            auth_timeout=CONNECT_TIMEOUT,
            sock=sock,
        )
    except paramiko.BadHostKeyException as exc:
        client.close()
        raise RemoteError(
            f"host key for {host.ip} CHANGED. Possible MITM or reinstalled OS. "
            f"If expected, remove its line from {known_hosts} and bootstrap again."
        ) from exc
    except paramiko.SSHException as exc:
        client.close()
        if "not found in known_hosts" in str(exc):
            raise RemoteError(f"{host.ip} not in known_hosts; run 'clusterctl bootstrap' first") from exc
        raise RemoteError(f"SSH to {host.user}@{host.ip}: {exc}") from exc
    except OSError as exc:
        client.close()
        raise RemoteError(f"cannot reach {host.ip}:{host.port}: {exc}") from exc
    if trust_new_host_key:
        client.save_host_keys(str(known_hosts))
    return client


class HostSession:
    """Commands on one host. sudo uses the vault password via stdin (never argv)."""

    def __init__(self, client: paramiko.SSHClient, host: Host, sudo_password: str | None) -> None:
        self.client = client
        self.host = host
        self._sudo_password = sudo_password

    def __enter__(self) -> "HostSession":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.client.close()

    def run(self, command: str, *, check: bool = True, stdin_data: str | None = None,
            timeout: int = DEFAULT_CMD_TIMEOUT) -> CommandResult:
        stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        if stdin_data is not None:
            stdin.write(stdin_data)
            stdin.flush()
        stdin.channel.shutdown_write()
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        code = stdout.channel.recv_exit_status()
        result = CommandResult(code, out, err)
        if check and not result.ok:
            raise RemoteError(f"command failed ({code}): {_short(command)}\n{_short(err or out, 800)}")
        return result

    def stream(self, command: str, on_line: Callable[[str], None],
               timeout: int = 6 * 3600) -> int:
        """Run a command and call on_line for each output line (stdout+stderr merged)."""
        transport = self.client.get_transport()
        if transport is None:
            raise RemoteError("SSH transport closed")
        channel = transport.open_session()
        channel.set_combine_stderr(True)
        channel.settimeout(timeout)
        channel.exec_command(command)
        buffer = b""
        for chunk in iter(lambda: channel.recv(4096), b""):
            buffer += chunk
            *lines, buffer = buffer.split(b"\n")
            for line in lines:
                on_line(line.decode("utf-8", "replace"))
        if buffer:
            on_line(buffer.decode("utf-8", "replace"))
        return channel.recv_exit_status()

    def sudo(self, command: str, *, check: bool = True, timeout: int = DEFAULT_CMD_TIMEOUT) -> CommandResult:
        wrapped = f"bash -c {shlex.quote(command)}"
        if self._sudo_password is None:
            return self.run(f"sudo -n {wrapped}", check=check, timeout=timeout)
        # -k forces sudo to consume the password from stdin every time, so the
        # password line can never leak into the wrapped command's stdin.
        return self.run(f"sudo -k -S -p '' {wrapped}", check=check,
                        stdin_data=self._sudo_password + "\n", timeout=timeout)

    def put_bytes(self, data: bytes, remote_path: str, mode: int = 0o644) -> None:
        with self.client.open_sftp() as sftp:
            with sftp.file(remote_path, "wb") as fh:
                fh.write(data)
            sftp.chmod(remote_path, mode)

    def put_file(self, local: Path, remote_path: str,
                 progress: Callable[[int, int], None] | None = None) -> None:
        with self.client.open_sftp() as sftp:
            sftp.put(str(local), remote_path, callback=progress)

    def get_file(self, remote_path: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        with self.client.open_sftp() as sftp:
            sftp.get(remote_path, str(local))

    def read_text(self, remote_path: str) -> str:
        with self.client.open_sftp() as sftp:
            buf = io.BytesIO()
            sftp.getfo(remote_path, buf)
        return buf.getvalue().decode("utf-8", "replace")

    def sudo_write(self, remote_path: str, content: str, *, mode: str = "644",
                   owner: str = "root") -> bool:
        """Write a root-owned file atomically. Returns True if content changed."""
        tmp = posixpath.join("/tmp", f"clusterctl-{uuid.uuid4().hex}")
        self.put_bytes(content.encode("utf-8"), tmp, 0o600)
        q_tmp, q_dst = shlex.quote(tmp), shlex.quote(remote_path)
        result = self.sudo(
            f"if cmp -s {q_tmp} {q_dst}; then rm -f {q_tmp}; echo same; "
            f"else install -D -m {mode} -o {shlex.quote(owner)} {q_tmp} {q_dst} && rm -f {q_tmp} && echo changed; fi"
        )
        return result.out.strip().endswith("changed")


def _short(text: str, limit: int = 200) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."

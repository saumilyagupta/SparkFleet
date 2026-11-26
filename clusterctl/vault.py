"""Encrypted password store (secrets.vault).

Format: JSON with scrypt parameters, a random salt, and a Fernet token
(AES-128-CBC + HMAC-SHA256) holding {host_name: password}. One master
password unlocks it. The file is useless without that password.
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

VAULT_VERSION = 1
SCRYPT_N = 2**15
SCRYPT_R = 8
SCRYPT_P = 1
SALT_BYTES = 16
MIN_MASTER_PASSWORD = 8


class VaultError(Exception):
    """Raised for a missing vault, wrong master password, or corrupt file."""


def _derive_key(master_password: str, salt: bytes, n: int, r: int, p: int) -> bytes:
    kdf = Scrypt(salt=salt, length=32, n=n, r=r, p=p)
    return base64.urlsafe_b64encode(kdf.derive(master_password.encode("utf-8")))


@dataclass(frozen=True)
class Vault:
    path: Path
    _key: bytes = field(repr=False)
    _salt: bytes = field(repr=False)
    _secrets: Mapping[str, str] = field(default_factory=dict, repr=False)

    @classmethod
    def create(cls, path: Path, master_password: str) -> "Vault":
        if path.exists():
            raise VaultError(f"{path} already exists")
        if len(master_password) < MIN_MASTER_PASSWORD:
            raise VaultError(f"master password must be at least {MIN_MASTER_PASSWORD} characters")
        salt = os.urandom(SALT_BYTES)
        vault = cls(path, _derive_key(master_password, salt, SCRYPT_N, SCRYPT_R, SCRYPT_P), salt, {})
        vault.save()
        return vault

    @classmethod
    def open(cls, path: Path, master_password: str) -> "Vault":
        if not path.exists():
            raise VaultError(f"{path} not found; run 'clusterctl vault init'")
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            salt = base64.b64decode(doc["salt"])
            key = _derive_key(master_password, salt, doc["n"], doc["r"], doc["p"])
            plaintext = Fernet(key).decrypt(doc["data"].encode("ascii"))
        except InvalidToken as exc:
            raise VaultError("wrong master password (or vault file was modified)") from exc
        except (KeyError, ValueError, TypeError) as exc:
            raise VaultError(f"vault file {path} is corrupt: {exc}") from exc
        return cls(path, key, salt, MappingProxyType(json.loads(plaintext)))

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._secrets))

    def get(self, name: str) -> str | None:
        return self._secrets.get(name)

    def with_secret(self, name: str, password: str) -> "Vault":
        if not password:
            raise VaultError("password must not be empty")
        return Vault(self.path, self._key, self._salt, MappingProxyType({**self._secrets, name: password}))

    def without_secret(self, name: str) -> "Vault":
        remaining = {k: v for k, v in self._secrets.items() if k != name}
        return Vault(self.path, self._key, self._salt, MappingProxyType(remaining))

    def save(self) -> None:
        token = Fernet(self._key).encrypt(json.dumps(dict(self._secrets)).encode("utf-8"))
        doc = {
            "version": VAULT_VERSION,
            "kdf": "scrypt",
            "n": SCRYPT_N,
            "r": SCRYPT_R,
            "p": SCRYPT_P,
            "salt": base64.b64encode(self._salt).decode("ascii"),
            "data": token.decode("ascii"),
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        tmp.replace(self.path)

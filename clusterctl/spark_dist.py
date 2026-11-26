"""Download the Spark release once on the laptop and verify its SHA-512."""

from __future__ import annotations

import hashlib
import re
import urllib.request
from pathlib import Path

from clusterctl.inventory import ClusterConfig
from clusterctl.runner import Reporter

MIRRORS = (
    "https://downloads.apache.org/spark",
    "https://archive.apache.org/dist/spark",
)
CHUNK = 1 << 20


class DistError(RuntimeError):
    """Download or checksum failure."""


def tarball_name(cfg: ClusterConfig) -> str:
    return f"{cfg.spark_dirname}.tgz"


def parse_sha512(text: str, filename: str) -> str:
    """Accept both 'HEX  file' and Apache's gpg-style 'file: HEX HEX ...' formats."""
    cleaned = text.replace(filename, " ")
    digest = "".join(re.findall(r"[0-9a-fA-F]", cleaned.split(":", 1)[-1] if ":" in cleaned else cleaned))
    if len(digest) != 128:
        raise DistError(f"could not parse SHA-512 for {filename} (got {len(digest)} hex chars)")
    return digest.lower()


def sha512_file(path: Path) -> str:
    digest = hashlib.sha512()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch(url: str, timeout: int = 60) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - fixed https mirrors
        return resp.read()


def _download(url: str, dest: Path, reporter: Reporter) -> None:
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(url, timeout=60) as resp, tmp.open("wb") as out:  # noqa: S310
        total = int(resp.headers.get("Content-Length") or 0)
        done, last_pct = 0, -10
        for chunk in iter(lambda: resp.read(CHUNK), b""):
            out.write(chunk)
            done += len(chunk)
            pct = int(done * 100 / total) if total else 0
            if pct >= last_pct + 10:
                reporter.info(f"downloading {dest.name}: {pct}%")
                last_pct = pct
    tmp.replace(dest)


def ensure_tarball(cfg: ClusterConfig, cache_dir: Path, reporter: Reporter) -> tuple[Path, str]:
    """Return (local tarball path, verified sha512). Downloads only if not cached."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = tarball_name(cfg)
    dest = cache_dir / name
    errors = []
    for mirror in MIRRORS:
        base = f"{mirror}/spark-{cfg.spark_version}/{name}"
        try:
            expected = parse_sha512(_fetch(base + ".sha512").decode("utf-8", "replace"), name)
        except Exception as exc:  # noqa: BLE001 - try next mirror
            errors.append(f"{mirror}: {exc}")
            continue
        if dest.exists() and sha512_file(dest) == expected:
            reporter.info(f"using cached {name} (checksum ok)")
            return dest, expected
        reporter.info(f"downloading {base}")
        try:
            _download(base, dest, reporter)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{mirror}: {exc}")
            continue
        actual = sha512_file(dest)
        if actual != expected:
            dest.unlink(missing_ok=True)
            raise DistError(f"SHA-512 mismatch for {name}; file deleted")
        reporter.info(f"{name} verified (SHA-512 ok)")
        return dest, expected
    raise DistError("could not download Spark: " + "; ".join(errors))

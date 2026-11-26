"""Copy run results from results/ (gitignored scratch) into docs/results/ (committed).

Only result artifacts are copied (JSON summaries, demo reports and logs, evidence
pages, screenshots). Every file is scanned first: anything containing a vault
password, the vault master password or a private key is refused.
"""

from __future__ import annotations

import html
import json
import re
from urllib.parse import quote
from dataclasses import dataclass
from pathlib import Path

from clusterctl.paths import ProjectPaths

PATTERNS = (
    re.compile(r"^bench-\d{8}-\d{6}\.json$"),          # benchmark summaries (not per-run files)
    re.compile(r"^demo-[\w-]+\.(json|md|log)$"),       # every failure / scenario / failover demo
    re.compile(r"^wordcount-\d{8}-\d{6}\.json$"),      # Word Count run summaries
    re.compile(r"^evidence-\d{8}-\d{6}\.html$"),       # generated evidence reports
    re.compile(r"^(dashboard|evidence)[\w-]*\.png$"),  # screenshots of the real system
)
EXCLUDE = re.compile(r"preview", re.IGNORECASE)        # previews use FAKE data: never archive them
MAX_BYTES = 5 * 1024 * 1024
PRIVATE_KEY = re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----")


@dataclass(frozen=True)
class ArchiveResult:
    copied: tuple[str, ...]
    unchanged: tuple[str, ...]
    refused: tuple[str, ...]


def _variants(secret: str) -> set[bytes]:
    """The secret as it would appear raw, JSON-escaped, HTML-escaped or URL-encoded."""
    forms = {secret, json.dumps(secret)[1:-1], json.dumps(secret, ensure_ascii=False)[1:-1],
             html.escape(secret), html.escape(secret, quote=False), quote(secret, safe="")}
    return {f.encode("utf-8") for f in forms if f}


def _secrets(paths: ProjectPaths, vault_secrets: tuple[str, ...]) -> tuple[bytes, ...]:
    found: set[bytes] = set()
    for secret in vault_secrets:
        if secret:
            found |= _variants(secret)
    pass_file = paths.root / ".vault-pass"
    if pass_file.is_file() and not pass_file.is_symlink():
        found |= _variants(pass_file.read_text(encoding="utf-8").strip())
    return tuple(f for f in found if f)


def wanted(name: str) -> bool:
    return any(p.match(name) for p in PATTERNS) and not EXCLUDE.search(name)


def archive(paths: ProjectPaths, vault_secrets: tuple[str, ...] = ()) -> ArchiveResult:
    src, dst = paths.results_dir, paths.root / "docs" / "results"
    dst.mkdir(parents=True, exist_ok=True)
    secrets = _secrets(paths, vault_secrets)
    copied, unchanged, refused = [], [], []
    for path in sorted(src.glob("*")) if src.is_dir() else []:
        if path.is_symlink() or not path.is_file() or not wanted(path.name):
            continue
        data = path.read_bytes()
        if len(data) > MAX_BYTES:
            refused.append(f"{path.name} (larger than {MAX_BYTES // 1024 // 1024} MB)")
            continue
        if PRIVATE_KEY.search(data) or any(s in data for s in secrets):
            refused.append(f"{path.name} (contains a secret)")
            continue
        target = dst / path.name
        if target.exists() and target.read_bytes() == data:
            unchanged.append(path.name)
            continue
        target.write_bytes(data)  # exactly the bytes that were scanned
        copied.append(path.name)
    return ArchiveResult(tuple(copied), tuple(unchanged), tuple(refused))

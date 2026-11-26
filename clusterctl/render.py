"""Tiny template renderer: replaces {{name}} placeholders. Fails on any missing value."""

from __future__ import annotations

import re
from pathlib import Path

from clusterctl.paths import TEMPLATES_DIR

PLACEHOLDER = re.compile(r"\{\{\s*([a-z_][a-z0-9_]*)\s*\}\}")


class TemplateError(KeyError):
    """A template referenced a value that was not provided."""


def render_text(text: str, values: dict[str, object]) -> str:
    def sub(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise TemplateError(f"template value '{key}' not provided")
        return str(values[key])

    return PLACEHOLDER.sub(sub, text)


def render(template_name: str, values: dict[str, object], templates_dir: Path = TEMPLATES_DIR) -> str:
    text = (templates_dir / template_name).read_text(encoding="utf-8")
    return render_text(text, values).replace("\r\n", "\n")

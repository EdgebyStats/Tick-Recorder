"""Configuration loading + persistence."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from pydantic import ValidationError

from .schema import RecorderConfig


class ConfigError(Exception):
    """Raised when the config file is invalid."""


def load_config(path: str | Path) -> RecorderConfig:
    """Load and validate a recorder configuration file.

    Raises ConfigError on any issue (missing file, invalid JSON, schema violation).
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")

    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise ConfigError(f"invalid JSON in {path}: {e}") from e

    try:
        return RecorderConfig.model_validate(raw)
    except ValidationError as e:
        messages = []
        for err in e.errors():
            loc = ".".join(str(x) for x in err["loc"])
            messages.append(f"  {loc}: {err['msg']}")
        raise ConfigError(
            f"invalid config in {path}:\n" + "\n".join(messages)
        ) from e


def save_config(path: str | Path, config: RecorderConfig) -> None:
    """Persist updated config back to disk (atomic, with .bak backup).

    Note: _comment fields from the original file are NOT preserved — Pydantic
    strips them during loading. If you need to preserve comments, switch to
    TOML or YAML with comment support.
    """
    path = Path(path)
    if path.exists():
        shutil.copyfile(path, str(path) + ".bak")

    tmp = str(path) + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(config.model_dump(mode="json"), fh, indent=2)
    os.replace(tmp, str(path))

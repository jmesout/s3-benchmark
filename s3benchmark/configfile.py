"""Config-file (.toml) support, named profiles, and CLI-override merging.

The config system now resolves settings from three sources, in order of
precedence (highest first):

1. CLI flags
2. Environment variables (including ``.env``)
3. A ``s3benchmark.toml`` config file (optional), with optional named
   ``[profiles.<name>]`` sections.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore

from .config import ConfigError, TransferParams, parse_bool, parse_int_list


def _read_toml(path: str) -> Dict[str, Any]:
    if tomllib is None:
        raise ConfigError("TOML config requires Python 3.11+")
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"Failed to parse {path}: {exc}") from exc


@dataclass
class ConfigFile:
    """Parsed representation of s3benchmark.toml."""

    data: Dict[str, Any] = field(default_factory=dict)
    path: Optional[str] = None

    @classmethod
    def load(cls, path: str) -> ConfigFile:
        return cls(data=_read_toml(path), path=path)

    def profile(self, name: Optional[str]) -> Dict[str, Any]:
        if not name:
            # Merge top-level keys (excluding 'profiles') as defaults.
            return {k: v for k, v in self.data.items() if k != "profiles"}
        profiles = self.data.get("profiles", {})
        if name not in profiles:
            raise ConfigError(f"Profile '{name}' not found in {self.path}")
        # Merge top-level defaults with the named profile.
        merged = {k: v for k, v in self.data.items() if k != "profiles"}
        merged.update(profiles[name])
        return merged

    def file_sizes_mb(self, name: Optional[str]) -> List[int]:
        raw = self.profile(name).get("file_sizes", "")
        if isinstance(raw, str):
            return parse_int_list(raw)
        if isinstance(raw, list):
            return [int(x) for x in raw]
        return []

    def transfer(self, name: Optional[str]) -> TransferParams:
        p = self.profile(name)
        return TransferParams(
            multipart_threshold=int(p.get("multipart_threshold", 50 * 1024 * 1024)),
            max_concurrency=int(p.get("max_concurrency", 10)),
            multipart_chunksize=int(p.get("multipart_chunksize", 50 * 1024 * 1024)),
            use_threads=parse_bool(str(p.get("use_threads", "true"))),
        )


def find_config_file(cwd: str | None = None) -> Optional[str]:
    """Locate an s3benchmark.toml in cwd or its parents."""
    d = os.path.abspath(cwd or os.getcwd())
    while True:
        candidate = os.path.join(d, "s3benchmark.toml")
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent

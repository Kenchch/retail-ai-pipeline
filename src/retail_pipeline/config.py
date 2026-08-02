"""Configuration loading and shared logging setup."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config.yaml"


@dataclass
class Config:
    """Typed view over config.yaml with paths resolved to absolute."""

    paths: dict[str, Path]
    extract: dict[str, Any]
    quality: dict[str, Any]
    recommend: dict[str, Any]
    adoption: dict[str, Any]
    root: Path = field(default=PROJECT_ROOT)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        cfg_path = Path(path) if path else DEFAULT_CONFIG
        with open(cfg_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        root = cfg_path.resolve().parent
        paths = {k: (root / v).resolve() for k, v in raw["paths"].items()}
        return cls(
            paths=paths,
            extract=raw["extract"],
            quality=raw["quality"],
            recommend=raw["recommend"],
            adoption=cls._resolve_adoption(raw.get("adoption", {})),
            root=root,
        )

    @staticmethod
    def _resolve_adoption(adoption: dict[str, Any]) -> dict[str, Any]:
        """Licensed headcount is derived from the roster, never stored beside it.

        A total that is maintained separately from the per-team numbers is a
        total that will eventually disagree with them.
        """
        roster = adoption.get("roster") or {}
        adoption = dict(adoption)
        adoption["roster"] = {str(k): int(v) for k, v in roster.items()}
        adoption["licensed_users"] = sum(adoption["roster"].values())
        return adoption

    def ensure_dirs(self) -> None:
        self.paths["processed"].mkdir(parents=True, exist_ok=True)
        self.paths["reports"].mkdir(parents=True, exist_ok=True)
        self.paths["warehouse"].parent.mkdir(parents=True, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """One logger config for the whole pipeline, so every stage logs the same way."""
    logger = logging.getLogger(name)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-7s | %(name)-28s | %(message)s",
            datefmt="%H:%M:%S",
        )
    return logger

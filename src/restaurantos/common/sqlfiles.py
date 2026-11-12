"""Access to the hand-written SQL in db/. Standard library only."""

from __future__ import annotations

import os
from functools import cache
from pathlib import Path
from typing import Literal

NodeKind = Literal["region", "hq"]


def db_dir() -> Path:
    """The db/ folder: $ROS_DB_DIR if set, else <repo root>/db."""
    override = os.environ.get("ROS_DB_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "db"


def schema_files(kind: NodeKind) -> list[Path]:
    """Schema files for one node kind, in the order they must be applied.

    Shared files first, then the kind's own, each sorted by file name.
    The baseline node uses kind="region".
    """
    root = db_dir()
    return [*sorted((root / "shared").glob("*.sql")), *sorted((root / kind).glob("*.sql"))]


@cache
def load_query(name: str) -> str:
    """Text of db/queries/<name>.sql, e.g. load_query("region/place_order")."""
    path = db_dir() / "queries" / f"{name}.sql"
    return path.read_text(encoding="utf-8")

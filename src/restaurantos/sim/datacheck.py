"""Check the data directory against MANIFEST.csv before any load.

MANIFEST.csv lists every dataset file with its row count and its sha256,
so a truncated download or a stale mix of files fails before half a
cluster has been loaded. Row counts are counted with the csv reader, not
by lines, because quoted fields can contain newlines.
"""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from restaurantos.sim.loading import REFERENCE_ORDER

MANIFEST_NAME = "MANIFEST.csv"
MANIFEST_COLUMNS = ("file", "rows", "sha256")
CHUNK = 1 << 20

# Every dataset file bootstrap actually reads: the six reference tables
# (sim/loading, which also feeds the HQ and baseline loads), plus the two
# order files that warm_up_stock measures usage from (sim/stock). The
# shard step reads vendors.csv, which is one of the six. A file that is
# missing from MANIFEST.csv would otherwise load unchecked, so the check
# refuses unless every required file is both listed and verified.
REQUIRED_FILES: frozenset[str] = frozenset(REFERENCE_ORDER) | {
    "orders.csv",
    "order_items.csv",
}


class DataCheckError(RuntimeError):
    """The data directory does not match its manifest."""


def _count_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def check_manifest(
    data_dir: Path, *, required: frozenset[str] = REQUIRED_FILES
) -> list[tuple[str, int]]:
    """Verify every manifest entry against the files on disk, and that
    every file bootstrap reads is among the verified entries.

    Returns (file, rows) for each entry, so the caller can print the row
    counts it just verified. Raises DataCheckError naming every problem.
    """
    manifest = data_dir / MANIFEST_NAME
    if not manifest.is_file():
        raise DataCheckError(
            f"{manifest} does not exist; the data directory must hold the "
            "dataset and its manifest (see docs/setup.md for where it comes from)"
        )
    problems: list[str] = []
    checked: list[tuple[str, int]] = []
    with manifest.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if tuple(header or ()) != MANIFEST_COLUMNS:
            raise DataCheckError(
                f"{MANIFEST_NAME}: header is {header}, expected {list(MANIFEST_COLUMNS)}"
            )
        for row in reader:
            if not row:
                continue
            if len(row) != 3:
                problems.append(
                    f"line {reader.line_num}: {len(row)} fields, expected 3 "
                    f"({'file, rows, sha256'}): {row}"
                )
                continue
            name, expected_rows, expected_hash = row
            path = data_dir / name
            if not path.is_file():
                problems.append(f"{name}: file is missing")
                continue
            digest = _sha256(path)
            if digest != expected_hash:
                problems.append(f"{name}: sha256 is {digest}, expected {expected_hash}")
            rows = _count_rows(path)
            if str(rows) != expected_rows:
                problems.append(f"{name}: {rows} rows, expected {expected_rows}")
            checked.append((name, rows))
    missing = required - {name for name, _ in checked}
    if missing:
        problems.append("required files not listed in the manifest: " + ", ".join(sorted(missing)))
    if problems:
        raise DataCheckError(
            f"the data directory does not match {MANIFEST_NAME}:\n  " + "\n  ".join(problems)
        )
    return checked

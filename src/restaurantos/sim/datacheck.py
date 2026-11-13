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

MANIFEST_NAME = "MANIFEST.csv"
MANIFEST_COLUMNS = ("file", "rows", "sha256")
CHUNK = 1 << 20


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


def check_manifest(data_dir: Path) -> list[tuple[str, int]]:
    """Verify every manifest entry against the files on disk.

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
    if problems:
        raise DataCheckError(
            f"the data directory does not match {MANIFEST_NAME}:\n  " + "\n  ".join(problems)
        )
    return checked

"""Assign geohash cells to regions (bootstrap step 7).

Count vendors per cell from vendors.csv — never order data — sort the
cells, and cut the sorted list into N contiguous ranges with roughly
equal vendor counts. The sort field is the cell name itself,
lexicographic: a geohash encodes latitude and longitude interleaved in
base32, so string order is a spatial Z-order and each range stays
spatially compact, which is what makes the border and nearby queries hit
a real boundary.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path


def vendor_counts(data_dir: Path, *, column: str = "geohash") -> dict[str, int]:
    """Cell name → number of vendors whose home cell it is."""
    counts: dict[str, int] = {}
    with (data_dir / "vendors.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            counts[row[column]] = counts.get(row[column], 0) + 1
    return counts


def assign_shards(counts: dict[str, int], regions: Sequence[str]) -> list[tuple[str, str]]:
    """(cell, region_id) for every cell, in Z-order, in roughly equal
    vendor-count ranges: one range per region, contiguous in cell name.

    Greedy cut: walk the sorted cells and close a region's range once the
    running total has passed that region's share of all vendors. The last
    region takes whatever remains, so no vendor is dropped and no region
    can end up empty while cells remain.
    """
    cells = sorted(counts)
    total = sum(counts.values())
    if not cells:
        raise ValueError("no vendors: the shard directory cannot be built")
    if len(regions) > len(cells):
        raise ValueError(
            f"{len(regions)} regions but only {len(cells)} cells: a region needs at least one cell"
        )
    shares = [total * (i + 1) / len(regions) for i in range(len(regions))]
    out: list[tuple[str, str]] = []
    running = 0
    region = 0
    for cell in cells:
        out.append((cell, regions[region]))
        running += counts[cell]
        if region < len(regions) - 1 and running >= shares[region]:
            region += 1
    return out

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

from collections.abc import Iterable, Sequence
from pathlib import Path

from restaurantos.sim.loading import REFERENCE_COLUMNS, read_rows


def counts_by_cell(pairs: Iterable[tuple[str, str]]) -> dict[str, int]:
    """Cell name -> number of vendors whose home cell it is, from
    (vendor_id, geohash) pairs. Both bootstrap (through vendor_counts)
    and playback count through this one function, so the two cannot
    drift apart (review round 3, item 11)."""
    counts: dict[str, int] = {}
    for _vendor, cell in pairs:
        counts[cell] = counts.get(cell, 0) + 1
    return counts


def vendor_pairs(rows: Iterable[list[str]]) -> list[tuple[str, str]]:
    """(vendor_id, geohash) from vendors.csv rows, each row checked
    (review round 4, item 7): a wrong value count first, then an empty
    vendor_id, each a ValueError with its own message and the line
    number (the header is line 1). Both playback's restock list and
    bootstrap's vendor_counts go through here, so the two paths give
    the same errors."""
    columns = REFERENCE_COLUMNS["vendors.csv"]
    geohash_column = columns.index("geohash")
    pairs: list[tuple[str, str]] = []
    for number, row in enumerate(rows, start=2):  # the header is line 1
        if len(row) != len(columns):
            raise ValueError(
                f"vendors.csv line {number}: expected {len(columns)} values, got {len(row)}"
            )
        if not row[0]:
            raise ValueError(f"vendors.csv line {number}: empty vendor_id")
        pairs.append((row[0], row[geohash_column]))
    return pairs


def vendor_counts(data_dir: Path) -> dict[str, int]:
    """Cell name -> number of vendors whose home cell it is:
    bootstrap's own read of vendors.csv, through the same checked
    `vendor_pairs` the playback side uses (review round 4, item 7).
    A broken header is a ValueError naming the file, like every other
    data error, not a RuntimeError escaping the CLI; any other
    RuntimeError passes through unchanged."""
    try:
        rows = list(read_rows(data_dir / "vendors.csv", REFERENCE_COLUMNS["vendors.csv"]))
    except RuntimeError as error:
        if "header is" in str(error):
            raise ValueError(str(error)) from None
        raise
    return counts_by_cell(vendor_pairs(rows))


def assign_shards(counts: dict[str, int], regions: Sequence[str]) -> list[tuple[str, str]]:
    """(cell, region_id) for every cell, in Z-order, in roughly equal
    vendor-count ranges: one range per region, contiguous in cell name.

    Greedy cut: walk the sorted cells and close a region's range once the
    running total has passed that region's share of all vendors. A range
    also closes when the cells still unassigned are exactly the regions
    still to fill, so every region gets at least one cell even when the
    running total has not reached its share yet. The last region takes
    whatever remains, so no vendor is dropped either.
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
    for index, cell in enumerate(cells):
        out.append((cell, regions[region]))
        running += counts[cell]
        cells_left = len(cells) - index - 1
        regions_left = len(regions) - region - 1
        if region < len(regions) - 1 and (running >= shares[region] or cells_left == regions_left):
            region += 1
    assigned = {rid for _, rid in out}
    if assigned != set(regions):
        raise RuntimeError(
            f"shard assignment left {sorted(set(regions) - assigned)} empty; this is a bug"
        )
    return out

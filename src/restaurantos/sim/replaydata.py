"""The sorted order stream that playback and the oracle both read.

`orders.csv` and `order_items.csv` are joined once, sorted by
(order_day, order_time, order_id as a number), and cached as one JSONL
file: one line per order, its items in file order (which is where
`line_no` comes from when the order is placed). Sorting 2M orders and
4M order lines every run is minutes of pure waste, and the brief says
to sort once and cache.

The cache lives under `<data_dir>/cached/`, next to the files it is
derived from: `data/` is git-ignored wholesale, and every worktree
symlinks the same `data/`, so one cache is shared and can never be
committed. Validity is checked against `MANIFEST.csv`: the meta sidecar
records the sha256 of the two source files as the manifest lists them,
the warm-up window, and the row count, and any mismatch rebuilds.

Sorting is done through a throwaway SQLite database (stdlib, no new
dependency): it streams the join in index order without ever holding
the whole dataset in memory, and it spills to disk when the ORDER BY
does not fit. `order_id` is sorted with CAST(order_id AS INTEGER), per
the data dictionary's "compared as a number"; the text value is the
final tiebreak, so the mini fixture's `o0001`-style ids sort
deterministically too.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import time as dt_time
from pathlib import Path

from restaurantos.common.models import OrderRequest
from restaurantos.sim.datacheck import check_manifest
from restaurantos.sim.loading import REFERENCE_COLUMNS, check_header, read_rows

CACHE_DIR_NAME = "cached"
CACHE_FILE = "orders-sorted.jsonl"
CACHE_META = "orders-sorted.meta.json"
CHANNEL = "dhrd"  # the data dictionary: every request carries this constant

ORDERS_FILE = "orders.csv"
ITEMS_FILE = "order_items.csv"
ORDERS_COLUMNS = (
    "order_id",
    "customer_id",
    "vendor_id",
    "geohash",
    "day_of_week",
    "order_day",
    "order_time",
)
ITEMS_COLUMNS = ("order_id", "product_id", "quantity")

IMPORT_BATCH = 100_000


@dataclass(frozen=True)
class ReplayWindow:
    """The replay window a run will actually use, after validation.

    `to_day` is the effective end: the caller's to_day, the cache's
    last day when the caller left it open, or the cache's last day
    again when the caller asked for more than the data holds - the
    last case is reported in `shortened_from` so the caller can say
    so; this module never prints.
    """

    from_day: int
    to_day: int
    shortened_from: int | None = None

    def __iter__(self) -> Iterator[int]:
        # unpacking stays legal for the callers that only want the days
        yield self.from_day
        yield self.to_day


@dataclass(frozen=True)
class CacheMeta:
    """What the cache was built from; enough to decide it is stale."""

    orders_sha256: str
    order_items_sha256: str
    warm_up_days: int
    first_day: int
    last_day: int
    orders: int
    itemless_orders: int

    def to_json(self) -> str:
        return json.dumps(self.__dict__, indent=2, sort_keys=True) + "\n"


def cache_paths(data_dir: Path) -> tuple[Path, Path, Path]:
    """(cache file, meta file, cache directory) for a data directory."""
    cache_dir = data_dir / CACHE_DIR_NAME
    return cache_dir / CACHE_FILE, cache_dir / CACHE_META, cache_dir


def _manifest_sha256(data_dir: Path) -> dict[str, str]:
    """The sha256 column of MANIFEST.csv, without hashing any data file.

    The manifest is the trust anchor: `build_cache` verified the files
    against it, so binding the cache to the manifest (cheap, text) is
    enough on every later read. If the files were swapped without
    updating the manifest, that is a broken data directory, and the next
    bootstrap's full check_manifest rejects it.
    """
    with (data_dir / "MANIFEST.csv").open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header: file, rows, sha256
        entries: dict[str, str] = {}
        for row in reader:
            if len(row) == 3:
                entries[row[0]] = row[2]
        return entries


def manifest_rows(data_dir: Path, name: str) -> list[list[str]]:
    """One read of one data file, checked before anything is parsed
    from it: the sha256 matches its MANIFEST.csv entry, the header
    matches `REFERENCE_COLUMNS`, and the rows come back so every
    derived thing (vendor list, cell counts, region map) is built from
    these same checked bytes - never from a second, unchecked read
    (review round 3, item 11).

    Blank rows are kept: the caller numbers them - the header is
    line 1 - and rejects them. An empty file is a data error too, not
    a StopIteration escaping the CLI (review round 4, item 7):
    `next(reader, [])` lets check_header report the missing header,
    and the header error is a ValueError naming the file, like every
    other data error."""
    expected = _manifest_sha256(data_dir).get(name)
    if expected is None:
        raise ValueError(f"{name} is not listed in MANIFEST.csv")
    raw = (data_dir / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError(f"{name} does not match its MANIFEST.csv entry")
    reader = csv.reader(io.StringIO(raw.decode("utf-8"), newline=""))
    try:
        check_header(name, next(reader, []), REFERENCE_COLUMNS[name])
    except RuntimeError as error:
        raise ValueError(str(error)) from None
    return list(reader)


def load_meta(data_dir: Path) -> CacheMeta | None:
    """The cache's meta, or None when there is nothing usable."""
    _, meta_path, _ = cache_paths(data_dir)
    if not meta_path.is_file():
        return None
    try:
        raw = json.loads(meta_path.read_text(encoding="utf-8"))
        return CacheMeta(
            orders_sha256=raw["orders_sha256"],
            order_items_sha256=raw["order_items_sha256"],
            warm_up_days=int(raw["warm_up_days"]),
            first_day=int(raw["first_day"]),
            last_day=int(raw["last_day"]),
            orders=int(raw["orders"]),
            itemless_orders=int(raw["itemless_orders"]),
        )
    except KeyError, ValueError, TypeError:
        return None


def meta_is_current(data_dir: Path, meta: CacheMeta) -> bool:
    """True when the cache's source files still match the manifest.

    The warm-up window is compared by the caller, which knows the W it
    was asked for; this only binds the cache to its inputs.
    """
    manifest = _manifest_sha256(data_dir)
    return (
        manifest.get(ORDERS_FILE) == meta.orders_sha256
        and manifest.get(ITEMS_FILE) == meta.order_items_sha256
    )


def _settings_warm_up_days() -> int:
    # Late import: sim.config pulls pydantic-settings; replaydata is also
    # used by tests that build the cache with an explicit window.
    from restaurantos.sim.config import get_sim_settings

    return get_sim_settings().stock_warm_up_days


def _import_orders(conn: sqlite3.Connection, data_dir: Path, warm_up_days: int) -> None:
    batch: list[tuple[str, str, str, str, int, str]] = []
    for row in read_rows(data_dir / ORDERS_FILE, ORDERS_COLUMNS):
        day = int(row[5])
        if day < warm_up_days:
            continue  # the warm-up window is never replayed
        batch.append((row[0], row[1], row[2], row[3], day, row[6]))
        if len(batch) >= IMPORT_BATCH:
            conn.executemany(
                "INSERT INTO orders (order_id, customer_id, vendor_id, geohash, "
                "order_day, order_time) VALUES (?, ?, ?, ?, ?, ?)",
                batch,
            )
            batch.clear()
    if batch:
        conn.executemany(
            "INSERT INTO orders (order_id, customer_id, vendor_id, geohash, "
            "order_day, order_time) VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )


def _import_items(conn: sqlite3.Connection, data_dir: Path) -> None:
    batch: list[tuple[str, str, int]] = []
    for row in read_rows(data_dir / ITEMS_FILE, ITEMS_COLUMNS):
        batch.append((row[0], row[1], int(row[2])))
        if len(batch) >= IMPORT_BATCH:
            conn.executemany(
                "INSERT INTO items (order_id, product_id, quantity) VALUES (?, ?, ?)", batch
            )
            batch.clear()
    if batch:
        conn.executemany(
            "INSERT INTO items (order_id, product_id, quantity) VALUES (?, ?, ?)", batch
        )


def _write_jsonl(out: Path, conn: sqlite3.Connection) -> tuple[int, int, int, int]:
    """Stream the sorted join out; returns (orders, itemless, first, last).

    Orders without items never reach the join (a request needs at
    least one line), so the itemless count is the imported total minus
    the written count - not a NOT EXISTS scan over both tables, which
    is quadratic on the real dataset.
    """
    orders = 0
    first_day: int | None = None
    last_day: int | None = None
    current: dict | None = None
    with out.open("w", encoding="utf-8") as f:
        cursor = conn.execute(
            """
            SELECT o.order_id, o.customer_id, o.vendor_id, o.geohash,
                   o.order_day, o.order_time, i.product_id, i.quantity
            FROM orders o JOIN items i ON i.order_id = o.order_id
            ORDER BY o.order_day, o.order_time,
                     CAST(o.order_id AS INTEGER), o.order_id, i.seq
            """
        )
        for order_id, customer_id, vendor_id, geohash, day, order_time, product, qty in cursor:
            if current is None or current["order_id"] != order_id:
                if current is not None:
                    f.write(json.dumps(current, separators=(",", ":")) + "\n")
                    orders += 1
                current = {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "vendor_id": vendor_id,
                    "geohash": geohash,
                    "order_day": day,
                    "order_time": order_time,
                    "items": [],
                }
                first_day = day if first_day is None else first_day
                last_day = day
            current["items"].append({"product_id": product, "quantity": qty})
        if current is not None:
            f.write(json.dumps(current, separators=(",", ":")) + "\n")
            orders += 1
    # Orders without items cannot be sent (a request needs at least one
    # line); count them so a dataset change shows up loudly. The join
    # drops them, so: imported total minus written count.
    (imported,) = conn.execute("SELECT COUNT(*) FROM orders").fetchone()
    if first_day is None or last_day is None:
        raise ValueError(
            f"no orders on or after the warm-up window: {ORDERS_FILE} holds "
            "nothing that could be replayed"
        )
    return orders, imported - orders, first_day, last_day


# A killed build's temp files are only removed by a later build. A real
# build takes about half a minute, so an hour of grace cannot touch a
# live concurrent build.
_STALE_TEMP_AFTER_S = 3600.0


def _remove_stale_temps(cache_dir: Path) -> None:
    """Sweep temp files from builds that were hard-killed: the next
    build has a new pid and would otherwise never clean them (the
    .sort-<pid>.db of a killed real-data build is hundreds of MB)."""
    cutoff = time.time() - _STALE_TEMP_AFTER_S
    for path in cache_dir.iterdir():
        name = path.name
        is_temp = (name.startswith(".sort-") and name.endswith(".db")) or name.endswith(".tmp")
        if not is_temp:
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except FileNotFoundError:
            pass  # a concurrent sweep got there first; fine


def build_cache(data_dir: Path, *, warm_up_days: int, quiet: bool = False) -> CacheMeta:
    """Verify the data directory and (re)build the sorted order cache.

    Raises DataCheckError when the files do not match MANIFEST.csv; the
    build never starts from data it has not verified.
    """
    check_manifest(data_dir)
    manifest = _manifest_sha256(data_dir)
    cache_file, meta_path, cache_dir = cache_paths(data_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    _remove_stale_temps(cache_dir)

    # Pid-suffixed temp names: two builds at once must not write the
    # same temp files and install a mix of both under a valid sidecar.
    pid = os.getpid()
    sort_db = cache_dir / f".sort-{pid}.db"
    tmp_cache = cache_file.with_name(f"{cache_file.name}.{pid}.tmp")
    tmp_meta = meta_path.with_name(f"{meta_path.name}.{pid}.tmp")

    # a build that was interrupted leaves a half-written database
    # behind; every build starts from nothing
    sort_db.unlink(missing_ok=True)
    try:
        conn = sqlite3.connect(sort_db)
        try:
            conn.execute("PRAGMA journal_mode = OFF")
            conn.execute("PRAGMA synchronous = OFF")
            conn.execute(
                "CREATE TABLE orders (order_id TEXT PRIMARY KEY, customer_id TEXT, "
                "vendor_id TEXT, geohash TEXT, order_day INTEGER, order_time TEXT)"
            )
            conn.execute(
                "CREATE TABLE items (seq INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT, "
                "product_id TEXT, quantity INTEGER)"
            )
            _import_orders(conn, data_dir, warm_up_days)
            _import_items(conn, data_dir)
            orders, itemless, first_day, last_day = _write_jsonl(tmp_cache, conn)
        finally:
            conn.close()
            sort_db.unlink(missing_ok=True)

        meta = CacheMeta(
            orders_sha256=manifest[ORDERS_FILE],
            order_items_sha256=manifest[ITEMS_FILE],
            warm_up_days=warm_up_days,
            first_day=first_day,
            last_day=last_day,
            orders=orders,
            itemless_orders=itemless,
        )
        tmp_meta.write_text(meta.to_json(), encoding="utf-8")
        # Publish order: drop the old sidecar first, then install the
        # new cache, then the new sidecar. A crash between the renames
        # then leaves a cache with no sidecar, which ensure_cache reads
        # as "no usable cache" and rebuilds - instead of a new cache
        # quietly described by the old sidecar.
        meta_path.unlink(missing_ok=True)
        tmp_cache.replace(cache_file)
        tmp_meta.replace(meta_path)
    except BaseException:
        # a failed build must not leave temp files behind (renamed or
        # not), or the next build could install a half-written cache
        tmp_cache.unlink(missing_ok=True)
        tmp_meta.unlink(missing_ok=True)
        raise
    if not quiet:
        note = f" ({itemless} orders had no items and were dropped)" if itemless else ""
        print(
            f"sorted order cache built: {orders} orders, days {first_day} to "
            f"{last_day}, {cache_file}{note}"
        )
    return meta


def ensure_cache(data_dir: Path, *, warm_up_days: int | None = None) -> CacheMeta:
    """The current cache, building it first if missing or stale."""
    if warm_up_days is None:
        warm_up_days = _settings_warm_up_days()
    meta = load_meta(data_dir)
    cache_file, _, _ = cache_paths(data_dir)
    if (
        meta is not None
        and cache_file.is_file()  # a valid sidecar with a deleted cache is still stale
        and meta.warm_up_days == warm_up_days
        and meta_is_current(data_dir, meta)
    ):
        return meta
    return build_cache(data_dir, warm_up_days=warm_up_days)


def iter_orders(
    cache_file: Path,
    *,
    from_day: int | None = None,
    to_day: int | None = None,
) -> Iterator[dict]:
    """Yield cached orders as plain dicts, in replay order, streamed."""
    with cache_file.open(encoding="utf-8") as f:
        for line in f:
            order = json.loads(line)
            day = order["order_day"]
            if from_day is not None and day < from_day:
                continue
            if to_day is not None and day > to_day:
                return
            yield order


def to_order_request(order: dict) -> OrderRequest:
    """The data dictionary's mapping of one cached order to a POST body."""
    return OrderRequest(
        channel=CHANNEL,
        external_order_id=str(order["order_id"]),
        vendor_id=order["vendor_id"],
        customer_id=order["customer_id"],
        customer_geohash=order["geohash"],
        order_day=order["order_day"],
        order_time=dt_time.fromisoformat(order["order_time"]),
        items=order["items"],
    )


def require_window(meta: CacheMeta, from_day: int, to_day: int | None) -> ReplayWindow:
    """Validate the replay window against the cache, or raise ValueError.

    from_day below the warm-up window is refused, not clamped: those
    orders are already baked into the starting stock (bootstrap measured
    its usage), so replaying them would count every ingredient twice
    and no oracle could ever match. The two settings stay decoupled;
    this only rejects the combination that cannot mean anything.

    A to_day past the last cached day is shortened to it and reported
    in `shortened_from`, not refused: the caller prints the warning,
    never this module. Days with no orders inside the window stay
    empty, but the window itself never runs past the data.
    """
    if from_day < max(meta.warm_up_days, meta.first_day):
        raise ValueError(
            f"from_day {from_day} is inside the warm-up window: days before "
            f"{max(meta.warm_up_days, meta.first_day)} set the starting stock "
            "and are never replayed"
        )
    if from_day > meta.last_day:
        raise ValueError(
            f"from_day {from_day} is after the last cached day ({meta.last_day}); "
            "that run would replay nothing"
        )
    if to_day is not None and to_day < from_day:
        raise ValueError(f"to_day {to_day} is before from_day {from_day}")
    shortened_from = None
    if to_day is not None and to_day > meta.last_day:
        shortened_from = to_day
        to_day = meta.last_day
    return ReplayWindow(
        from_day=from_day,
        to_day=to_day if to_day is not None else meta.last_day,
        shortened_from=shortened_from,
    )

"""The sorted order cache: warm-up exclusion, numeric ordering, the
join, staleness against the manifest, and the window rules.

Each test builds its own tiny data directory (all eight dataset files
plus a correct MANIFEST, hashed like the real one), so the cache is
exercised through exactly the path playback uses, hermetically.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from restaurantos.sim.datacheck import DataCheckError
from restaurantos.sim.loading import REFERENCE_COLUMNS
from restaurantos.sim.replaydata import (
    ITEMS_COLUMNS,
    ORDERS_COLUMNS,
    build_cache,
    ensure_cache,
    iter_orders,
    load_meta,
    meta_is_current,
    require_window,
    to_order_request,
)

ORDERS_HEADER = ",".join(ORDERS_COLUMNS)
ITEMS_HEADER = ",".join(ITEMS_COLUMNS)


def _write_csv(path: Path, header: str, rows: list[list[object]]) -> None:
    lines = [header] + [",".join(str(value) for value in row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_data_dir(root: Path, orders: list[list[object]], items: list[list[object]]) -> Path:
    """A data directory that passes check_manifest: the six reference
    files (header-only: nothing reads them here), the two order files,
    and a MANIFEST with real row counts and sha256s."""
    data = root / "data"
    data.mkdir(parents=True)
    names: list[str] = []
    for name, columns in REFERENCE_COLUMNS.items():
        (data / name).write_text(",".join(columns) + "\n", encoding="utf-8")
        names.append(name)
    _write_csv(data / "orders.csv", ORDERS_HEADER, orders)
    _write_csv(data / "order_items.csv", ITEMS_HEADER, items)
    manifest = ["file,rows,sha256"]
    for name in [*names, "orders.csv", "order_items.csv"]:
        raw = (data / name).read_bytes()
        rows = len(raw.decode().strip().splitlines()) - 1
        digest = hashlib.sha256(raw).hexdigest()
        manifest.append(f"{name},{rows},{digest}")
    (data / "MANIFEST.csv").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    return data


def cache_lines(data_dir: Path) -> list[dict]:
    cache_file = data_dir / "cached" / "orders-sorted.jsonl"
    return [json.loads(line) for line in cache_file.read_text().splitlines()]


def test_warm_up_days_are_excluded_and_order_is_numeric(tmp_path: Path) -> None:
    # order_id is compared as a NUMBER: text order would sort "10"
    # before "9". Day 0 is warm-up, whatever its time.
    orders = [
        ["9", "c1", "v1", "w21z7", "3", 2, "00:00:00"],
        ["10", "c1", "v1", "w21z7", "3", 2, "00:00:00"],
        ["2", "c1", "v1", "w21z7", "3", 1, "23:59:59"],
        ["1", "c1", "v1", "w21z7", "3", 0, "00:00:00"],
    ]
    items = [
        ["9", "p1", 1],
        ["9", "p2", 2],
        ["10", "p1", 1],
        ["2", "p1", 1],
    ]
    data = make_data_dir(tmp_path, orders, items)
    meta = build_cache(data, warm_up_days=1)

    lines = cache_lines(data)
    assert [line["order_id"] for line in lines] == ["2", "9", "10"]
    assert meta.first_day == 1
    assert meta.last_day == 2
    assert meta.orders == 3
    # day ordering first, then time, then numeric id
    assert lines[0]["order_day"] == 1 and lines[0]["order_time"] == "23:59:59"
    assert [line["order_id"] for line in lines[1:]] == ["9", "10"]
    # items stay in file order (line_no comes from this order)
    assert lines[1]["items"] == [
        {"product_id": "p1", "quantity": 1},
        {"product_id": "p2", "quantity": 2},
    ]


def test_build_is_deterministic(tmp_path: Path) -> None:
    orders = [[str(i), "c1", "v1", "w21z7", "3", 14, "12:00:00"] for i in range(20)]
    items = [[str(i), "p1", 1] for i in range(20)]
    data_a = make_data_dir(tmp_path / "a", orders, items)
    data_b = make_data_dir(tmp_path / "b", orders, items)
    build_cache(data_a, warm_up_days=14, quiet=True)
    build_cache(data_b, warm_up_days=14, quiet=True)
    assert (data_a / "cached" / "orders-sorted.jsonl").read_bytes() == (
        data_b / "cached" / "orders-sorted.jsonl"
    ).read_bytes()


def test_itemless_orders_are_dropped_and_counted(tmp_path: Path) -> None:
    orders = [
        ["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"],
        ["2", "c1", "v1", "w21z7", "3", 14, "13:00:00"],
    ]
    items = [["1", "p1", 1]]
    data = make_data_dir(tmp_path, orders, items)
    meta = build_cache(data, warm_up_days=14)
    lines = cache_lines(data)
    assert [line["order_id"] for line in lines] == ["1"]
    assert meta.orders == 1
    assert meta.itemless_orders == 1


def test_cache_is_rebuilt_when_the_manifest_moves(tmp_path: Path) -> None:
    orders = [["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"]]
    items = [["1", "p1", 1]]
    data = make_data_dir(tmp_path, orders, items)
    meta = build_cache(data, warm_up_days=14)
    assert meta_is_current(data, meta)

    # the source changes, and the manifest says so: the cache is stale
    orders.append(["2", "c1", "v1", "w21z7", "3", 14, "12:30:00"])
    items.append(["2", "p1", 1])
    _write_csv(data / "orders.csv", ORDERS_HEADER, orders)
    _write_csv(data / "order_items.csv", ITEMS_HEADER, items)
    raw_orders = (data / "orders.csv").read_bytes()
    raw_items = (data / "order_items.csv").read_bytes()
    manifest = (data / "MANIFEST.csv").read_text().splitlines()
    # the six reference files come first, so the order files are lines 7 and 8
    manifest[7] = f"orders.csv,2,{hashlib.sha256(raw_orders).hexdigest()}"
    manifest[8] = f"order_items.csv,2,{hashlib.sha256(raw_items).hexdigest()}"
    (data / "MANIFEST.csv").write_text("\n".join(manifest) + "\n")

    assert not meta_is_current(data, meta)
    rebuilt = ensure_cache(data, warm_up_days=14)
    assert rebuilt.orders == 2
    assert [line["order_id"] for line in cache_lines(data)] == ["1", "2"]


def test_new_warm_up_window_rebuilds(tmp_path: Path) -> None:
    orders = [
        ["1", "c1", "v1", "w21z7", "3", 13, "12:00:00"],
        ["2", "c1", "v1", "w21z7", "3", 14, "12:00:00"],
    ]
    items = [["1", "p1", 1], ["2", "p1", 1]]
    data = make_data_dir(tmp_path, orders, items)
    build_cache(data, warm_up_days=14)
    assert [line["order_id"] for line in cache_lines(data)] == ["2"]
    ensure_cache(data, warm_up_days=13)
    lines = cache_lines(data)
    assert [line["order_id"] for line in lines] == ["1", "2"]
    assert load_meta(data).warm_up_days == 13


def test_missing_manifest_is_a_data_error(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    with pytest.raises(DataCheckError):
        build_cache(data, warm_up_days=14)


def test_require_window_refuses_the_warm_up(tmp_path: Path) -> None:
    orders = [["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"]]
    data = make_data_dir(tmp_path, orders, [["1", "p1", 1]])
    meta = build_cache(data, warm_up_days=14)

    # decoupled but validated: replaying warm-up days would double-count
    # usage already baked into the starting stock
    with pytest.raises(ValueError, match="warm-up"):
        require_window(meta, from_day=13, to_day=20)
    with pytest.raises(ValueError, match="before from_day"):
        require_window(meta, from_day=14, to_day=13)
    assert require_window(meta, from_day=14, to_day=20) == (14, 20)
    assert require_window(meta, from_day=14, to_day=None) == (14, meta.last_day)


def test_iter_orders_windows_the_stream(tmp_path: Path) -> None:
    orders = [
        ["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"],
        ["2", "c1", "v1", "w21z7", "3", 15, "12:00:00"],
        ["3", "c1", "v1", "w21z7", "3", 16, "12:00:00"],
    ]
    items = [[oid, "p1", 1] for oid in ("1", "2", "3")]
    data = make_data_dir(tmp_path, orders, items)
    build_cache(data, warm_up_days=14)
    cache_file = data / "cached" / "orders-sorted.jsonl"

    all_ids = [o["order_id"] for o in iter_orders(cache_file)]
    assert all_ids == ["1", "2", "3"]
    window = [o["order_id"] for o in iter_orders(cache_file, from_day=15, to_day=15)]
    assert window == ["2"]


def test_to_order_request_maps_the_data_dictionary(tmp_path: Path) -> None:
    orders = [["0042", "c1", "v1", "w21z7", "3", 14, "12:03:29"]]
    items = [["0042", "p1", 1], ["0042", "p2", 3]]
    data = make_data_dir(tmp_path, orders, items)
    build_cache(data, warm_up_days=14)
    (order,) = iter_orders(data / "cached" / "orders-sorted.jsonl")

    request = to_order_request(order)
    assert request.channel == "dhrd"
    assert request.external_order_id == "0042"
    assert request.vendor_id == "v1"
    assert request.customer_geohash == "w21z7"
    assert request.order_day == 14
    assert request.order_time.isoformat() == "12:03:29"
    assert [item.product_id for item in request.items] == ["p1", "p2"]
    assert request.items[1].quantity == 3

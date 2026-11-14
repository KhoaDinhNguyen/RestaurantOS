"""Shard assignment, the MANIFEST check, warm-up stock and the volume
prompt: everything in bootstrap that can be tested without Docker."""

import csv
import hashlib
import io
from pathlib import Path

import pytest
from pydantic import ValidationError

from restaurantos.sim.config import SimSettings
from restaurantos.sim.datacheck import (
    REQUIRED_FILES,
    DataCheckError,
    check_manifest,
)
from restaurantos.sim.loading import REFERENCE_ORDER
from restaurantos.sim.shard import assign_shards, vendor_counts
from restaurantos.sim.stock import warm_up_stock

# ---- stock settings ---------------------------------------------------------


def test_zero_warm_up_days_is_rejected():
    with pytest.raises(ValidationError):
        SimSettings(stock_warm_up_days=0)


def test_negative_cover_days_is_rejected():
    with pytest.raises(ValidationError):
        SimSettings(stock_cover_days=-1)


def test_negative_floor_is_rejected():
    with pytest.raises(ValidationError):
        SimSettings(stock_floor=-1)


# ---- shards -----------------------------------------------------------------


def _vendors(path: Path, rows: list[tuple[str, str]]) -> None:
    lines = ["vendor_id,chain_id,geohash,primary_cuisine"]
    lines += [f"{v},,{c},x" for v, c in rows]
    (path / "vendors.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_vendor_counts_reads_only_the_cell_column(tmp_path):
    _vendors(tmp_path, [("v1", "w21z7"), ("v2", "w21z7"), ("v3", "w21z6")])
    assert vendor_counts(tmp_path) == {"w21z7": 2, "w21z6": 1}


def test_shards_are_contiguous_in_z_order_and_cover_everything():
    counts = {f"w2{i}z{i}": 1 for i in range(9)}
    assigned = assign_shards(counts, ["r1", "r2", "r3"])
    assert [cell for cell, _ in assigned] == sorted(counts)
    # each region's cells are one contiguous run of the sorted list
    runs: dict[str, list[str]] = {}
    for cell, rid in assigned:
        runs.setdefault(rid, []).append(cell)
    assert set(runs) == {"r1", "r2", "r3"}
    assert all(len(v) == 3 for v in runs.values())
    assert sum(len(v) for v in runs.values()) == len(counts)


def test_shards_balance_vendor_counts():
    counts = {"w21z7": 30, "w21z6": 30, "w21z3": 3}
    assigned = dict(assign_shards(counts, ["r1", "r2"]))
    # the greedy cut follows the running total, not the cell count
    assert assigned["w21z3"] == assigned["w21z6"] != assigned["w21z7"]
    assert sum(counts[c] for c, r in assign_shards(counts, ["r1", "r2"]) if r == "r1") >= 30


def test_shards_are_deterministic():
    counts = {"w21zt": 5, "w21z7": 1309, "w21z6": 745}
    first = assign_shards(counts, ["r1", "r2"])
    assert first == assign_shards(dict(reversed(list(counts.items()))), ["r1", "r2"])


def test_more_regions_than_cells_is_an_error():
    with pytest.raises(ValueError, match="at least one cell"):
        assign_shards({"w21z7": 5}, ["r1", "r2"])


def test_no_vendors_is_an_error():
    with pytest.raises(ValueError, match="no vendors"):
        assign_shards({}, ["r1"])


def test_one_huge_cell_cannot_starve_the_last_regions():
    # CodeRabbit's skew case: the greedy share closes every range after the
    # big cell, so regions 2 and 3 would get nothing. The cut must also
    # close when the cells left are exactly the regions left.
    counts = {"w21z3": 1, "w21z6": 1, "w21z7": 100}
    assigned = dict(assign_shards(counts, ["r1", "r2", "r3"]))
    assert set(assigned.values()) == {"r1", "r2", "r3"}
    assert assigned == {"w21z3": "r1", "w21z6": "r2", "w21z7": "r3"}


def test_every_region_gets_at_least_one_cell():
    counts = {"w21z2": 1, "w21z3": 1, "w21z5": 1, "w21z7": 97}
    for regions in (["r1", "r2"], ["r1", "r2", "r3"], ["r1", "r2", "r3", "r4"]):
        assigned = dict(assign_shards(counts, regions))
        assert set(assigned.values()) == set(regions), regions


# ---- manifest ---------------------------------------------------------------


def _write_data(tmp_path: Path, files: dict[str, str]) -> None:
    entries = []
    for name, content in files.items():
        (tmp_path / name).write_text(content, encoding="utf-8")
        rows = sum(1 for _ in csv.reader(io.StringIO(content))) - 1
        entries.append((name, str(rows), hashlib.sha256(content.encode()).hexdigest()))
    manifest = io.StringIO()
    manifest.write("file,rows,sha256\n")
    for entry in entries:
        manifest.write(",".join(entry) + "\n")
    (tmp_path / "MANIFEST.csv").write_text(manifest.getvalue(), encoding="utf-8")


def test_manifest_passes_on_matching_files(tmp_path):
    _write_data(
        tmp_path, {"vendors.csv": "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n"}
    )
    assert check_manifest(tmp_path, required=frozenset({"vendors.csv"})) == [("vendors.csv", 1)]


def test_manifest_counts_rows_not_lines(tmp_path):
    _write_data(tmp_path, {"products.csv": 'product_id,vendor_id,name\np1,v1,"quoted\nnewline"\n'})
    assert check_manifest(tmp_path, required=frozenset({"products.csv"})) == [("products.csv", 1)]


def test_manifest_rejects_a_wrong_hash(tmp_path):
    _write_data(tmp_path, {"vendors.csv": "vendor_id\nv1\n"})
    (tmp_path / "vendors.csv").write_text("vendor_id\nv2\n", encoding="utf-8")
    with pytest.raises(DataCheckError, match="sha256"):
        check_manifest(tmp_path)


def test_manifest_rejects_a_wrong_row_count(tmp_path):
    _write_data(tmp_path, {"vendors.csv": "vendor_id\nv1\n"})
    (tmp_path / "vendors.csv").write_text("vendor_id\nv1\nv2\n", encoding="utf-8")
    with pytest.raises(DataCheckError, match="rows"):
        check_manifest(tmp_path)


def test_manifest_rejects_a_missing_file(tmp_path):
    _write_data(tmp_path, {"vendors.csv": "vendor_id\nv1\n"})
    (tmp_path / "vendors.csv").unlink()
    with pytest.raises(DataCheckError, match="missing"):
        check_manifest(tmp_path)


def test_a_missing_manifest_is_named(tmp_path):
    with pytest.raises(DataCheckError, match="MANIFEST"):
        check_manifest(tmp_path)


def test_a_required_file_left_out_of_the_manifest_is_rejected(tmp_path):
    # every file on disk and verified, but the manifest does not mention
    # one of the files bootstrap reads: it would load unchecked
    _write_data(
        tmp_path, {"vendors.csv": "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n"}
    )
    manifest = tmp_path / "MANIFEST.csv"
    lines = manifest.read_text(encoding="utf-8").splitlines()
    kept = [line for line in lines if "vendors" not in line]
    manifest.write_text("\n".join(kept) + "\n", encoding="utf-8")
    with pytest.raises(DataCheckError, match="required files not listed.*vendors"):
        check_manifest(tmp_path)


def test_required_files_cover_everything_bootstrap_reads():
    # the six reference tables (loading) plus the two order files that
    # warm_up_stock measures usage from; if this fails, datacheck's
    # required set has drifted from what the code reads
    assert (
        frozenset(REFERENCE_ORDER)
        | {
            "orders.csv",
            "order_items.csv",
        }
        == REQUIRED_FILES
    )


# ---- warm-up stock ----------------------------------------------------------


def _dataset(tmp_path: Path) -> Path:
    """One vendor, one per_ingredient dish (multiplied) and one per_item
    dish, with warm-up and replay orders."""

    def write(name: str, header: str, *rows: str) -> None:
        (tmp_path / name).write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")

    write("vendors.csv", "vendor_id,chain_id,geohash,primary_cuisine", "v1,,w21z7,x")
    write("ingredients.csv", "ingredient_id,source", "beef,recipenlg", "rice,recipenlg")
    write(
        "dishes.csv",
        "dish_id,dish_name,scope,stock_mode,n_products,n_vendors,example_name",
        "d1,noodles,independent,per_ingredient,1,1,x",
        "d2,drink,independent,per_item,1,1,x",
    )
    write(
        "dish_ingredients.csv",
        "dish_id,ingredient_id,amount,unit",
        "d1,beef,2,kg",
        "d1,rice,1,portion",
    )
    write(
        "products.csv",
        "product_id,vendor_id,name,unit_price,price_units",
        "p1,v1,noodles,0.01,10",
        "p2,v1,drink,0.01,10",
    )
    write(
        "product_dish.csv",
        "product_id,vendor_id,dish_id,multiplier,multiplier_ambiguous",
        "p1,v1,d1,2,false",
        "p2,v1,d2,1,false",
    )
    # warm-up: day 0 has one order for 3 noodles; day 20 (replay) is ignored
    write(
        "orders.csv",
        "order_id,customer_id,vendor_id,geohash,day_of_week,order_day,order_time",
        "o1,c1,v1,w21z7,0,0,10:00:00",
        "o2,c2,v1,w21z7,1,20,11:00:00",
    )
    write("order_items.csv", "order_id,product_id,quantity", "o1,p1,3", "o2,p1,50", "o2,p2,5")
    return tmp_path


def test_stock_uses_the_warm_up_window_only(tmp_path):
    rows = warm_up_stock(_dataset(tmp_path), warm_days=14, cover_days=7, floor=5)
    stock = {(kind, item): on_hand for vendor, kind, item, on_hand, _ in rows}
    # beef usage: 3 x multiplier 2 x amount 2 = 12 over 14 warm-up days
    # ceil(12 / 14 x 7) = 6; rice: 3 x 2 x 1 = 6 -> ceil(6/14 x 7) = 3 < F
    assert stock[("ingredient", "beef")] == 6
    assert stock[("ingredient", "rice")] == 5  # the floor
    # per_item dish counter: no usage during warm-up, so the floor; and the
    # replay-day order (o2, 5 drinks) must not count
    assert stock[("dish", "d2")] == 5
    # par_level is the same number
    assert all(on_hand == par for _, _, _, on_hand, par in rows)


def test_stock_never_used_counters_get_the_floor(tmp_path):
    # add a product whose dish needs an ingredient no warm-up order touched
    path = _dataset(tmp_path)
    with (path / "dish_ingredients.csv").open("a", encoding="utf-8") as f:
        f.write("d1,herbs,1,portion\n")
    with (path / "ingredients.csv").open("a", encoding="utf-8") as f:
        f.write("herbs,recipenlg\n")
    rows = warm_up_stock(path, warm_days=14, cover_days=7, floor=5)
    assert ("ingredient", "herbs") in {(k, i) for _, k, i, _, _ in rows}


def test_stock_covers_every_counter_a_vendor_needs(tmp_path):
    rows = warm_up_stock(_dataset(tmp_path), warm_days=14, cover_days=7, floor=5)
    assert len(rows) == 3  # beef, rice, d2 - and nothing for other vendors

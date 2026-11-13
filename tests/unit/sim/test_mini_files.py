"""The mini fixture files, checked without any database: headers, foreign
keys, and every trap the briefs rely on. If a trap is missing, the wrong
package's test fails far away from the cause; these checks point here."""

import csv
from pathlib import Path

import pytest

from restaurantos.sim.mini import mini_dir

# Expected headers, copied here on purpose: the loader's own expectations
# must not be the only thing that says what the files look like.
HEADERS = {
    "ingredients.csv": ["ingredient_id", "source"],
    "dishes.csv": [
        "dish_id",
        "dish_name",
        "scope",
        "stock_mode",
        "n_products",
        "n_vendors",
        "example_name",
    ],
    "dish_ingredients.csv": ["dish_id", "ingredient_id", "amount", "unit"],
    "vendors.csv": ["vendor_id", "chain_id", "geohash", "primary_cuisine"],
    "products.csv": ["product_id", "vendor_id", "name", "unit_price", "price_units"],
    "product_dish.csv": [
        "product_id",
        "vendor_id",
        "dish_id",
        "multiplier",
        "multiplier_ambiguous",
    ],
    "orders.csv": [
        "order_id",
        "customer_id",
        "vendor_id",
        "geohash",
        "day_of_week",
        "order_day",
        "order_time",
    ],
    "order_items.csv": ["order_id", "product_id", "quantity"],
    "stock.csv": ["vendor_id", "item_kind", "item_id", "on_hand", "par_level"],
}


def rows(name: str) -> list[dict[str, str]]:
    path = mini_dir() / name
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == HEADERS[name], f"{name}: header changed"
        return list(reader)


@pytest.fixture(scope="module")
def data() -> dict[str, list[dict[str, str]]]:
    return {name: rows(name) for name in HEADERS}


def test_every_file_has_rows(data):
    for name, file_rows in data.items():
        assert file_rows, f"{name} is empty"


def test_ids_are_unique(data):
    for name, key in [
        ("ingredients.csv", "ingredient_id"),
        ("dishes.csv", "dish_id"),
        ("vendors.csv", "vendor_id"),
        ("products.csv", "product_id"),
        ("orders.csv", "order_id"),
    ]:
        values = [row[key] for row in data[name]]
        assert len(values) == len(set(values)), f"{name}: duplicate {key}"


def test_foreign_keys_resolve(data):
    vendors = {row["vendor_id"] for row in data["vendors.csv"]}
    products = {row["product_id"] for row in data["products.csv"]}
    dishes = {row["dish_id"] for row in data["dishes.csv"]}
    ingredients = {row["ingredient_id"] for row in data["ingredients.csv"]}
    orders = {row["order_id"] for row in data["orders.csv"]}
    assert {row["vendor_id"] for row in data["products.csv"]} <= vendors
    for row in data["product_dish.csv"]:
        assert row["product_id"] in products
        assert row["vendor_id"] in vendors
        assert row["dish_id"] in dishes
    for row in data["dish_ingredients.csv"]:
        assert row["dish_id"] in dishes
        assert row["ingredient_id"] in ingredients
    for row in data["order_items.csv"]:
        assert row["order_id"] in orders
        assert row["product_id"] in products
    for row in data["orders.csv"]:
        assert row["vendor_id"] in vendors
    for row in data["stock.csv"]:
        assert row["vendor_id"] in vendors


def test_every_product_links_to_a_dish(data):
    linked = {row["product_id"] for row in data["product_dish.csv"]}
    products = {row["product_id"] for row in data["products.csv"]}
    assert products == linked


def test_units_are_never_empty_and_never_conflict(data):
    units: dict[str, set[str]] = {}
    for row in data["dish_ingredients.csv"]:
        assert row["unit"], "empty unit"
        assert float(row["amount"]) > 0, "amount must be positive"
        units.setdefault(row["ingredient_id"], set()).add(row["unit"])
    conflicts = {name: seen for name, seen in units.items() if len(seen) > 1}
    assert not conflicts, "an ingredient appears with two different units"


def test_traps_amounts_above_one_with_other_units(data):
    traps = [
        row
        for row in data["dish_ingredients.csv"]
        if float(row["amount"]) != 1 and row["unit"] != "portion"
    ]
    assert len(traps) >= 2, "the real data cannot catch a missing multiply"
    usage: dict[str, int] = {}
    for row in data["dish_ingredients.csv"]:
        usage[row["ingredient_id"]] = usage.get(row["ingredient_id"], 0) + 1
    for trap in traps:
        assert usage[trap["ingredient_id"]] == 1, (
            f"{trap['ingredient_id']} appears more than once: unit conflict risk"
        )


def test_traps_multipliers_above_one(data):
    boosted = [row for row in data["product_dish.csv"] if float(row["multiplier"]) > 1]
    assert len(boosted) >= 2


def test_traps_one_blank_dish_name(data):
    blank = [row for row in data["dishes.csv"] if row["dish_name"] == ""]
    assert len(blank) == 1


def test_traps_one_blank_chain(data):
    blank = [row for row in data["vendors.csv"] if row["chain_id"] == ""]
    assert len(blank) == 1


def test_traps_one_low_stock_counter(data):
    low = [row for row in data["stock.csv"] if int(row["on_hand"]) < int(row["par_level"])]
    assert len(low) == 1


def test_no_empty_fields_outside_the_two_nullable_columns(data):
    """Data dictionary rule 4: an empty field exists only in
    vendors.chain_id and dishes.dish_name, and means NULL. The loader
    refuses any other empty, so the files must not contain one."""
    nullable = {("vendors.csv", "chain_id"), ("dishes.csv", "dish_name")}
    for name, file_rows in data.items():
        for row in file_rows:
            for column, value in row.items():
                if value == "" and (name, column) not in nullable:
                    pytest.fail(f"{name}.{column}: unexpected empty value")


def test_unit_price_kept_to_4_decimals(data):
    """price_units is the price the system uses; unit_price is kept short
    only so the files stay readable (the real files carry up to 19)."""
    for row in data["products.csv"]:
        decimals = row["unit_price"].split(".")[1] if "." in row["unit_price"] else ""
        assert len(decimals) <= 4


def test_one_product_id_is_all_digits_with_a_leading_zero(data):
    """The dictionary warns that some real ids are all digits, a few with
    leading zeros, and that a reader which guesses types breaks every
    join. The fixture keeps one such id, ordered four times."""
    assert "0017" in {row["product_id"] for row in data["products.csv"]}
    for name in ("products.csv", "product_dish.csv", "order_items.csv"):
        values = [row["product_id"] for row in data[name]]
        assert "0017" in values, f"{name} lost the all-digit id"
    orders_of_it = [row for row in data["order_items.csv"] if row["product_id"] == "0017"]
    assert len(orders_of_it) == 4


def test_orders_cover_days_and_cells_for_query_tests(data):
    days = {row["order_day"] for row in data["orders.csv"]}
    cells = {row["geohash"] for row in data["orders.csv"]}
    vendors = {row["vendor_id"] for row in data["orders.csv"]}
    assert len(days) == 3
    assert len(cells) >= 2, "a border query needs more than one cell"
    assert len(vendors) >= 3, "a region split needs more than one vendor"


def test_loader_reads_the_all_digit_id_as_text():
    from restaurantos.sim.mini import _read

    for name in ("products.csv", "product_dish.csv", "order_items.csv"):
        assert any(row["product_id"] == "0017" for row in _read(name)), name


def test_mini_dir_override_is_reported(monkeypatch, tmp_path):
    from restaurantos.sim import mini

    monkeypatch.setenv("ROS_MINI_DIR", str(tmp_path / "nowhere"))
    with pytest.raises(RuntimeError, match="ROS_MINI_DIR"):
        mini.mini_dir()


def test_mini_dir_found_without_the_override(monkeypatch):
    from restaurantos.sim import mini

    monkeypatch.delenv("ROS_MINI_DIR", raising=False)
    assert (mini.mini_dir() / "vendors.csv").is_file()
    assert Path(mini.mini_dir()).name == "mini"

"""Load the mini fixture (tests/fixtures/mini/) into a database.

The fixture is a small, sanitized slice of the real dataset; see the
README in that folder for where it comes from and which traps it plants.
`load_mini` is a test helper, not the real loader: it follows the data
dictionary's rules (every value read as text, explicit column lists,
foreign-key order) but invents the live-only columns of the region's
orders table deterministically, so a region and a baseline loaded from
the same files hold identical rows.

Apply a schema first (``common.sqlfiles.schema_files``, as
``tests/integration/test_schema.py`` does), then:

    from restaurantos.sim.mini import load_mini
    load_mini(conn)                          # region or baseline: everything
    load_mini(conn, with_region_data=False)  # HQ kind: reference data only
    load_mini(conn, vendors=["v001"])        # one region's slice

Passing ``vendors`` loads only those vendors and everything reachable from
them, so two regions loaded with disjoint vendor slices hold the same rows
as one baseline loaded with no slice.
"""

from __future__ import annotations

import csv
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import psycopg

FIXTURE_DIR = Path("tests/fixtures/mini")

# The dataset files in foreign-key order, as the data dictionary requires.
# stock.csv is fixture-only: starting stock is warm-up-derived in the real
# system, so the dataset has no file for it.
REFERENCE_FILES = (
    ("ingredients.csv", ("ingredient_id", "source")),
    (
        "dishes.csv",
        ("dish_id", "dish_name", "scope", "stock_mode", "n_products", "n_vendors", "example_name"),
    ),
    ("dish_ingredients.csv", ("dish_id", "ingredient_id", "amount", "unit")),
    ("vendors.csv", ("vendor_id", "chain_id", "geohash", "primary_cuisine")),
    ("products.csv", ("product_id", "vendor_id", "name", "unit_price", "price_units")),
    (
        "product_dish.csv",
        ("product_id", "vendor_id", "dish_id", "multiplier", "multiplier_ambiguous"),
    ),
)

REGION_FILES = (
    (
        "orders.csv",
        (
            "order_id",
            "customer_id",
            "vendor_id",
            "geohash",
            "day_of_week",
            "order_day",
            "order_time",
        ),
    ),
    ("order_items.csv", ("order_id", "product_id", "quantity")),
    ("stock.csv", ("vendor_id", "item_kind", "item_id", "on_hand", "par_level")),
)

# Values for the live-only columns of the region's orders table. The order
# id is a uuid5 of the fixture's order id, so reloading, or loading a region
# and a baseline from the same files, yields the same rows.
CHANNEL = "mini"
MENU_VERSION = 1
EPOCH = 1
STATUS = "accepted"
RECEIVED_AT = "2026-11-01 12:00:00+00"


@dataclass(frozen=True)
class MiniCounts:
    """Rows written per table by one `load_mini` call."""

    ingredients: int
    dishes: int
    dish_ingredients: int
    vendors: int
    products: int
    product_dish: int
    orders: int
    order_items: int
    stock: int


def mini_dir() -> Path:
    """The fixture folder.

    ROS_MINI_DIR overrides the search; unset, the loader walks up from its
    own file to the repository root and expects the fixture there. A clear
    error names both when neither works.
    """
    override = os.environ.get("ROS_MINI_DIR")
    if override:
        path = Path(override)
        if path.is_dir():
            return path
        raise RuntimeError(
            f"ROS_MINI_DIR is set to {path}, which is not a directory; "
            "unset it or point it at a folder holding the mini CSV files"
        )
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file() and (parent / FIXTURE_DIR).is_dir():
            return parent / FIXTURE_DIR
    raise RuntimeError(
        "mini fixture not found: walked up from this file looking for "
        f"{FIXTURE_DIR}; set ROS_MINI_DIR to the folder holding the mini CSV files"
    )


def _read(name: str) -> list[dict[str, str]]:
    """All rows of one fixture file, read as text and checked against its header."""
    expected = dict(REFERENCE_FILES + REGION_FILES).get(name)
    with (mini_dir() / name).open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if expected is not None and tuple(reader.fieldnames or ()) != expected:
            raise RuntimeError(f"{name}: header is {reader.fieldnames}, expected {list(expected)}")
        return list(reader)


def _reference_rows(vendors: frozenset[str] | None) -> dict[str, list[tuple[str, ...]]]:
    """The six reference tables, sliced by vendor when a slice is given."""
    rows = {name: [tuple(row.values()) for row in _read(name)] for name, _ in REFERENCE_FILES}
    if vendors is None:
        return rows
    products = {r[0] for r in rows["products.csv"] if r[1] in vendors}
    links = [r for r in rows["product_dish.csv"] if r[0] in products]
    dishes = {r[2] for r in links}
    dish_ing = [r for r in rows["dish_ingredients.csv"] if r[0] in dishes]
    ingredients = {r[1] for r in dish_ing}
    return {
        "ingredients.csv": [r for r in rows["ingredients.csv"] if r[0] in ingredients],
        "dishes.csv": [r for r in rows["dishes.csv"] if r[0] in dishes],
        "dish_ingredients.csv": dish_ing,
        "vendors.csv": [r for r in rows["vendors.csv"] if r[0] in vendors],
        "products.csv": [r for r in rows["products.csv"] if r[0] in products],
        "product_dish.csv": links,
    }


def _copy(
    conn: psycopg.Connection,
    table: str,
    columns: tuple[str, ...],
    rows: Sequence[tuple[str, ...]],
) -> int:
    if not rows:
        return 0
    column_list = ", ".join(columns)
    with conn.cursor() as cur, cur.copy(f"COPY {table} ({column_list}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    return len(rows)


def _order_rows(
    vendors: frozenset[str] | None,
) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    """(orders, order_items) with the live-only columns filled in."""
    products = {row["product_id"]: row for row in _read("products.csv")}
    orders = [row for row in _read("orders.csv") if vendors is None or row["vendor_id"] in vendors]
    by_id = {row["order_id"]: row for row in orders}
    items: list[tuple[str, ...]] = []
    lines: dict[str, int] = {}
    for item in _read("order_items.csv"):
        order = by_id.get(item["order_id"])
        if order is None:
            continue
        order_id = str(uuid5(NAMESPACE_URL, f"restaurantos/mini/{item['order_id']}"))
        lines[item["order_id"]] = lines.get(item["order_id"], 0) + 1
        items.append(
            (
                order["order_day"],
                order_id,
                str(lines[item["order_id"]]),
                order["vendor_id"],
                item["product_id"],
                item["quantity"],
                products[item["product_id"]]["price_units"],
            )
        )
    order_rows = [
        (
            row["order_day"],
            str(uuid5(NAMESPACE_URL, f"restaurantos/mini/{row['order_id']}")),
            row["vendor_id"],
            CHANNEL,
            row["order_id"],
            row["customer_id"],
            row["geohash"],
            row["order_time"],
            STATUS,
            str(MENU_VERSION),
            str(EPOCH),
            RECEIVED_AT,
        )
        for row in orders
    ]
    return order_rows, items


def load_mini(
    conn: psycopg.Connection,
    *,
    vendors: Sequence[str] | None = None,
    with_region_data: bool = True,
) -> MiniCounts:
    """Load the fixture. See the module docstring for the parameters."""
    if vendors is not None and not vendors:
        raise ValueError("vendors is empty: load everything or name at least one vendor")
    slice_ = frozenset(vendors) if vendors is not None else None
    reference = _reference_rows(slice_)
    unit_conflicts = _unit_conflicts(reference["dish_ingredients.csv"])
    if unit_conflicts:
        raise ValueError(
            "an ingredient appears with two different units: " + ", ".join(sorted(unit_conflicts))
        )
    order_rows, item_rows = _order_rows(slice_) if with_region_data else ([], [])
    stock_rows = [tuple(row.values()) for row in _read("stock.csv")] if with_region_data else []
    if slice_ is not None:
        stock_rows = [r for r in stock_rows if r[0] in slice_]

    tables = {
        "ingredients": ("ingredients", REFERENCE_FILES[0][1], reference["ingredients.csv"]),
        "dishes": ("dishes", REFERENCE_FILES[1][1], reference["dishes.csv"]),
        "dish_ingredients": (
            "dish_ingredients",
            REFERENCE_FILES[2][1],
            reference["dish_ingredients.csv"],
        ),
        "vendors": ("vendors", REFERENCE_FILES[3][1], reference["vendors.csv"]),
        "products": ("products", REFERENCE_FILES[4][1], reference["products.csv"]),
        "product_dish": ("product_dish", REFERENCE_FILES[5][1], reference["product_dish.csv"]),
        "orders": (
            "orders",
            (
                "order_day",
                "order_id",
                "vendor_id",
                "channel",
                "external_order_id",
                "customer_id",
                "customer_geohash",
                "order_time",
                "status",
                "menu_version",
                "epoch",
                "received_at",
            ),
            order_rows,
        ),
        "order_items": (
            "order_items",
            (
                "order_day",
                "order_id",
                "line_no",
                "vendor_id",
                "product_id",
                "quantity",
                "price_units",
            ),
            item_rows,
        ),
        "stock": (
            "stock",
            ("vendor_id", "item_kind", "item_id", "on_hand", "par_level"),
            stock_rows,
        ),
    }
    counts: dict[str, int] = {}
    with conn.transaction():
        for name, (table, columns, rows) in tables.items():
            counts[name] = _copy(conn, table, columns, rows)
    return MiniCounts(**counts)


def _unit_conflicts(rows: Sequence[tuple[str, ...]]) -> set[str]:
    """Ingredients that appear with more than one unit (must never happen)."""
    units: dict[str, set[str]] = {}
    for row in rows:
        units.setdefault(row[1], set()).add(row[3])
    return {name for name, seen in units.items() if len(seen) > 1}

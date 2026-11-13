"""The data dictionary's load rules, shared by the mini fixture loader and
bootstrap. One implementation, so the fixture and the real data cannot
drift apart: `restaurantos.sim.mini` reads the same six files with the
same rules, just from tests/fixtures/mini.

The rules (docs/schema/data-dictionary.md):

- read every value as text (ids are all digits sometimes, a few with
  leading zeros);
- COPY with the file's column list, never `COPY *`;
- load in foreign-key order: ingredients, dishes, dish_ingredients,
  vendors, products, product_dish;
- an empty field means "no value" and loads as NULL, and only
  vendors.chain_id and dishes.dish_name have empty fields (rule 4);
- an ingredient must never appear with two different units;
- a vendor slice holds only its own vendors, products and product_dish
  links, and full copies of the three small tables, exactly as a region
  holds them (db/shared/010_reference.sql).
"""

from __future__ import annotations

import csv
from collections.abc import Iterator, Sequence
from pathlib import Path

import psycopg

# The six reference files. Table name = file name without .csv, and the
# header of each file is this column list, in this order.
REFERENCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "ingredients.csv": ("ingredient_id", "source"),
    "dishes.csv": (
        "dish_id",
        "dish_name",
        "scope",
        "stock_mode",
        "n_products",
        "n_vendors",
        "example_name",
    ),
    "dish_ingredients.csv": ("dish_id", "ingredient_id", "amount", "unit"),
    "vendors.csv": ("vendor_id", "chain_id", "geohash", "primary_cuisine"),
    "products.csv": ("product_id", "vendor_id", "name", "unit_price", "price_units"),
    "product_dish.csv": (
        "product_id",
        "vendor_id",
        "dish_id",
        "multiplier",
        "multiplier_ambiguous",
    ),
}

# The load order: every table's foreign keys come before it.
REFERENCE_ORDER = (
    "ingredients.csv",
    "dishes.csv",
    "dish_ingredients.csv",
    "vendors.csv",
    "products.csv",
    "product_dish.csv",
)

NULLABLE = {("vendors", "chain_id"), ("dishes", "dish_name")}


def table_of(file_name: str) -> str:
    return file_name.removesuffix(".csv")


def read_rows(path: Path, columns: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    """Rows of one CSV as text tuples, its header checked against
    `columns` so a changed column order fails loudly, not subtly."""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if tuple(header) != columns:
            raise RuntimeError(f"{path.name}: header is {header}, expected {list(columns)}")
        yield from reader


def apply_null_rule(
    table: str, columns: tuple[str, ...], rows: Sequence[tuple[object, ...]]
) -> list[tuple[object, ...]]:
    """Empty means NULL, and only for the two columns rule 4 allows. An
    empty anywhere else is a broken file, so say so instead of storing ''."""
    out: list[tuple[object, ...]] = []
    for row in rows:
        if len(row) != len(columns):
            raise ValueError(f"{table}: {len(row)} values for {len(columns)} columns")
        fixed: list[object] = list(row)
        for i, column in enumerate(columns):
            if fixed[i] == "":
                if (table, column) in NULLABLE:
                    fixed[i] = None
                else:
                    raise ValueError(
                        f"{table}.{column}: empty value, but the data dictionary "
                        "allows empty fields only for vendors.chain_id and "
                        "dishes.dish_name"
                    )
        out.append(tuple(fixed))
    return out


def unit_conflicts(rows: Sequence[tuple[str, ...]]) -> set[str]:
    """Ingredient names that appear with more than one unit (must never
    happen: a stock counter is kept in the ingredient's unit)."""
    units: dict[str, set[str]] = {}
    for row in rows:
        units.setdefault(row[1], set()).add(row[3])
    return {name for name, seen in units.items() if len(seen) > 1}


def reference_rows(
    data_dir: Path, *, vendors: frozenset[str] | None = None
) -> dict[str, list[tuple[str, ...]]]:
    """The six reference tables from a data directory. A vendor slice cuts
    only the three tables a region really slices; the small reference
    tables load in full either way."""
    rows = {
        name: list(read_rows(data_dir / name, REFERENCE_COLUMNS[name])) for name in REFERENCE_ORDER
    }
    if vendors is None:
        return rows
    products = [r for r in rows["products.csv"] if r[1] in vendors]
    product_ids = {r[0] for r in products}
    return {
        "ingredients.csv": rows["ingredients.csv"],
        "dishes.csv": rows["dishes.csv"],
        "dish_ingredients.csv": rows["dish_ingredients.csv"],
        "vendors.csv": [r for r in rows["vendors.csv"] if r[0] in vendors],
        "products.csv": products,
        "product_dish.csv": [r for r in rows["product_dish.csv"] if r[0] in product_ids],
    }


def copy_sync(
    conn: psycopg.Connection,
    table: str,
    columns: tuple[str, ...],
    rows: Sequence[tuple[object, ...]],
) -> int:
    if not rows:
        return 0
    column_list = ", ".join(columns)
    with conn.cursor() as cur, cur.copy(f"COPY {table} ({column_list}) FROM STDIN") as copy:
        for row in apply_null_rule(table, columns, rows):
            copy.write_row(row)
    return len(rows)


async def copy_async(
    conn: psycopg.AsyncConnection,
    table: str,
    columns: tuple[str, ...],
    rows: Sequence[tuple[object, ...]],
) -> int:
    if not rows:
        return 0
    column_list = ", ".join(columns)
    async with conn.cursor() as cur, cur.copy(f"COPY {table} ({column_list}) FROM STDIN") as copy:
        for row in apply_null_rule(table, columns, rows):
            await copy.write_row(row)
    return len(rows)


def copy_sql(table: str, columns: tuple[str, ...]) -> str:
    """The same COPY statement the two helpers run, for callers that stream
    rows instead of holding them in a list."""
    return f"COPY {table} ({', '.join(columns)}) FROM STDIN"


def write_rows_to_copy(
    copy: psycopg.Copy, table: str, columns: tuple[str, ...], row: tuple[str, ...]
) -> None:
    """One validated row onto an open COPY (sync or async block alike)."""
    (fixed,) = apply_null_rule(table, columns, [row])
    copy.write_row(fixed)


def check_reference(data_dir: Path) -> None:
    """The unit rule, checked once on the files before any load."""
    rows = list(
        read_rows(data_dir / "dish_ingredients.csv", REFERENCE_COLUMNS["dish_ingredients.csv"])
    )
    conflicts = unit_conflicts(rows)
    if conflicts:
        raise ValueError(
            "an ingredient appears with two different units: " + ", ".join(sorted(conflicts))
        )

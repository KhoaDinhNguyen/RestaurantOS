"""Describe the cleaned dataset without printing any of its rows.

    uv run python scripts/profile_dataset.py data > dataset-profile.txt

The numbers in docs/schema/data-dictionary.md come from this script. Run it
again when the dataset is re-exported and update that file.

For every CSV: row count, and per column the kind of value, how many are
empty, how many distinct, numeric range, text length, and the "shape" of the
values (letters become a, digits 9: w21z7 -> a99a9). Actual values are shown
only for columns with at most 12 distinct ones (codes such as stock_mode).
Then the cross-file counts the data dictionary quotes. Standard library only.
"""

import csv
import re
import sys
from collections import Counter
from pathlib import Path

csv.field_size_limit(10**9)
CAP = 200_000  # stop tracking distinct values of a column beyond this


def shape(value: str) -> str:
    s = re.sub(r"[a-z]", "a", value)
    s = re.sub(r"[A-Z]", "A", s)
    s = re.sub(r"[0-9]", "9", s)
    return s if len(s) <= 24 else s[:24] + "..."


def kind(value: str) -> str:
    if re.fullmatch(r"-?\d+", value):
        return "int"
    if re.fullmatch(r"-?\d*\.\d+(e-?\d+)?", value):
        return "decimal"
    return "text"


def profile(path: Path) -> dict[str, set[str]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        n = len(header)
        empty, kinds, shapes = [0] * n, [Counter() for _ in header], [Counter() for _ in header]
        distinct = [set() for _ in header]
        low, high = [None] * n, [None] * n
        shortest, longest = [None] * n, [0] * n
        rows = 0
        for row in reader:
            rows += 1
            for i, value in enumerate(row[:n]):
                if value == "":
                    empty[i] += 1
                    continue
                k = kind(value)
                kinds[i][k] += 1
                shapes[i][shape(value)] += 1
                if len(distinct[i]) <= CAP:
                    distinct[i].add(value)
                if k != "text":
                    x = float(value)
                    low[i] = x if low[i] is None else min(low[i], x)
                    high[i] = x if high[i] is None else max(high[i], x)
                shortest[i] = len(value) if shortest[i] is None else min(shortest[i], len(value))
                longest[i] = max(longest[i], len(value))
    print(f"\n== {path.name}: {rows} rows, {n} columns")
    for i, name in enumerate(header):
        count = f">{CAP}" if len(distinct[i]) > CAP else str(len(distinct[i]))
        line = f"  {name}: kinds={dict(kinds[i])} empty={empty[i]} distinct={count}"
        line += f" length={shortest[i]}..{longest[i]}"
        if low[i] is not None:
            line += f" range={low[i]:g}..{high[i]:g}"
        print(line)
        if 0 < len(distinct[i]) <= 12:
            values = Counter()
            with path.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    values[row[name]] += 1
            print(f"    values: {dict(values.most_common())}")
        else:
            print(f"    shapes ({len(shapes[i])}): {shapes[i].most_common(3)}")
    return {name: distinct[i] for i, name in enumerate(header)}


def keys(path: Path, columns: list[str]) -> set[tuple[str, ...]] | None:
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not set(columns) <= set(reader.fieldnames or []):
            return None
        return {tuple(row[c] for c in columns) for row in reader}


def cross_checks(data: Path, warm_up_days: int) -> None:
    print("\n== cross-file counts (None means a file or column was not found)")
    vp = ["vendor_id", "product_id"]
    products, mapped = keys(data / "products.csv", vp), keys(data / "product_dish.csv", vp)
    if products is not None and mapped is not None:
        print(f"products without a product_dish row: {len(products - mapped)}")
        print(f"product_dish rows without a product: {len(mapped - products)}")

    modes = keys(data / "dishes.csv", ["dish_id", "stock_mode"])
    recipes = keys(data / "dish_ingredients.csv", ["dish_id"])
    if modes is not None and recipes is not None:
        with_rows = {d for (d,) in recipes}
        table = Counter((mode, dish in with_rows) for dish, mode in modes)
        print(f"dishes by (stock_mode, has dish_ingredients rows): {dict(table)}")

    units = keys(data / "dish_ingredients.csv", ["ingredient_id", "unit"])
    if units is not None:
        per_ingredient = Counter(ingredient for ingredient, _ in units)
        mixed = sum(1 for n in per_ingredient.values() if n > 1)
        print(f"ingredients used with more than one unit: {mixed}")

    vendor_cells = keys(data / "vendors.csv", ["geohash"])
    customer_cells = keys(data / "orders.csv", ["geohash"])
    if vendor_cells is not None and customer_cells is not None:
        print(
            f"vendor cells: {len(vendor_cells)}, customer cells: {len(customer_cells)},"
            f" customer cells with no vendor: {len(customer_cells - vendor_cells)}"
        )

    orders, lines = data / "orders.csv", data / "order_items.csv"
    if not orders.exists():
        return
    vendor_of: dict[str, str] = {}
    per_day: Counter[int] = Counter()
    with orders.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            vendor_of[row["order_id"]] = sys.intern(row["vendor_id"])
            per_day[int(row["order_day"])] += 1
    days = sorted(per_day)
    warm = sum(n for day, n in per_day.items() if day < warm_up_days)
    print(
        f"orders per order_day: days {days[0]}..{days[-1]} ({len(days)} distinct),"
        f" fewest {min(per_day.values())} (day {min(per_day, key=per_day.get)}),"
        f" most {max(per_day.values())} (day {max(per_day, key=per_day.get)})"
    )
    print(
        f"orders in the warm-up window (days 0..{warm_up_days - 1}): {warm};"
        f" replayed (day {warm_up_days} on): {sum(per_day.values()) - warm}"
    )
    if not lines.exists() or products is None:
        return
    ordered: set[tuple[str, str]] = set()
    per_order: Counter[str] = Counter()
    orphans = 0
    with lines.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            vendor = vendor_of.get(row["order_id"])
            if vendor is None:
                orphans += 1
                continue
            per_order[row["order_id"]] += 1
            ordered.add((vendor, row["product_id"]))
    print(f"order lines whose order_id is not in orders: {orphans}")
    print(f"orders without any line: {len(vendor_of) - len(per_order)}")
    print(f"lines per order: 1..{max(per_order.values())}")
    print(f"ordered (vendor, product) pairs not in products: {len(ordered - products)}")
    print(f"products never ordered: {len(products - ordered)}")


def main() -> None:
    data = Path(sys.argv[1] if len(sys.argv) > 1 else "data")
    warm_up_days = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    files = sorted(data.glob("*.csv"))
    print(f"folder: {data}  files: {[p.name for p in files]}")
    manifest = data / "MANIFEST.csv"
    if manifest.exists():
        print("\n== MANIFEST.csv, in full")
        print(manifest.read_text(encoding="utf-8").strip())
    for path in files:
        if path.name != "MANIFEST.csv":
            profile(path)
    cross_checks(data, warm_up_days)


if __name__ == "__main__":
    main()

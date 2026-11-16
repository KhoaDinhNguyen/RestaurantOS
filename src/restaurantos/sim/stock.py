"""Starting stock from the warm-up window (bootstrap step 9).

docs/replay-stock-parameters.md: measure usage on the first W days only
(playback starts at day W, so warm-up orders are never replayed, and no
future order informs the starting stock), and start every counter at
max(F, ceil(average daily warm-up usage x C days of cover)); the same
number is the par level. Usage is `quantity x multiplier x amount`, read
from the data, never assumed.

A counter exists for every (vendor, item) the vendor's products can
need, including items never used during warm-up — those start at the
floor. `per_ingredient` dishes keep an ingredient counter per recipe
row; `per_item` dishes keep one dish counter, with no recipe involved.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path


def _dict_rows(data_dir: Path, name: str) -> list[dict[str, str]]:
    with (data_dir / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def warm_up_stock(
    data_dir: Path, *, warm_days: int, cover_days: int, floor: int
) -> list[tuple[str, str, str, int, int]]:
    """(vendor_id, item_kind, item_id, on_hand, par_level) rows, sorted.

    Only the big files (orders, order_items) are streamed; the reference
    tables are read whole, as every other part of bootstrap does.
    """
    # The key of products is (vendor_id, product_id), not product_id
    # alone: two vendors may share a product_id, and keying by the id
    # would overwrite one of them and merge their dish links.
    vendor_products: set[tuple[str, str]] = set()  # (vendor_id, product_id)
    for row in _dict_rows(data_dir, "products.csv"):
        vendor_products.add((row["vendor_id"], row["product_id"]))

    links: dict[
        tuple[str, str], list[tuple[str, float]]
    ] = {}  # (vendor, product) -> [(dish, mult)]
    for row in _dict_rows(data_dir, "product_dish.csv"):
        key = (row["vendor_id"], row["product_id"])
        links.setdefault(key, []).append((row["dish_id"], float(row["multiplier"])))

    stock_mode: dict[str, str] = {}  # dish_id -> per_ingredient | per_item
    for row in _dict_rows(data_dir, "dishes.csv"):
        stock_mode[row["dish_id"]] = row["stock_mode"]

    recipe: dict[str, list[tuple[str, float]]] = {}  # dish_id -> [(ingredient, amount)]
    for row in _dict_rows(data_dir, "dish_ingredients.csv"):
        recipe.setdefault(row["dish_id"], []).append((row["ingredient_id"], float(row["amount"])))

    def counters_of(dish_id: str) -> list[tuple[tuple[str, str], float]]:
        """((kind, item), factor) counters a dish needs under its stock_mode.

        The data dictionary has three modes: per_ingredient keeps an
        ingredient counter per recipe row (factor = the recipe amount),
        per_item keeps one dish counter (factor 1), and none keeps no
        counter at all — even when the dish has recipe rows. Any other
        value is a data error, not something to guess about.
        """
        mode = stock_mode.get(dish_id)
        if mode == "per_ingredient":
            return [
                (("ingredient", ingredient_id), amount)
                for ingredient_id, amount in recipe.get(dish_id, ())
            ]
        if mode == "per_item":
            return [(("dish", dish_id), 1.0)]
        if mode == "none":
            return []
        raise ValueError(f"dish {dish_id}: unknown stock_mode {mode!r}")

    # Every counter a vendor's products can need, before any usage is seen.
    needed: set[tuple[str, str, str]] = set()
    for vendor_id, product_id in sorted(vendor_products):
        for dish_id, _ in links.get((vendor_id, product_id), ()):
            for (kind, item_id), _factor in counters_of(dish_id):
                needed.add((vendor_id, kind, item_id))

    # Usage in the warm-up window: quantity x multiplier x amount, charged
    # to the vendor that took the order (the one whose stock it comes off).
    warm_orders: dict[str, str] = {}  # order_id -> vendor_id
    with (data_dir / "orders.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if int(row["order_day"]) < warm_days:
                warm_orders[row["order_id"]] = row["vendor_id"]

    usage: dict[tuple[str, str, str], float] = {}
    with (data_dir / "order_items.csv").open(newline="", encoding="utf-8") as f:
        for item in csv.DictReader(f):
            vendor_id = warm_orders.get(item["order_id"])
            if vendor_id is None:
                continue
            quantity = float(item["quantity"])
            # the order's vendor is the one whose stock the line comes
            # off, so the links are looked up by (vendor, product)
            for dish_id, multiplier in links.get((vendor_id, item["product_id"]), ()):
                for (kind, item_id), factor in counters_of(dish_id):
                    key = (vendor_id, kind, item_id)
                    usage[key] = usage.get(key, 0.0) + quantity * multiplier * factor

    rows: list[tuple[str, str, str, int, int]] = []
    for vendor_id, item_kind, item_id in sorted(needed):
        average_daily = usage.get((vendor_id, item_kind, item_id), 0.0) / warm_days
        on_hand = max(floor, math.ceil(average_daily * cover_days))
        rows.append((vendor_id, item_kind, item_id, on_hand, on_hand))
    return rows

"""The counting oracle (WP6a): place_order's stock rules, no database.

A plain-Python replay that walks the same orders in the same order as
playback - the same sorted cache, filtered the same way - and applies the
same rules WP1's ``place_order`` applies (docs/work-packages/
wp1-region-write-path.md, steps 3 to 7), with ``amount`` and
``multiplier`` always read from the data and never assumed. Used to check
a region after a sequential run, and to sanity-check the stock
parameters before any real run (docs/replay-stock-parameters.md, rule 5).

The rules are mirrored from the merged SQL and the region driver, not
from the brief's prose; the PR description carries the rule-by-rule
mapping table. What the oracle deliberately does NOT model:

- ``product_availability`` and the out-of-stock marking
  (``mark_out_of_stock``, ``clear_stock_marks``). A product is switched
  off for stock only when one of its counters cannot make one unit, so
  an order for it would be refused on stock anyway - the outcome agrees,
  and only a refused order's *reason* can differ (the run log says
  ``unavailable``, the oracle says ``out_of_stock``); the compare
  subcommand counts that pair as an expected divergence.
- the epoch fence, the inbox and the outbox: playback sends every order
  exactly once and nothing here writes anything.
- ``can_make_one``: that function serves ``set_availability``; the
  order path checks the summed needs directly, and so does the oracle.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from restaurantos.sim.loading import REFERENCE_COLUMNS
from restaurantos.sim.replaydata import manifest_rows

OUTCOME_ACCEPTED = "accepted"
OUTCOME_REFUSED = "refused"
REASON_OUT_OF_STOCK = "out_of_stock"
REASON_UNKNOWN_PRODUCT = "unknown_product"

# (vendor_id, item_kind, item_id): one stock counter.
StockKey = tuple[str, str, str]
# (item_kind, item_id): one counter within one order's needs.
NeedKey = tuple[str, str]

# The reference files the rules are read from, in load order.
RULE_FILES = ("products.csv", "product_dish.csv", "dishes.csv", "dish_ingredients.csv")


def _int(value: Any, what: str) -> int:
    """Parse a whole number from the data, with the data error named."""
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{what}: {value!r} is not a whole number") from error


@dataclass(frozen=True)
class OracleRules:
    """What one unit of each product takes, read once from the data.

    Mirrors what the region database holds after a bootstrap load: the
    products this vendor has, each product's one dish link and its
    multiplier, each dish's stock_mode, and each recipe row's amount.
    """

    products: frozenset[tuple[str, str]]
    """(vendor_id, product_id) that exist: step 3's unknown check."""
    links: dict[tuple[str, str], tuple[str, int]]
    """(vendor_id, product_id) -> (dish_id, multiplier); at most one each."""
    stock_mode: dict[str, str]
    """dish_id -> per_ingredient | per_item | none."""
    recipe: dict[str, tuple[tuple[str, int], ...]]
    """dish_id -> ((ingredient_id, amount), ...), in file order."""

    @classmethod
    def from_dict_rows(
        cls,
        products: Iterable[dict[str, str]],
        product_dish: Iterable[dict[str, str]],
        dishes: Iterable[dict[str, str]],
        dish_ingredients: Iterable[dict[str, str]],
    ) -> OracleRules:
        """Build the rules from dict rows of the four reference files.

        A product_dish row for a vendor that does not have the product,
        a second link for one product, or a recipe row for an unknown
        dish is a data error: the database's keys (product_dish's
        PRIMARY KEY (vendor_id, product_id), dish_ingredients' PRIMARY
        KEY (dish_id, ingredient_id)) forbid them, so the files cannot
        carry them either.
        """
        product_set = frozenset((row["vendor_id"], row["product_id"]) for row in products)
        links: dict[tuple[str, str], tuple[str, int]] = {}
        for row in product_dish:
            key = (row["vendor_id"], row["product_id"])
            if key in links:
                raise ValueError(
                    f"product_dish: {row['product_id']!r} of {row['vendor_id']!r} has two rows"
                )
            links[key] = (row["dish_id"], _int(row["multiplier"], "product_dish multiplier"))
        stock_mode = {row["dish_id"]: row["stock_mode"] for row in dishes}
        recipe: dict[str, tuple[tuple[str, int], ...]] = {}
        grouped: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for row in dish_ingredients:
            dish_id = row["dish_id"]
            ingredient_id = row["ingredient_id"]
            if any(ingredient == ingredient_id for ingredient, _amount in grouped[dish_id]):
                raise ValueError(f"dish_ingredients: {dish_id!r} lists {ingredient_id!r} twice")
            grouped[dish_id].append((ingredient_id, _int(row["amount"], "dish_ingredients amount")))
        for dish_id, rows in grouped.items():
            recipe[dish_id] = tuple(rows)
        return cls(products=product_set, links=links, stock_mode=stock_mode, recipe=recipe)

    @classmethod
    def from_data_dir(cls, data_dir: Path) -> OracleRules:
        """Build the rules from a verified data directory.

        Every file is read through ``manifest_rows`` (sha256 against
        MANIFEST.csv, header against the data dictionary), and the rows
        are turned into dicts by the same headers - never a second,
        unchecked read.
        """
        rows: dict[str, list[dict[str, str]]] = {}
        for name in RULE_FILES:
            header = REFERENCE_COLUMNS[name]
            dicts: list[dict[str, str]] = []
            for number, raw in enumerate(manifest_rows(data_dir, name), start=2):
                if len(raw) != len(header):
                    raise ValueError(
                        f"{name} line {number}: expected {len(header)} values, got {len(raw)}"
                    )
                dicts.append(dict(zip(header, raw, strict=True)))
            rows[name] = dicts
        return cls.from_dict_rows(
            rows["products.csv"],
            rows["product_dish.csv"],
            rows["dishes.csv"],
            rows["dish_ingredients.csv"],
        )


class CountingOracle:
    """The replay engine: same stream, same rules, no database.

    One instance replays one window once. The stream is the same one
    playback walks (``replaydata.iter_orders``, filtered the same way),
    so the oracle's per-order outcomes are comparable, line for line,
    with the run log's order lines.
    """

    def __init__(
        self,
        rules: OracleRules,
        stock_rows: Iterable[tuple[str, str, str, int, int]],
        *,
        restock_days: Iterable[int],
        vendors: Iterable[str],
    ) -> None:
        """``stock_rows`` are (vendor_id, item_kind, item_id, on_hand,
        par_level) - the same shape ``sim.stock.warm_up_stock`` returns
        and the region's stock table holds; ``restock_days`` the days
        the run restocks on (``playback.restock_days`` of the window);
        ``vendors`` every vendor in scope (the region's vendors for a
        single-region run, all of them otherwise).
        """
        self.rules = rules
        self.on_hand: dict[StockKey, int] = {}
        self.par_level: dict[StockKey, int] = {}
        self._by_vendor: dict[str, list[StockKey]] = defaultdict(list)
        for vendor_id, item_kind, item_id, on_hand, par_level in stock_rows:
            key = (vendor_id, item_kind, item_id)
            if key in self.on_hand:
                raise ValueError(f"stock: counter {key} appears twice")
            if on_hand < 0 or par_level < 0:
                raise ValueError(f"stock: counter {key} holds a negative amount")
            self.on_hand[key] = on_hand
            self.par_level[key] = par_level
            self._by_vendor[vendor_id].append(key)
        self._restock_days = list(restock_days)
        if self._restock_days != sorted(self._restock_days):
            raise ValueError("restock_days must be sorted")
        if self._restock_days and self._restock_days[0] <= 0:
            raise ValueError("restock_days must be inside the replay window")
        self._restock_index = 0
        self._vendors = frozenset(vendors)
        self.orders = 0
        self.accepted = 0
        self.refused = 0
        self.refusals: dict[str, int] = defaultdict(int)
        self.counters_ever_zero: set[StockKey] = set()

    # --- the replay ---------------------------------------------------

    def replay(self, orders: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        """Walk the orders, yielding one outcome record per order.

        The records come out in stream order, one per order, the shape
        the report is written from. A restock day applies at 00:00:00
        of that day, before its first order - and before the first
        order of the run when the window starts on one - and restock
        days after the last order still apply, because a playback run
        keeps dispatching them until the window ends.
        """
        for order in orders:
            day = int(order["order_day"])
            while (
                self._restock_index < len(self._restock_days)
                and self._restock_days[self._restock_index] <= day
            ):
                self._restock()
            yield self._place(order)
        while self._restock_index < len(self._restock_days):
            self._restock()

    def _restock(self) -> None:
        """top_up_stock: every counter below par goes to par, never down."""
        self._restock_index += 1
        for vendor_id in sorted(self._by_vendor):
            for key in self._by_vendor[vendor_id]:
                par = self.par_level[key]
                if self.on_hand[key] < par:
                    self.on_hand[key] = par

    def _place(self, order: dict[str, Any]) -> dict[str, Any]:
        """Steps 3 to 7 of place_order over one cached order."""
        vendor_id = order["vendor_id"]
        if vendor_id not in self._vendors:
            # The region filter guarantees this cannot happen; a foreign
            # vendor in the stream means the filter and the data disagree.
            raise ValueError(f"order {order['order_id']}: vendor {vendor_id} is not in scope")
        items = order["items"]
        if not items:
            raise ValueError(
                f"order {order['order_id']}: no lines; the cache drops itemless orders"
            )

        # 3. Unknown products refuse first, whatever the stock says:
        # a product this vendor does not have, or one with no dish link.
        for item in items:
            key = (vendor_id, item["product_id"])
            if key not in self.rules.products or key not in self.rules.links:
                return self._record(order, OUTCOME_REFUSED, REASON_UNKNOWN_PRODUCT)

        # 4. What the order consumes, per counter: the same sums as the
        # needs CTE in add_counters.sql, with the order's quantities.
        needs: dict[NeedKey, int] = defaultdict(int)
        dish_units: dict[str, int] = defaultdict(int)
        for item in items:
            dish_id, multiplier = self.rules.links[(vendor_id, item["product_id"])]
            units = multiplier * int(item["quantity"])
            mode = self.rules.stock_mode.get(dish_id)
            if mode == "per_item":
                needs[("dish", dish_id)] += units
            elif mode == "per_ingredient":
                dish_units[dish_id] += units
            elif mode == "none":
                pass
            else:
                raise ValueError(f"dish {dish_id}: unknown stock_mode {mode!r}")
        for dish_id, units in dish_units.items():
            for ingredient_id, amount in self.rules.recipe.get(dish_id, ()):
                needs[("ingredient", ingredient_id)] += amount * units

        # 6. One short counter refuses the whole order; a missing
        # counter row counts as zero.
        for (item_kind, item_id), need in needs.items():
            if self.on_hand.get((vendor_id, item_kind, item_id), 0) < need:
                return self._record(order, OUTCOME_REFUSED, REASON_OUT_OF_STOCK)

        # 7. Take the summed needs; all-or-nothing, so a refused order
        # above subtracted nothing.
        for (item_kind, item_id), need in needs.items():
            key = (vendor_id, item_kind, item_id)
            self.on_hand[key] -= need
            if self.on_hand[key] == 0:
                self.counters_ever_zero.add(key)
        return self._record(order, OUTCOME_ACCEPTED, None)

    def _record(self, order: dict[str, Any], outcome: str, reason: str | None) -> dict[str, Any]:
        self.orders += 1
        if outcome == OUTCOME_ACCEPTED:
            self.accepted += 1
        else:
            self.refused += 1
            self.refusals[reason or ""] += 1
        record: dict[str, Any] = {
            "kind": "order",
            "seq": self.orders - 1,
            "external_order_id": str(order["order_id"]),
            "vendor_id": order["vendor_id"],
            "order_day": order["order_day"],
            "order_time": order["order_time"],
            "outcome": outcome,
        }
        if reason is not None:
            record["refusal_reason"] = reason
        return record


def write_report(run_dir: Path, oracle: CountingOracle) -> None:
    """Write stock.csv next to report.jsonl: the final counters, sorted.

    The header and column order are the stock table's, so the file can
    be loaded or diffed against the region's table directly.
    """
    with (run_dir / "stock.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(("vendor_id", "item_kind", "item_id", "on_hand", "par_level"))
        for key in sorted(oracle.on_hand):
            writer.writerow((*key, oracle.on_hand[key], oracle.par_level[key]))

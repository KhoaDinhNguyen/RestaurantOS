"""The counting oracle's rules, mirrored rule for rule from place_order.

The mini fixture carries the traps the real dataset cannot (an amount
above 1, a unit other than portion, a multiplier above 1, a dish that
is sold out by hand), so its files pin the arithmetic; synthetic rules
pin the edge cases the fixture does not reach.

Every test mirrors a specific SQL or driver line; the PR description's
mapping table names the pairs.
"""

from __future__ import annotations

import csv
from collections import defaultdict

from restaurantos.sim.mini import mini_dir
from restaurantos.sim.oracle import (
    OUTCOME_ACCEPTED,
    OUTCOME_REFUSED,
    REASON_OUT_OF_STOCK,
    REASON_UNKNOWN_PRODUCT,
    CountingOracle,
    OracleRules,
)
from restaurantos.sim.stock import warm_up_stock

MINI = mini_dir()
MINI_VENDORS = frozenset({"v001", "v002", "v003", "v004"})


def _mini_dicts(name: str) -> list[dict[str, str]]:
    with (MINI / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def mini_rules() -> OracleRules:
    return OracleRules.from_dict_rows(
        _mini_dicts("products.csv"),
        _mini_dicts("product_dish.csv"),
        _mini_dicts("dishes.csv"),
        _mini_dicts("dish_ingredients.csv"),
    )


def mini_stock_rows() -> list[tuple[str, str, str, int, int]]:
    return [
        (
            row["vendor_id"],
            row["item_kind"],
            row["item_id"],
            int(row["on_hand"]),
            int(row["par_level"]),
        )
        for row in _mini_dicts("stock.csv")
    ]


def _numeric(order_id: str) -> int:
    """SQLite's CAST(order_id AS INTEGER): digits parse, anything else is 0."""
    try:
        return int(order_id)
    except ValueError:
        return 0


def mini_orders() -> list[dict]:
    """The fixture's orders joined and sorted the way the cache is:
    (order_day, order_time, order_id as a number, order_id)."""
    items: dict[str, list[dict]] = defaultdict(list)
    for row in _mini_dicts("order_items.csv"):
        items[row["order_id"]].append(
            {"product_id": row["product_id"], "quantity": int(row["quantity"])}
        )
    orders = [
        {
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "vendor_id": row["vendor_id"],
            "geohash": row["geohash"],
            "order_day": int(row["order_day"]),
            "order_time": row["order_time"],
            "items": items[row["order_id"]],
        }
        for row in _mini_dicts("orders.csv")
    ]
    orders.sort(
        key=lambda o: (o["order_day"], o["order_time"], _numeric(o["order_id"]), o["order_id"])
    )
    return orders


def simple_rules(
    links: dict[tuple[str, str], tuple[str, int]],
    modes: dict[str, str],
    recipe: dict[str, list[tuple[str, int]]],
) -> OracleRules:
    return OracleRules(
        products=frozenset(links),
        links=dict(links),
        stock_mode=dict(modes),
        recipe={dish: tuple(rows) for dish, rows in recipe.items()},
    )


# --- the mini fixture, hand-checked ----------------------------------------


def test_mini_rules_read_the_multipliers_and_amounts() -> None:
    rules = mini_rules()
    # the fixture's planted traps: multiplier 2 on p001, amount 3 of caramel
    assert rules.links[("v004", "p001")] == ("d018", 2)
    assert rules.recipe["d018"] == (("caramel", 3),)
    assert rules.links[("v002", "p012")] == ("d017", 5)
    # the product with the all-digit id and leading zero reads as text
    assert rules.links[("v003", "0017")] == ("d008", 1)
    assert rules.stock_mode["d008"] == "none"
    assert rules.stock_mode["d015"] == "per_item"


def test_mini_window_accepts_every_order_and_counts_the_stock() -> None:
    """The fixture's own stock.csv holds plenty for its 20 orders, so the
    whole window is accepted and only the takes below remain."""
    oracle = CountingOracle(mini_rules(), mini_stock_rows(), restock_days=[], vendors=MINI_VENDORS)
    records = list(oracle.replay(mini_orders()))
    assert [record["outcome"] for record in records] == [OUTCOME_ACCEPTED] * 20
    assert oracle.refused == 0
    # p001 at v004: caramel amount 3 x multiplier 2 x quantity 1 (o0014)
    assert oracle.on_hand[("v004", "ingredient", "caramel")] == 0
    assert oracle.counters_ever_zero == {("v004", "ingredient", "caramel")}
    # five cupcake orders at v004 (o0014, o0002, o0007, o0015, o0016)
    assert oracle.on_hand[("v004", "ingredient", "cupcakes")] == 45
    # beef: amount 2 x multiplier 1 x quantity 1 (o0005)
    assert oracle.on_hand[("v001", "ingredient", "beef")] == 48
    # p015 is per_item: the dish counter goes down by the multiplier (o0003)
    assert oracle.on_hand[("v001", "dish", "d013")] == 39
    # two orders at v003 take rice: d002 and d003 share the counter (o0001)
    assert oracle.on_hand[("v003", "ingredient", "rice")] == 48
    assert oracle.on_hand[("v003", "dish", "d011")] == 39
    # 0017's dish is stock_mode none: its counter is never touched
    assert oracle.on_hand[("v003", "dish", "d008")] == 40


def test_mini_window_refuses_when_the_warm_up_floor_is_too_low() -> None:
    """Starting from warm-up-derived stock (every counter at its floor,
    F = 5 here: no mini order falls inside the warm-up window), the two
    orders that need 6 fish (p012's multiplier 5 plus p019's 1) are
    refused whole: shared counter across two lines, all-or-nothing."""
    stock = warm_up_stock(MINI, warm_days=14, cover_days=7, floor=5)
    oracle = CountingOracle(mini_rules(), stock, restock_days=[], vendors=MINI_VENDORS)
    records = list(oracle.replay(mini_orders()))
    # in replay order: o0011 is day 14 19:33:19, o0008 is day 15 01:35:26
    refused = [r["external_order_id"] for r in records if r["outcome"] == OUTCOME_REFUSED]
    assert refused == ["o0011", "o0008"]
    assert oracle.refusals == {REASON_OUT_OF_STOCK: 2}
    # the refusals took nothing: fish keeps its floor until o0012 and o0013
    assert oracle.on_hand[("v002", "ingredient", "fish")] == 3
    assert oracle.accepted == 18
    # every record carries the refusal reason the run log would carry
    for record in records:
        if record["external_order_id"] in refused:
            assert record["refusal_reason"] == REASON_OUT_OF_STOCK
        else:
            assert "refusal_reason" not in record


def test_warm_up_stock_on_the_mini_fixture_floors_one_unit() -> None:
    """The counter floor covers one unit of the hungriest product:
    caramel's floor is 6 (amount 3 x multiplier 2), not F; a none-mode
    dish keeps no counter at all."""
    rows = warm_up_stock(MINI, warm_days=14, cover_days=7, floor=5)
    by_key = {(row[0], row[1], row[2]): (row[3], row[4]) for row in rows}
    assert len(rows) == len(by_key) == 17
    assert by_key[("v004", "ingredient", "caramel")] == (6, 6)
    assert by_key[("v001", "ingredient", "beef")] == (5, 5)
    assert ("v003", "dish", "d008") not in by_key  # stock_mode none: no counter


# --- the rules, one by one ---------------------------------------------------


def test_need_multiplies_quantity_multiplier_and_amount() -> None:
    """The needs CTE in add_counters.sql, times the line's quantity:
    per_ingredient takes amount x multiplier x quantity, per_item takes
    multiplier x quantity (WP1 done-when: 250 x 2 x 3 = 1500)."""
    rules = simple_rules(
        {("v", "p1"): ("d1", 2), ("v", "p2"): ("d2", 1)},
        {"d1": "per_ingredient", "d2": "per_item"},
        {"d1": [("i", 250)]},
    )
    stock = [("v", "ingredient", "i", 2_000, 2_000), ("v", "dish", "d2", 100, 200)]
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p1", "quantity": 3}],
        },
        {
            "order_id": "o2",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "09:00:00",
            "items": [{"product_id": "p2", "quantity": 7}],
        },
    ]
    oracle = CountingOracle(rules, stock, restock_days=[], vendors={"v"})
    records = list(oracle.replay(orders))
    assert [r["outcome"] for r in records] == [OUTCOME_ACCEPTED] * 2
    assert oracle.on_hand[("v", "ingredient", "i")] == 2_000 - 250 * 2 * 3
    assert oracle.on_hand[("v", "dish", "d2")] == 100 - 1 * 7


def test_two_lines_of_one_order_share_the_counter_and_fail_together() -> None:
    """Step 6 refuses the whole order when one counter is short, and a
    refused order subtracts nothing (take_stock runs once, after)."""
    rules = simple_rules(
        {("v", "pa"): ("da", 1), ("v", "pb"): ("db", 1)},
        {"da": "per_ingredient", "db": "per_ingredient"},
        {"da": [("shared", 4)], "db": [("shared", 4), ("only_b", 1)]},
    )
    stock = [("v", "ingredient", "shared", 7, 10), ("v", "ingredient", "only_b", 5, 5)]
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "pa", "quantity": 1}, {"product_id": "pb", "quantity": 1}],
        },
    ]
    oracle = CountingOracle(rules, stock, restock_days=[], vendors={"v"})
    (record,) = list(oracle.replay(orders))
    # both lines together need 8 of a counter holding 7: refused, whole
    assert record["outcome"] == OUTCOME_REFUSED
    assert record["refusal_reason"] == REASON_OUT_OF_STOCK
    assert oracle.on_hand == {("v", "ingredient", "shared"): 7, ("v", "ingredient", "only_b"): 5}


def test_a_missing_counter_row_counts_as_zero() -> None:
    """lock_stock returns no row for a counter that was never created;
    place_order treats that as zero and refuses."""
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "per_ingredient"}, {"d": [("ghost", 1)]})
    oracle = CountingOracle(rules, [], restock_days=[], vendors={"v"})
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p", "quantity": 1}],
        },
    ]
    (record,) = list(oracle.replay(orders))
    assert record["outcome"] == OUTCOME_REFUSED
    assert record["refusal_reason"] == REASON_OUT_OF_STOCK


def test_unknown_products_refuse_before_stock_is_even_looked_at() -> None:
    """Step 3: a product this vendor does not have, or one with no dish
    link, refuses as unknown_product - even when stock would also be
    short, and even for another line of the same order."""
    rules = simple_rules(
        {("v", "linked"): ("d", 1), ("other", "p"): ("d", 1)},
        {"d": "per_item"},
        {},
    )
    # "unlinked" exists as a product but has no product_dish row
    rules = OracleRules(
        products=frozenset({("v", "linked"), ("v", "unlinked"), ("other", "p")}),
        links=dict(rules.links),
        stock_mode=dict(rules.stock_mode),
        recipe=dict(rules.recipe),
    )
    stock = [("v", "dish", "d", 0, 5)]  # also out of stock
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "unlinked", "quantity": 1}],
        },
        {
            "order_id": "o2",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:30:00",
            "items": [{"product_id": "p", "quantity": 1}],
        },  # another vendor's product
        {
            "order_id": "o3",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "09:00:00",
            "items": [{"product_id": "linked", "quantity": 1}],
        },  # stock would refuse this
    ]
    oracle = CountingOracle(rules, stock, restock_days=[], vendors={"v", "other"})
    records = list(oracle.replay(orders))
    assert [r["refusal_reason"] for r in records] == [
        REASON_UNKNOWN_PRODUCT,
        REASON_UNKNOWN_PRODUCT,
        REASON_OUT_OF_STOCK,
    ]


def test_a_none_dish_never_refuses_and_never_takes() -> None:
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "none"}, {"d": [("i", 1)]})
    oracle = CountingOracle(rules, [], restock_days=[], vendors={"v"})
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p", "quantity": 1_000}],
        },
    ]
    (record,) = list(oracle.replay(orders))
    assert record["outcome"] == OUTCOME_ACCEPTED
    assert oracle.on_hand == {}


def test_a_per_ingredient_dish_with_no_recipe_rows_takes_nothing() -> None:
    """recipes.sql returns nothing for such a dish, so the order needs
    no counter: it is accepted and changes no stock."""
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "per_ingredient"}, {})
    oracle = CountingOracle(rules, [], restock_days=[], vendors={"v"})
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p", "quantity": 5}],
        },
    ]
    (record,) = list(oracle.replay(orders))
    assert record["outcome"] == OUTCOME_ACCEPTED
    assert oracle.on_hand == {}


# --- restock -----------------------------------------------------------------


def test_restock_at_midnight_precedes_that_days_first_order() -> None:
    """A restock fires at 00:00:00 of its day, before the first order of
    the day - even when that order is itself at 00:00:00 (the dispatcher
    ranks restocks first)."""
    rules = simple_rules({("v", "p"): ("d", 2)}, {"d": "per_item"}, {})
    stock = [("v", "dish", "d", 3, 10)]
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 21,
            "order_time": "00:00:00",
            "items": [{"product_id": "p", "quantity": 2}],
        },
    ]
    # without the restock day, the order needs 4 of 3 and is refused
    plain = CountingOracle(rules, stock, restock_days=[], vendors={"v"})
    (refused,) = list(plain.replay([dict(o) for o in orders]))
    assert refused["outcome"] == OUTCOME_REFUSED
    # on a restock day the vendor is topped up first: 10 >= 4
    restocked = CountingOracle(rules, stock, restock_days=[21], vendors={"v"})
    (accepted,) = list(restocked.replay([dict(o) for o in orders]))
    assert accepted["outcome"] == OUTCOME_ACCEPTED
    assert restocked.on_hand[("v", "dish", "d")] == 6


def test_restock_goes_up_to_par_never_down() -> None:
    """top_up_stock: on_hand = par_level WHERE on_hand < par_level; a
    counter at or above par is untouched."""
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "per_item"}, {})
    stock = [
        ("v", "dish", "d", 2, 10),  # below par: goes up
        ("v", "ingredient", "i", 50, 10),  # above par: stays
    ]
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p", "quantity": 1}],
        },
    ]
    oracle = CountingOracle(rules, stock, restock_days=[15], vendors={"v"})
    records = list(oracle.replay(orders))
    # the restock day sits after the last order: it still applies,
    # because a run keeps dispatching restocks until the window ends
    assert [r["outcome"] for r in records] == [OUTCOME_ACCEPTED]
    assert oracle.on_hand[("v", "dish", "d")] == 10
    assert oracle.on_hand[("v", "ingredient", "i")] == 50


def test_restock_days_without_orders_still_apply() -> None:
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "per_item"}, {})
    stock = [("v", "dish", "d", 4, 10)]
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "v",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p", "quantity": 4}],
        },
    ]
    oracle = CountingOracle(rules, stock, restock_days=[16, 23], vendors={"v"})
    list(oracle.replay(orders))  # day 14 order; days 16 and 23 have no orders
    assert oracle.on_hand[("v", "dish", "d")] == 10


# --- the guards --------------------------------------------------------------


def test_a_vendor_outside_the_scope_is_a_data_error() -> None:
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "none"}, {})
    oracle = CountingOracle(rules, [], restock_days=[], vendors={"v"})
    orders = [
        {
            "order_id": "o1",
            "vendor_id": "elsewhere",
            "order_day": 14,
            "order_time": "08:00:00",
            "items": [{"product_id": "p", "quantity": 1}],
        },
    ]
    try:
        list(oracle.replay(orders))
    except ValueError as error:
        assert "elsewhere" in str(error)
    else:
        raise AssertionError("a foreign vendor in the stream must raise")


def test_duplicate_and_negative_counters_are_refused() -> None:
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "none"}, {})
    try:
        CountingOracle(
            rules,
            [("v", "dish", "d", 1, 1), ("v", "dish", "d", 1, 1)],
            restock_days=[],
            vendors={"v"},
        )
    except ValueError as error:
        assert "appears twice" in str(error)
    else:
        raise AssertionError("a doubled counter row must raise")


def test_unsorted_restock_days_are_refused() -> None:
    rules = simple_rules({("v", "p"): ("d", 1)}, {"d": "none"}, {})
    try:
        CountingOracle(rules, [], restock_days=[21, 15], vendors={"v"})
    except ValueError as error:
        assert "sorted" in str(error)
    else:
        raise AssertionError("unsorted restock days must raise")

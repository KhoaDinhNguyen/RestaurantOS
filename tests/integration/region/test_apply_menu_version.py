"""apply_menu_version against one PostgreSQL. Needs ROS_TEST_DSN.

WP1 brief, "apply_menu_version"; architecture Sections 5.2 and 6.1. The rows
have the shape the menu publisher (relay/menu.py) sends: this region's whole
HQ slice, one mapping per product.

Each test gets its own throwaway schema with the region schema applied. The
recipe amounts and multipliers are above 1, as the brief asks.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from restaurantos.common.db import translate_db_errors
from restaurantos.common.errors import StaleEpochError
from restaurantos.common.sqlfiles import schema_files
from restaurantos.region import DishLinkChangedError, apply_menu_version, restock
from restaurantos.sim.config import get_sim_settings

pytestmark = pytest.mark.integration

type Connect = Callable[[], Awaitable[psycopg.AsyncConnection]]

SEED = """
INSERT INTO node_state (region_id, menu_version) VALUES ('r1', 3);

INSERT INTO ingredients (ingredient_id, source) VALUES
    ('rice', 'test'), ('beef', 'test'), ('chili', 'test');

INSERT INTO dishes (dish_id, dish_name, scope, stock_mode) VALUES
    ('d_rice', 'beef rice', 'global', 'per_ingredient'),
    ('d_laksa', 'laksa', 'global', 'per_ingredient'),
    ('d_cake', 'cake', 'global', 'per_item'),
    ('d_pie', 'pie', 'global', 'per_item'),
    ('d_tea', 'tea', 'global', 'none');

INSERT INTO dish_ingredients (dish_id, ingredient_id, amount, unit) VALUES
    ('d_rice', 'rice', 250, 'gram'),
    ('d_rice', 'beef', 2, 'kg'),
    ('d_laksa', 'rice', 100, 'gram'),
    ('d_laksa', 'chili', 3, 'gram');

INSERT INTO vendors (vendor_id, geohash) VALUES ('v1', 'w21z7'), ('v2', 'w21z7');

INSERT INTO products (vendor_id, product_id, name, unit_price, price_units, menu_version, origin)
VALUES
    ('v1', 'p_rice', 'Beef rice, double', 0.012, 30, 1, 'hq'),
    ('v1', 'p_cake', 'Cake, box of 3', 0.0048, 12, 1, 'hq'),
    ('v1', 'p_local', 'Made by the region', 0.0008, 2, 1, 'region');

INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier) VALUES
    ('v1', 'p_rice', 'd_rice', 2),
    ('v1', 'p_cake', 'd_cake', 3),
    ('v1', 'p_local', 'd_tea', 1);

INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level) VALUES
    ('v1', 'ingredient', 'rice', 1000, 2000),
    ('v1', 'ingredient', 'beef', 0, 100),
    ('v1', 'dish', 'd_cake', 6, 30);

-- p_rice is off because beef ran out; p_cake was switched off by hand.
INSERT INTO product_availability (vendor_id, product_id, available, reason) VALUES
    ('v1', 'p_rice', false, 'stock'),
    ('v1', 'p_cake', false, 'manual');
"""


@pytest.fixture
async def region(monkeypatch) -> AsyncIterator[Connect]:
    """A fresh seeded region schema. Yields a function that opens connections to it."""
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    # F as bootstrap uses it, pinned so a local .env cannot change the numbers.
    monkeypatch.setenv("ROS_SIM_STOCK_FLOOR", "5")
    get_sim_settings.cache_clear()

    schema = f"ros_test_wp1_{uuid4().hex[:8]}"
    opened: list[psycopg.AsyncConnection] = []

    async def connect() -> psycopg.AsyncConnection:
        # autocommit: apply_menu_version's own transaction() is then a real
        # BEGIN and COMMIT, and the check queries never leave a transaction open.
        conn = await psycopg.AsyncConnection.connect(
            dsn, autocommit=True, options=f"-c search_path={schema}"
        )
        opened.append(conn)
        return conn

    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        setup = await connect()
        for path in schema_files("region"):
            await setup.execute(path.read_text(encoding="utf-8"))
        await setup.execute(SEED)
        yield connect
    finally:
        for conn in opened:
            await conn.close()
        await admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        await admin.close()
        get_sim_settings.cache_clear()


def row(
    product: str,
    dish: str,
    multiplier: int,
    *,
    name: str | None = None,
    price_units: int = 10,
    menu_version: int = 4,
    vendor: str = "v1",
) -> dict[str, Any]:
    """One product of the slice, shaped like relay/read_menu_slice.sql."""
    return {
        "vendor_id": vendor,
        "product_id": product,
        "name": name or product,
        "unit_price": Decimal(price_units) / 2500,
        "price_units": price_units,
        "menu_version": menu_version,
        "origin": "hq",
        "dish_id": dish,
        "multiplier": multiplier,
        "multiplier_ambiguous": False,
    }


# The slice as the region has it now; each test adds or changes rows.
P_RICE = row("p_rice", "d_rice", 2, name="Beef rice, double", price_units=30, menu_version=1)
P_CAKE = row("p_cake", "d_cake", 3, name="Cake, box of 3", price_units=12, menu_version=1)


async def apply(conn: psycopg.AsyncConnection, *rows: dict, version: int = 4, epoch: int = 1):
    await apply_menu_version(conn, version, list(rows), epoch)


async def fetch_all(conn: psycopg.AsyncConnection, query: str, *params: Any) -> list[tuple]:
    cur = await conn.execute(query, params)
    return await cur.fetchall()


async def menu_version(conn: psycopg.AsyncConnection) -> int:
    return (await fetch_all(conn, "SELECT menu_version FROM node_state"))[0][0]


async def counters(conn: psycopg.AsyncConnection) -> dict[str, tuple[int, int]]:
    """item_id -> (on_hand, par_level) for v1."""
    rows = await fetch_all(
        conn, "SELECT item_id, on_hand, par_level FROM stock WHERE vendor_id = 'v1'"
    )
    return {item: (on_hand, par) for item, on_hand, par in rows}


async def marks(conn: psycopg.AsyncConnection) -> dict[str, str]:
    rows = await fetch_all(
        conn,
        "SELECT product_id, reason FROM product_availability"
        " WHERE vendor_id = 'v1' AND available = false",
    )
    return dict(rows)


async def everything(conn: psycopg.AsyncConnection) -> list[list[tuple]]:
    """Every row apply_menu_version could touch, for the tests that expect no change."""
    return [
        await fetch_all(conn, "SELECT * FROM node_state"),
        await fetch_all(conn, "SELECT * FROM products ORDER BY vendor_id, product_id"),
        await fetch_all(conn, "SELECT * FROM product_dish ORDER BY vendor_id, product_id"),
        await fetch_all(conn, "SELECT * FROM stock ORDER BY vendor_id, item_kind, item_id"),
        await fetch_all(conn, "SELECT * FROM product_availability ORDER BY vendor_id, product_id"),
    ]


# --- the version guard ----------------------------------------------------------


@pytest.mark.parametrize("version", [2, 3], ids=["below the node's", "the node's own"])
async def test_a_version_at_or_below_the_nodes_own_changes_nothing(region, version):
    conn = await region()
    state = await everything(conn)

    await apply(conn, row("p_new", "d_cake", 1), version=version)

    assert await everything(conn) == state


async def test_the_node_records_the_version(region):
    conn = await region()

    await apply(conn, P_RICE, P_CAKE, version=7)

    assert await menu_version(conn) == 7


async def test_the_same_version_twice_is_applied_once(region):
    conn = await region()
    await apply(conn, P_RICE, P_CAKE, row("p_laksa", "d_laksa", 2))
    state = await everything(conn)

    await apply(conn, P_RICE, P_CAKE, row("p_laksa", "d_laksa", 2, name="changed"))

    assert await everything(conn) == state


# --- a changed product ----------------------------------------------------------


async def test_a_changed_product_gets_its_new_name_price_and_version(region):
    conn = await region()

    await apply(conn, row("p_rice", "d_rice", 2, name="Beef rice XL", price_units=35), P_CAKE)

    assert await fetch_all(
        conn,
        "SELECT name, price_units, unit_price, menu_version, origin FROM products"
        " WHERE product_id = 'p_rice'",
    ) == [("Beef rice XL", 35, Decimal(35) / 2500, 4, "hq")]


async def test_an_unchanged_product_keeps_its_own_menu_version(region):
    conn = await region()

    await apply(conn, P_RICE, row("p_cake", "d_cake", 3, name="Cake, new box"))

    versions = await fetch_all(conn, "SELECT product_id, menu_version FROM products")
    assert dict(versions) == {"p_rice": 1, "p_cake": 4, "p_local": 1}


async def test_a_changed_product_keeps_its_marks_and_its_counters(region):
    conn = await region()
    before = await counters(conn)

    await apply(
        conn,
        row("p_rice", "d_rice", 2, name="renamed"),
        row("p_cake", "d_cake", 3, name="renamed too"),
    )

    assert await marks(conn) == {"p_rice": "stock", "p_cake": "manual"}
    assert await counters(conn) == before


async def test_unit_price_keeps_every_digit(region):
    conn = await region()
    precise = {**P_CAKE, "unit_price": Decimal("0.1234567890123456789"), "menu_version": 4}

    await apply(conn, P_RICE, precise)

    assert await fetch_all(conn, "SELECT unit_price FROM products WHERE product_id = 'p_cake'") == [
        (Decimal("0.1234567890123456789"),)
    ]


# --- an added product: counters ---------------------------------------------------


async def test_an_added_product_gets_the_counters_it_lacks_with_nothing_on_hand(region):
    conn = await region()

    # One laksa x 2 needs 200 rice (counter exists) and 6 chili (no counter).
    await apply(conn, P_RICE, P_CAKE, row("p_laksa", "d_laksa", 2))

    found = await counters(conn)
    assert found["chili"] == (0, 6)  # par = max(F=5, 3 x 2)
    assert found["rice"] == (1000, 2000)  # existed, par already above 200


async def test_par_is_raised_to_one_units_need_without_touching_on_hand(region):
    conn = await region()

    # One p_rice_big needs 250 x 20 = 5000 rice and 2 x 20 = 40 beef.
    await apply(conn, P_RICE, P_CAKE, row("p_rice_big", "d_rice", 20))

    found = await counters(conn)
    assert found["rice"] == (1000, 5000)
    assert found["beef"] == (0, 100)  # 40 is below the par of 100: unchanged


async def test_par_is_never_lowered(region):
    conn = await region()

    await apply(conn, P_RICE, P_CAKE, row("p_cake_one", "d_cake", 1))

    assert (await counters(conn))["d_cake"] == (6, 30)


@pytest.mark.parametrize(("floor", "par"), [("5", 5), ("9", 9)])
async def test_a_new_counter_starts_at_the_bootstrap_floor(region, monkeypatch, floor, par):
    monkeypatch.setenv("ROS_SIM_STOCK_FLOOR", floor)
    get_sim_settings.cache_clear()
    conn = await region()

    # One pie needs 1 of a counter that does not exist yet.
    await apply(conn, P_RICE, P_CAKE, row("p_pie", "d_pie", 1))

    assert (await counters(conn))["d_pie"] == (0, par)


async def test_a_need_above_the_floor_sets_the_par(region):
    conn = await region()

    await apply(conn, P_RICE, P_CAKE, row("p_pie_box", "d_pie", 8))

    assert (await counters(conn))["d_pie"] == (0, 8)


async def test_two_added_products_on_one_counter_take_the_larger_need(region):
    conn = await region()

    await apply(conn, P_RICE, P_CAKE, row("p_pie", "d_pie", 1), row("p_pie_box", "d_pie", 8))

    assert (await counters(conn))["d_pie"] == (0, 8)


async def test_a_product_with_stock_mode_none_gets_no_counter(region):
    conn = await region()
    before = await counters(conn)

    await apply(conn, P_RICE, P_CAKE, row("p_tea", "d_tea", 2))

    assert await counters(conn) == before
    assert "p_tea" not in await marks(conn)


# --- an added product: availability ---------------------------------------------------


async def test_an_added_product_that_cannot_make_one_unit_gets_a_stock_mark(region):
    conn = await region()

    await apply(conn, P_RICE, P_CAKE, row("p_laksa", "d_laksa", 2))

    assert (await marks(conn))["p_laksa"] == "stock"


async def test_an_added_product_that_can_make_one_unit_stays_on(region):
    conn = await region()

    # 6 cakes on hand; one p_cake_two takes 2.
    await apply(conn, P_RICE, P_CAKE, row("p_cake_two", "d_cake", 2))

    assert "p_cake_two" not in await marks(conn)


async def test_the_next_restock_switches_an_added_product_on(region):
    conn = await region()
    await apply(conn, P_RICE, P_CAKE, row("p_laksa", "d_laksa", 2))

    await restock(conn, "v1", 1)

    assert "p_laksa" not in await marks(conn)
    assert (await counters(conn))["chili"] == (6, 6)


async def waits_on_a_lock(watch: psycopg.AsyncConnection, pid: int, task: asyncio.Task) -> bool:
    """True once backend `pid` waits on a lock; False if `task` ends first or 5 s pass."""
    for _ in range(250):
        if task.done():
            return False
        cur = await watch.execute(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,)
        )
        if await cur.fetchone() == ("Lock",):
            return True
        await asyncio.sleep(0.02)
    return False


async def test_a_restock_waits_for_a_menu_apply_in_flight(region):
    applying, restocking, watch = await region(), await region(), await region()

    # apply's own transaction() is a savepoint inside this one, so its
    # node_state lock is held until this block commits. p_pie's counter is new.
    async with applying.transaction():
        await apply(applying, P_RICE, P_CAKE, row("p_pie", "d_pie", 1))
        task = asyncio.create_task(restock(restocking, "v1", 1))
        waited = await waits_on_a_lock(watch, restocking.info.backend_pid, task)
    await task

    assert waited
    # The restock ran after the apply committed: it filled the new counter
    # before clearing the new product's mark.
    assert (await counters(watch))["d_pie"] == (5, 5)
    assert "p_pie" not in await marks(watch)


# --- what is never applied ----------------------------------------------------------


@pytest.mark.parametrize(
    "changed",
    [
        pytest.param(row("p_rice", "d_laksa", 2), id="another dish"),
        pytest.param(row("p_rice", "d_rice", 3), id="another multiplier"),
    ],
)
async def test_a_different_dish_link_refuses_the_whole_version(region, changed):
    conn = await region()
    state = await everything(conn)

    with pytest.raises(DishLinkChangedError):
        await apply(conn, changed, P_CAKE, row("p_laksa", "d_laksa", 2))

    assert await everything(conn) == state


async def test_a_product_the_region_created_is_never_touched(region):
    conn = await region()
    before = await fetch_all(conn, "SELECT * FROM products WHERE product_id = 'p_local'")

    # Same id as the region's own product, other name, price and even dish.
    await apply(conn, P_RICE, P_CAKE, row("p_local", "d_cake", 5, name="from HQ", price_units=99))

    assert await fetch_all(conn, "SELECT * FROM products WHERE product_id = 'p_local'") == before
    assert await fetch_all(
        conn, "SELECT dish_id, multiplier FROM product_dish WHERE product_id = 'p_local'"
    ) == [("d_tea", 1)]
    assert await menu_version(conn) == 4


async def test_a_failure_part_way_applies_nothing(region):
    conn = await region()
    state = await everything(conn)

    # The last row names a dish that does not exist. The products upsert
    # succeeds for every row, then the product_dish foreign key fails in the
    # next statement, so the products rows are only undone by the rollback.
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await apply(conn, P_RICE, row("p_laksa", "d_laksa", 2), row("p_x", "d_missing", 1))

    assert await everything(conn) == state


async def test_stale_epoch_is_rejected_and_changes_nothing(region):
    conn = await region()
    state = await everything(conn)

    with pytest.raises(StaleEpochError):
        async with translate_db_errors():  # what run_on does for real callers
            await apply(conn, P_RICE, P_CAKE, row("p_laksa", "d_laksa", 2), epoch=2)

    assert await everything(conn) == state

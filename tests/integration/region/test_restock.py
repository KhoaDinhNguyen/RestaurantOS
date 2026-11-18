"""restock against one PostgreSQL. Needs ROS_TEST_DSN.

Only the no-body restock is built: every counter of the vendor goes up to its
par level and never down, every `stock` mark of the vendor is cleared, and
`manual` marks stay (WP1 brief, "The rest"; architecture Section 6.1,
POST /vendors/{vendor_id}/restock).

Each test gets its own throwaway schema with the region schema applied. The
recipe amounts and multipliers are above 1, as the brief asks.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import time
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from restaurantos.common.db import translate_db_errors
from restaurantos.common.errors import StaleEpochError, UnknownVendorError
from restaurantos.common.models import OrderItemIn, OrderOutcome, OrderRequest, RefusalReason
from restaurantos.common.sqlfiles import schema_files
from restaurantos.region import place_order, restock

pytestmark = pytest.mark.integration

type Connect = Callable[[], Awaitable[psycopg.AsyncConnection]]

SEED = """
INSERT INTO node_state (region_id) VALUES ('r1');

INSERT INTO ingredients (ingredient_id, source) VALUES
    ('rice', 'test'), ('beef', 'test'), ('salt', 'test');

INSERT INTO dishes (dish_id, dish_name, scope, stock_mode) VALUES
    ('d_rice', 'beef rice', 'global', 'per_ingredient'),
    ('d_cake', 'cake', 'global', 'per_item'),
    ('d_tea', 'tea', 'global', 'none');

INSERT INTO dish_ingredients (dish_id, ingredient_id, amount, unit) VALUES
    ('d_rice', 'rice', 250, 'gram'),
    ('d_rice', 'beef', 2, 'kg');

-- v3 sells only tea (stock_mode none): it has no counters at all.
INSERT INTO vendors (vendor_id, geohash) VALUES
    ('v1', 'w21z7'), ('v2', 'w21z7'), ('v3', 'w21z7');

INSERT INTO products (vendor_id, product_id, name, unit_price, price_units) VALUES
    ('v1', 'p_rice', 'Beef rice, double', 0.012, 30),
    ('v1', 'p_rice_small', 'Beef rice', 0.006, 15),
    ('v1', 'p_cake', 'Cake, box of 3', 0.0048, 12),
    ('v1', 'p_cake_one', 'Cake', 0.0016, 4),
    ('v2', 'p_cake', 'Cake, box of 3', 0.0048, 12),
    ('v3', 'p_tea', 'Tea', 0.0008, 2);

INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier) VALUES
    ('v1', 'p_rice', 'd_rice', 2),
    ('v1', 'p_rice_small', 'd_rice', 1),
    ('v1', 'p_cake', 'd_cake', 3),
    ('v1', 'p_cake_one', 'd_cake', 1),
    ('v2', 'p_cake', 'd_cake', 3),
    ('v3', 'p_tea', 'd_tea', 1);

-- v1: rice below par, beef empty, salt above par (no recipe uses it, which
-- is allowed: a restaurant can stock up before a menu item exists), cake at par.
INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level) VALUES
    ('v1', 'ingredient', 'rice', 1500, 2000),
    ('v1', 'ingredient', 'beef', 0, 100),
    ('v1', 'ingredient', 'salt', 80, 50),
    ('v1', 'dish', 'd_cake', 30, 30),
    ('v2', 'dish', 'd_cake', 1, 30);

-- v1: both rice products are off because beef ran out. p_cake_one carries a
-- stock mark although its counter is at par; p_cake was switched off by hand.
INSERT INTO product_availability (vendor_id, product_id, available, reason) VALUES
    ('v1', 'p_rice', false, 'stock'),
    ('v1', 'p_rice_small', false, 'stock'),
    ('v1', 'p_cake_one', false, 'stock'),
    ('v1', 'p_cake', false, 'manual'),
    ('v2', 'p_cake', false, 'stock');
"""


@pytest.fixture
async def region() -> AsyncIterator[Connect]:
    """A fresh seeded region schema. Yields a function that opens connections to it."""
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    schema = f"ros_test_wp1_{uuid4().hex[:8]}"
    opened: list[psycopg.AsyncConnection] = []

    async def connect() -> psycopg.AsyncConnection:
        # autocommit: restock's own transaction() is then a real BEGIN and
        # COMMIT, and the check queries never leave a transaction open.
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


async def fetch_all(conn: psycopg.AsyncConnection, query: str, *params: Any) -> list[tuple]:
    cur = await conn.execute(query, params)
    return await cur.fetchall()


async def counters(conn: psycopg.AsyncConnection, vendor: str = "v1") -> dict[str, tuple]:
    """item_id -> (on_hand, par_level, updated_at) for one vendor."""
    rows = await fetch_all(
        conn,
        "SELECT item_id, on_hand, par_level, updated_at FROM stock WHERE vendor_id = %s",
        vendor,
    )
    return {item: (on_hand, par, updated) for item, on_hand, par, updated in rows}


async def on_hand(conn: psycopg.AsyncConnection, vendor: str = "v1") -> dict[str, int]:
    return {item: row[0] for item, row in (await counters(conn, vendor)).items()}


async def marks(conn: psycopg.AsyncConnection, vendor: str = "v1") -> dict[str, str]:
    """product_id -> reason, for every product of the vendor that is switched off."""
    rows = await fetch_all(
        conn,
        "SELECT product_id, reason FROM product_availability"
        " WHERE vendor_id = %s AND available = false",
        vendor,
    )
    return dict(rows)


async def everything(conn: psycopg.AsyncConnection) -> list[list[tuple]]:
    """Every row restock could touch, for the tests that expect no change at all."""
    return [
        await fetch_all(conn, "SELECT * FROM stock ORDER BY vendor_id, item_kind, item_id"),
        await fetch_all(conn, "SELECT * FROM product_availability ORDER BY vendor_id, product_id"),
        await fetch_all(conn, "SELECT * FROM outbox ORDER BY seq"),
    ]


def numbers(result: Any) -> tuple[int, int]:
    return result.counters_restocked, result.products_back_on


def order(product: str, quantity: int, ext: str) -> OrderRequest:
    return OrderRequest(
        channel="test",
        external_order_id=ext,
        vendor_id="v1",
        order_day=14,
        order_time=time(12, 3, 29),
        items=[OrderItemIn(product_id=product, quantity=quantity)],
    )


# --- counters -----------------------------------------------------------------


async def test_every_counter_below_par_goes_up_to_par(region):
    conn = await region()

    result = await restock(conn, "v1", 1)

    assert await on_hand(conn) == {"rice": 2000, "beef": 100, "salt": 80, "d_cake": 30}
    # rice and beef went up; salt (above par) and d_cake (at par) did not.
    assert result.counters_restocked == 2


async def test_a_counter_above_par_is_never_lowered(region):
    conn = await region()

    await restock(conn, "v1", 1)

    assert (await counters(conn))["salt"][:2] == (80, 50)


async def test_par_levels_do_not_change(region):
    conn = await region()
    pars_before = {item: row[1] for item, row in (await counters(conn)).items()}

    await restock(conn, "v1", 1)

    assert {item: row[1] for item, row in (await counters(conn)).items()} == pars_before


async def test_updated_at_moves_only_on_counters_that_went_up(region):
    conn = await region()
    before = await counters(conn)

    await restock(conn, "v1", 1)

    after = await counters(conn)
    assert after["rice"][2] > before["rice"][2]
    assert after["beef"][2] > before["beef"][2]
    assert after["salt"][2] == before["salt"][2]
    assert after["d_cake"][2] == before["d_cake"][2]


# --- availability marks -------------------------------------------------------


async def test_every_stock_mark_of_the_vendor_is_cleared_and_counted(region):
    conn = await region()

    result = await restock(conn, "v1", 1)

    assert await marks(conn) == {"p_cake": "manual"}
    assert result.products_back_on == 3


async def test_a_stock_mark_is_cleared_even_when_its_counter_was_already_at_par(region):
    conn = await region()

    await restock(conn, "v1", 1)

    # d_cake was at par, so no counter changed for p_cake_one, but every stock
    # mark of the vendor goes.
    assert "p_cake_one" not in await marks(conn)


async def test_a_manual_mark_stays(region):
    conn = await region()

    await restock(conn, "v1", 1)

    assert (await marks(conn))["p_cake"] == "manual"


# --- scope and repetition -----------------------------------------------------


async def test_other_vendors_are_left_alone(region):
    conn = await region()
    v2_counters = await counters(conn, "v2")

    await restock(conn, "v1", 1)

    assert await counters(conn, "v2") == v2_counters
    assert await marks(conn, "v2") == {"p_cake": "stock"}


async def test_restock_twice_is_safe(region):
    conn = await region()

    first = await restock(conn, "v1", 1)
    state = await everything(conn)
    second = await restock(conn, "v1", 1)

    assert numbers(first) == (2, 3)
    assert numbers(second) == (0, 0)
    assert await everything(conn) == state


async def test_a_vendor_with_no_counters_and_no_marks_restocks_nothing(region):
    conn = await region()
    state = await everything(conn)

    result = await restock(conn, "v3", 1)

    assert numbers(result) == (0, 0)
    assert await everything(conn) == state


async def test_restock_writes_no_outbox_event(region):
    conn = await region()

    await restock(conn, "v1", 1)

    assert await fetch_all(conn, "SELECT * FROM outbox") == []


# --- with place_order ---------------------------------------------------------


async def test_a_product_an_order_emptied_is_back_on_after_a_restock(region):
    conn = await region()
    await restock(conn, "v1", 1)  # clear the seed's marks first

    emptied = await place_order(conn, order("p_cake_one", 30, "o1"), 1)
    assert emptied.outcome == OrderOutcome.ACCEPTED
    assert (await on_hand(conn))["d_cake"] == 0
    assert (await marks(conn))["p_cake_one"] == "stock"

    result = await restock(conn, "v1", 1)

    assert numbers(result) == (1, 1)
    assert (await on_hand(conn))["d_cake"] == 30
    assert "p_cake_one" not in await marks(conn)
    again = await place_order(conn, order("p_cake_one", 1, "o2"), 1)
    assert again.outcome == OrderOutcome.ACCEPTED
    # The manual switch still holds p_cake off, whatever the stock.
    manual = await place_order(conn, order("p_cake", 1, "o3"), 1)
    assert manual.refusal_reason == RefusalReason.UNAVAILABLE


# --- rejected before writing --------------------------------------------------


async def test_stale_epoch_is_rejected_and_changes_nothing(region):
    conn = await region()
    state = await everything(conn)

    with pytest.raises(StaleEpochError):
        async with translate_db_errors():  # what run_on does for real callers
            await restock(conn, "v1", 2)

    assert await everything(conn) == state


async def test_unknown_vendor_raises_and_changes_nothing(region):
    conn = await region()
    state = await everything(conn)

    with pytest.raises(UnknownVendorError):
        await restock(conn, "v_nobody", 1)

    assert await everything(conn) == state


async def test_restock_with_items_is_not_built(region):
    conn = await region()
    state = await everything(conn)

    with pytest.raises(NotImplementedError):
        await restock(
            conn, "v1", 1, items=[{"item_kind": "ingredient", "item_id": "rice", "add": 20}]
        )

    assert await everything(conn) == state

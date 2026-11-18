"""set_availability against one PostgreSQL. Needs ROS_TEST_DSN.

The manual sold-out switch (WP1 brief, "set_availability"; architecture
Section 6.1). Off writes a `manual` mark. On removes it, then checks whether one
unit can be made right now; if not, a `stock` mark takes its place.

Each test gets its own throwaway schema with the region schema applied. The
recipe amounts and multipliers are above 1, as the brief asks.
"""

from __future__ import annotations

import asyncio
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
from restaurantos.region import place_order, restock, set_availability

pytestmark = pytest.mark.integration

type Connect = Callable[[], Awaitable[psycopg.AsyncConnection]]

SEED = """
INSERT INTO node_state (region_id) VALUES ('r1');

INSERT INTO ingredients (ingredient_id, source) VALUES ('rice', 'test'), ('beef', 'test');

INSERT INTO dishes (dish_id, dish_name, scope, stock_mode) VALUES
    ('d_rice', 'beef rice', 'global', 'per_ingredient'),
    ('d_cake', 'cake', 'global', 'per_item'),
    ('d_pie', 'pie', 'global', 'per_item'),
    ('d_tea', 'tea', 'global', 'none');

INSERT INTO dish_ingredients (dish_id, ingredient_id, amount, unit) VALUES
    ('d_rice', 'rice', 250, 'gram'),
    ('d_rice', 'beef', 2, 'kg');

INSERT INTO vendors (vendor_id, geohash) VALUES ('v1', 'w21z7'), ('v2', 'w21z7');

INSERT INTO products (vendor_id, product_id, name, unit_price, price_units) VALUES
    ('v1', 'p_rice', 'Beef rice, double', 0.012, 30),
    ('v1', 'p_rice_triple', 'Beef rice, triple', 0.018, 45),
    ('v1', 'p_cake', 'Cake, box of 3', 0.0048, 12),
    ('v1', 'p_cake_one', 'Cake', 0.0016, 4),
    ('v1', 'p_pie', 'Pie', 0.002, 5),
    ('v1', 'p_tea', 'Tea', 0.0008, 2),
    ('v1', 'p_nodish', 'No dish link', 0.0004, 1),
    ('v2', 'p_other', 'Sold by v2 only', 0.0004, 1);

INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier) VALUES
    ('v1', 'p_rice', 'd_rice', 2),
    ('v1', 'p_rice_triple', 'd_rice', 3),
    ('v1', 'p_cake', 'd_cake', 3),
    ('v1', 'p_cake_one', 'd_cake', 1),
    ('v1', 'p_pie', 'd_pie', 1),
    ('v1', 'p_tea', 'd_tea', 1),
    ('v2', 'p_other', 'd_tea', 1);

-- One unit of p_rice needs 500 rice and 4 beef: exactly what is there.
-- p_rice_triple needs 750 rice and 6 beef: not enough beef.
-- p_cake needs 3 cakes, p_cake_one needs 1: there are 2.
-- d_pie has no counter at all, which counts as zero.
INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level) VALUES
    ('v1', 'ingredient', 'rice', 1000, 2000),
    ('v1', 'ingredient', 'beef', 4, 100),
    ('v1', 'dish', 'd_cake', 2, 30);
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
        # autocommit: set_availability's own transaction() is then a real
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


async def fetch_all(conn: psycopg.AsyncConnection, query: str, *params: Any) -> list[tuple]:
    cur = await conn.execute(query, params)
    return await cur.fetchall()


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
    """Every row set_availability could touch, for the tests that expect no change."""
    return [
        await fetch_all(conn, "SELECT * FROM stock ORDER BY vendor_id, item_kind, item_id"),
        await fetch_all(conn, "SELECT * FROM product_availability ORDER BY vendor_id, product_id"),
        await fetch_all(conn, "SELECT * FROM outbox ORDER BY seq"),
    ]


async def switch(
    conn: psycopg.AsyncConnection,
    product: str,
    available: bool,
    *,
    vendor: str = "v1",
    epoch: int = 1,
) -> tuple[bool, str | None]:
    state = await set_availability(conn, vendor, product, available, epoch)
    return state.available, state.reason


def order(product: str, quantity: int, ext: str) -> OrderRequest:
    return OrderRequest(
        channel="test",
        external_order_id=ext,
        vendor_id="v1",
        order_day=14,
        order_time=time(12, 3, 29),
        items=[OrderItemIn(product_id=product, quantity=quantity)],
    )


ON = (True, None)
MANUAL = (False, "manual")
STOCK = (False, "stock")


# --- switching off ------------------------------------------------------------


async def test_off_writes_a_manual_mark(region):
    conn = await region()

    assert await switch(conn, "p_tea", False) == MANUAL
    assert await marks(conn) == {"p_tea": "manual"}


async def test_off_twice_is_harmless(region):
    conn = await region()

    await switch(conn, "p_tea", False)
    assert await switch(conn, "p_tea", False) == MANUAL
    assert await marks(conn) == {"p_tea": "manual"}


async def test_off_turns_a_stock_mark_into_a_manual_one_that_a_restock_leaves(region):
    conn = await region()
    await conn.execute(
        "INSERT INTO product_availability VALUES"
        " ('v1', 'p_cake', false, 'stock', clock_timestamp())"
    )

    assert await switch(conn, "p_cake", False) == MANUAL

    await restock(conn, "v1", 1)
    assert await marks(conn) == {"p_cake": "manual"}


async def test_an_order_for_a_product_switched_off_is_refused(region):
    conn = await region()
    await switch(conn, "p_tea", False)

    result = await place_order(conn, order("p_tea", 1, "o1"), 1)

    assert result.refusal_reason == RefusalReason.UNAVAILABLE


# --- switching on -------------------------------------------------------------


async def test_on_with_nothing_to_remove_is_available(region):
    conn = await region()

    assert await switch(conn, "p_tea", True) == ON
    assert await marks(conn) == {}


async def test_on_removes_the_manual_mark_when_one_unit_can_be_made(region):
    conn = await region()
    await switch(conn, "p_cake_one", False)

    assert await switch(conn, "p_cake_one", True) == ON
    assert await marks(conn) == {}


@pytest.mark.parametrize(
    ("product", "expected"),
    [
        pytest.param("p_rice", ON, id="per_ingredient, exactly amount x multiplier"),
        pytest.param("p_rice_triple", STOCK, id="per_ingredient, one recipe row short"),
        pytest.param("p_cake_one", ON, id="per_item, enough for the multiplier"),
        pytest.param("p_cake", STOCK, id="per_item, less than the multiplier"),
        pytest.param("p_pie", STOCK, id="per_item, no counter counts as zero"),
        pytest.param("p_tea", ON, id="none, always"),
    ],
)
async def test_on_checks_whether_one_unit_can_be_made(region, product, expected):
    conn = await region()
    await switch(conn, product, False)

    assert await switch(conn, product, True) == expected
    assert await marks(conn) == ({} if expected == ON else {product: "stock"})


async def test_on_twice_is_harmless(region):
    conn = await region()
    await switch(conn, "p_cake", False)

    assert await switch(conn, "p_cake", True) == STOCK
    assert await switch(conn, "p_cake", True) == STOCK
    assert await marks(conn) == {"p_cake": "stock"}


async def test_on_leaves_a_stock_mark_alone(region):
    conn = await region()
    # A stale stock mark: one cake can be made, but only a restock clears it.
    await conn.execute(
        "INSERT INTO product_availability VALUES"
        " ('v1', 'p_cake_one', false, 'stock', clock_timestamp())"
    )

    assert await switch(conn, "p_cake_one", True) == STOCK
    assert await marks(conn) == {"p_cake_one": "stock"}


async def test_a_product_that_ran_out_while_off_gets_a_stock_mark_when_switched_on(region):
    # The case the brief names: an order empties the counter while the product
    # is switched off, so the order's stock mark is kept out by the manual row.
    conn = await region()
    await switch(conn, "p_cake", False)
    emptied = await place_order(conn, order("p_cake_one", 2, "o1"), 1)
    assert emptied.outcome == OrderOutcome.ACCEPTED
    assert (await marks(conn))["p_cake"] == "manual"

    assert await switch(conn, "p_cake", True) == STOCK

    await restock(conn, "v1", 1)
    assert await marks(conn) == {}


# --- concurrency --------------------------------------------------------------


@pytest.mark.parametrize("attempt", range(5))
async def test_switch_on_racing_an_order_that_empties_a_shared_counter(region, attempt):
    # p_cake (needs 3) is switched off; 3 cakes are left. At the same moment an
    # order for p_cake_one takes all 3 and p_cake is switched back on.
    # Whichever commits first, p_cake ends with a stock mark: either the switch
    # sees the empty counter, or the order marks it once the manual row is
    # gone. Locks are taken counters first on both sides; taken the other way
    # round, PostgreSQL detects a deadlock and one side raises DeadlockDetected.
    # (No asyncio timeout here: AGENTS.md forbids cancelling a psycopg call.)
    conn_a, conn_b, check = await region(), await region(), await region()
    await check.execute("UPDATE stock SET on_hand = 3 WHERE item_id = 'd_cake'")
    await switch(check, "p_cake", False)

    placed, state = await asyncio.gather(
        place_order(conn_a, order("p_cake_one", 3, f"race{attempt}"), 1),
        set_availability(conn_b, "v1", "p_cake", True, 1),
    )

    assert placed.outcome == OrderOutcome.ACCEPTED
    assert await fetch_all(check, "SELECT on_hand FROM stock WHERE item_id = 'd_cake'") == [(0,)]
    assert (await marks(check))["p_cake"] == "stock"
    # The switch saw either 3 cakes (on) or none (stock), never a deadlock.
    assert (state.available, state.reason) in (ON, STOCK)


# --- rejected before writing --------------------------------------------------


@pytest.mark.parametrize(
    ("vendor", "product"),
    [
        pytest.param("v1", "p_missing", id="no such product"),
        pytest.param("v1", "p_other", id="another vendor's product"),
        pytest.param("v1", "p_nodish", id="product without a dish link"),
        pytest.param("v_nobody", "p_tea", id="no such vendor"),
    ],
)
async def test_unknown_product_raises_and_changes_nothing(region, vendor, product):
    conn = await region()
    state = await everything(conn)

    for available in (False, True):
        with pytest.raises(UnknownVendorError):
            await set_availability(conn, vendor, product, available, 1)

    assert await everything(conn) == state


async def test_stale_epoch_is_rejected_and_changes_nothing(region):
    conn = await region()
    state = await everything(conn)

    with pytest.raises(StaleEpochError):
        async with translate_db_errors():  # what run_on does for real callers
            await set_availability(conn, "v1", "p_tea", False, 2)

    assert await everything(conn) == state


async def test_the_switch_writes_no_outbox_event(region):
    conn = await region()

    await switch(conn, "p_tea", False)
    await switch(conn, "p_tea", True)

    assert await fetch_all(conn, "SELECT * FROM outbox") == []

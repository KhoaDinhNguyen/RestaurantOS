"""place_order against one PostgreSQL. Needs ROS_TEST_DSN.

Each test gets its own throwaway schema with the region schema applied and a
small menu whose amounts and multipliers are above 1, because the dataset's
amount of 1 cannot catch a rule that forgets to multiply.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, time
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from restaurantos.common.db import translate_db_errors
from restaurantos.common.errors import StaleEpochError, UnknownVendorError
from restaurantos.common.models import (
    OrderItemIn,
    OrderOutcome,
    OrderRequest,
    OrderStatus,
    RefusalReason,
)
from restaurantos.common.sqlfiles import schema_files
from restaurantos.region import place_order

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
    ('v1', 'p_rice_small', 'Beef rice', 0.006, 15),
    ('v1', 'p_cake', 'Cake, box of 3', 0.0048, 12),
    ('v1', 'p_pie', 'Pie', 0.002, 5),
    ('v1', 'p_tea', 'Tea', 0.0008, 2),
    ('v1', 'p_nodish', 'No dish link', 0.0004, 1),
    ('v2', 'p_other', 'Sold by v2 only', 0.0004, 1);

INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier) VALUES
    ('v1', 'p_rice', 'd_rice', 2),
    ('v1', 'p_rice_small', 'd_rice', 1),
    ('v1', 'p_cake', 'd_cake', 3),
    ('v1', 'p_pie', 'd_pie', 1),
    ('v1', 'p_tea', 'd_tea', 1),
    ('v2', 'p_other', 'd_tea', 1);

-- d_pie has no stock row on purpose: a missing counter counts as zero.
INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level) VALUES
    ('v1', 'ingredient', 'rice', 1500, 2000),
    ('v1', 'ingredient', 'beef', 100, 100),
    ('v1', 'dish', 'd_cake', 7, 10);
"""

RECEIVED_AT = datetime(2026, 11, 20, 18, 3, 11, tzinfo=UTC)


@pytest.fixture
async def region() -> AsyncIterator[Connect]:
    """A fresh seeded region schema. Yields a function that opens connections to it."""
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    schema = f"ros_test_wp1_{uuid4().hex[:8]}"
    opened: list[psycopg.AsyncConnection] = []

    async def connect() -> psycopg.AsyncConnection:
        # autocommit: place_order's own transaction() is then a real BEGIN and
        # COMMIT, and the check queries below never leave a transaction open.
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


def make_order(*items: tuple[str, int], ext: str = "o1", vendor: str = "v1") -> OrderRequest:
    return OrderRequest(
        channel="test",
        external_order_id=ext,
        vendor_id=vendor,
        customer_id="c1",
        customer_geohash="w21z7",
        order_day=14,
        order_time=time(12, 3, 29),
        items=[OrderItemIn(product_id=p, quantity=q) for p, q in items],
    )


async def scalar(conn: psycopg.AsyncConnection, query: Any, *params: Any) -> Any:
    cur = await conn.execute(query, params)
    row = await cur.fetchone()
    assert row is not None
    return row[0]


async def count(conn: psycopg.AsyncConnection, table: str) -> int:
    return await scalar(conn, sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))


async def on_hand(conn: psycopg.AsyncConnection, kind: str, item_id: str) -> int:
    return await scalar(
        conn,
        "SELECT on_hand FROM stock WHERE vendor_id = 'v1' AND item_kind = %s AND item_id = %s",
        kind,
        item_id,
    )


async def marks(conn: psycopg.AsyncConnection) -> dict[str, str]:
    cur = await conn.execute("SELECT product_id, reason FROM product_availability")
    return {product_id: reason for product_id, reason in await cur.fetchall()}


# --- accepted ----------------------------------------------------------------


async def test_amount_250_x_multiplier_2_x_quantity_3_takes_1500(region):
    conn = await region()

    result = await place_order(conn, make_order(("p_rice", 3)), 1, received_at=RECEIVED_AT)

    assert result.outcome == OrderOutcome.ACCEPTED
    assert result.status == OrderStatus.RECEIVED
    assert not result.duplicate
    assert await on_hand(conn, "ingredient", "rice") == 1500 - 250 * 2 * 3
    assert await on_hand(conn, "ingredient", "beef") == 100 - 2 * 2 * 3


async def test_accepted_order_writes_order_items_and_one_event(region):
    conn = await region()

    result = await place_order(conn, make_order(("p_rice", 1)), 1, received_at=RECEIVED_AT)

    cur = await conn.execute(
        "SELECT status, epoch, menu_version, received_at, order_day FROM orders"
        " WHERE order_id = %s",
        (result.order_id,),
    )
    assert await cur.fetchone() == ("received", 1, 1, RECEIVED_AT, 14)

    cur = await conn.execute("SELECT line_no, product_id, quantity, price_units FROM order_items")
    assert await cur.fetchall() == [(1, "p_rice", 1, 30)]

    cur = await conn.execute("SELECT event_type, order_day, payload FROM outbox")
    events = await cur.fetchall()
    assert len(events) == 1
    event_type, order_day, payload = events[0]
    assert (event_type, order_day) == ("order.created", 14)
    assert payload["order_id"] == str(result.order_id)
    assert payload["received_at"] is not None
    assert payload["items"] == [
        {"line_no": 1, "product_id": "p_rice", "quantity": 1, "price_units": 30}
    ]


async def test_stock_mode_none_takes_no_stock(region):
    conn = await region()

    result = await place_order(conn, make_order(("p_tea", 5)), 1)

    assert result.outcome == OrderOutcome.ACCEPTED
    assert await scalar(conn, "SELECT sum(on_hand) FROM stock") == 1500 + 100 + 7


# --- idempotency --------------------------------------------------------------


async def test_same_order_twice_has_one_effect(region):
    conn = await region()

    first = await place_order(conn, make_order(("p_rice_small", 1)), 1)
    second = await place_order(conn, make_order(("p_rice_small", 1)), 1)

    assert second.duplicate
    assert second.outcome == OrderOutcome.ACCEPTED
    assert second.order_id == first.order_id
    assert second.committed_at == first.committed_at
    assert await count(conn, "orders") == 1
    assert await count(conn, "outbox") == 1
    assert await on_hand(conn, "ingredient", "rice") == 1500 - 250


async def test_refused_order_sent_again_is_refused_again(region):
    conn = await region()
    # 3 boxes of 3 cakes is 9; the counter holds 7.

    first = await place_order(conn, make_order(("p_cake", 3)), 1)
    second = await place_order(conn, make_order(("p_cake", 3)), 1)

    assert first.refusal_reason == RefusalReason.OUT_OF_STOCK
    assert not first.duplicate
    assert second.outcome == OrderOutcome.REFUSED
    assert second.refusal_reason == RefusalReason.OUT_OF_STOCK
    assert second.duplicate
    assert await on_hand(conn, "dish", "d_cake") == 7
    assert await count(conn, "orders") == 0
    assert await count(conn, "outbox") == 0
    cur = await conn.execute("SELECT outcome, order_id, refusal_reason, order_day FROM inbox")
    assert await cur.fetchall() == [("refused", None, "out_of_stock", 14)]


# --- concurrency ----------------------------------------------------------------


async def test_two_orders_racing_for_the_last_stock_exactly_one_wins(region):
    a, b = await region(), await region()
    # Rice holds 1500: exactly one order of p_rice x 3 (250 x 2 x 3).

    results = await asyncio.gather(
        place_order(a, make_order(("p_rice", 3), ext="race-a"), 1),
        place_order(b, make_order(("p_rice", 3), ext="race-b"), 1),
    )

    outcomes = sorted(r.outcome for r in results)
    assert outcomes == [OrderOutcome.ACCEPTED, OrderOutcome.REFUSED]
    refused = next(r for r in results if r.outcome == OrderOutcome.REFUSED)
    # The loser waits on the stock lock (out_of_stock) if it read the products
    # before the winner committed, or finds p_rice marked (unavailable) if after.
    assert refused.refusal_reason in {RefusalReason.OUT_OF_STOCK, RefusalReason.UNAVAILABLE}
    assert await on_hand(a, "ingredient", "rice") == 0
    assert await count(a, "orders") == 1


# --- refusals -------------------------------------------------------------------


async def test_one_short_line_refuses_the_whole_order(region):
    conn = await region()

    result = await place_order(conn, make_order(("p_rice_small", 1), ("p_cake", 3)), 1)

    assert result.refusal_reason == RefusalReason.OUT_OF_STOCK
    assert await on_hand(conn, "ingredient", "rice") == 1500  # the good line took nothing


async def test_missing_stock_row_counts_as_zero(region):
    conn = await region()

    result = await place_order(conn, make_order(("p_pie", 1)), 1)

    assert result.refusal_reason == RefusalReason.OUT_OF_STOCK


@pytest.mark.parametrize(
    "product_id",
    [
        "p_other",  # another vendor sells it, this one does not
        "p_nodish",  # no product_dish row
        "p_missing",  # nobody sells it
    ],
)
async def test_unknown_products_are_refused(region, product_id):
    conn = await region()

    result = await place_order(conn, make_order((product_id, 1)), 1)

    assert result.refusal_reason == RefusalReason.UNKNOWN_PRODUCT
    assert await count(conn, "inbox") == 1


async def test_switched_off_product_is_unavailable(region):
    conn = await region()
    await conn.execute("INSERT INTO product_availability VALUES ('v1', 'p_tea', false, 'manual')")

    result = await place_order(conn, make_order(("p_tea", 1)), 1)

    assert result.refusal_reason == RefusalReason.UNAVAILABLE


async def test_unknown_vendor_raises_and_keeps_no_inbox_row(region):
    conn = await region()

    with pytest.raises(UnknownVendorError):
        await place_order(conn, make_order(("p_tea", 1), vendor="v9"), 1)

    assert await count(conn, "inbox") == 0


async def test_stale_epoch_is_rejected_and_changes_nothing(region):
    conn = await region()

    with pytest.raises(StaleEpochError):
        async with translate_db_errors():  # what run_on does for real callers
            await place_order(conn, make_order(("p_rice_small", 1)), 2)

    assert await count(conn, "inbox") == 0
    assert await count(conn, "orders") == 0
    assert await on_hand(conn, "ingredient", "rice") == 1500


# --- availability marks -----------------------------------------------------------


async def test_counter_reaching_zero_marks_every_product_using_it(region):
    conn = await region()

    await place_order(conn, make_order(("p_rice", 3)), 1)  # rice 1500 -> 0

    assert await marks(conn) == {"p_rice": "stock", "p_rice_small": "stock"}


async def test_counter_below_one_unit_marks_the_product(region):
    conn = await region()

    await place_order(conn, make_order(("p_cake", 2)), 1)  # d_cake 7 -> 1; a box needs 3

    assert await marks(conn) == {"p_cake": "stock"}

    result = await place_order(conn, make_order(("p_cake", 1), ext="o2"), 1)

    assert result.refusal_reason == RefusalReason.UNAVAILABLE
    assert await on_hand(conn, "dish", "d_cake") == 1


async def test_only_products_that_cannot_make_one_are_marked(region):
    conn = await region()

    # rice 1500 -> 250, beef 100 -> 90. p_rice needs 500 rice, p_rice_small 250.
    await place_order(conn, make_order(("p_rice_small", 5)), 1)

    assert await marks(conn) == {"p_rice": "stock"}

    result = await place_order(conn, make_order(("p_rice_small", 1), ext="o2"), 1)

    assert result.outcome == OrderOutcome.ACCEPTED
    assert await on_hand(conn, "ingredient", "rice") == 0
    assert await marks(conn) == {"p_rice": "stock", "p_rice_small": "stock"}


async def test_a_manual_mark_survives_running_out(region):
    conn = await region()
    await conn.execute("INSERT INTO product_availability VALUES ('v1', 'p_rice', false, 'manual')")

    result = await place_order(conn, make_order(("p_rice_small", 6)), 1)  # 250 x 1 x 6 = 1500

    assert result.outcome == OrderOutcome.ACCEPTED
    assert await marks(conn) == {"p_rice": "manual", "p_rice_small": "stock"}


async def test_foreign_vendor_cannot_reuse_local_inbox_key(region):
    conn = await region()

    local = make_order(
        ("p_rice_small", 1),
        ext="shared-key",
    )
    first = await place_order(conn, local, 1)

    foreign = make_order(
        ("p_tea", 1),
        ext="shared-key",
        vendor="v9",
    )

    with pytest.raises(UnknownVendorError):
        await place_order(conn, foreign, 1)

    assert await count(conn, "inbox") == 1
    assert await count(conn, "orders") == 1
    assert await count(conn, "outbox") == 1

    duplicate = await place_order(conn, local, 1)

    assert duplicate.duplicate
    assert duplicate.order_id == first.order_id

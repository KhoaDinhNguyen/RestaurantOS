"""change_status against one PostgreSQL. Needs ROS_TEST_DSN.

Each test gets its own throwaway schema with the region schema applied and
one vendor's menu. Orders are made with place_order, the real way in.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import time
from typing import Any
from uuid import UUID, uuid4

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
    StatusResult,
)
from restaurantos.common.sqlfiles import schema_files
from restaurantos.region import change_status, place_order

pytestmark = pytest.mark.integration

type Connect = Callable[[], Awaitable[psycopg.AsyncConnection]]

S = OrderStatus

SEED = """
INSERT INTO node_state (region_id) VALUES ('r1');

INSERT INTO dishes (dish_id, dish_name, scope, stock_mode) VALUES
    ('d_cake', 'cake', 'global', 'per_item');

INSERT INTO vendors (vendor_id, geohash) VALUES ('v1', 'w21z7'), ('v2', 'w21z7');

INSERT INTO products (vendor_id, product_id, name, unit_price, price_units) VALUES
    ('v1', 'p_cake', 'Cake, box of 2', 0.0048, 12);

INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier) VALUES
    ('v1', 'p_cake', 'd_cake', 2);

INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level) VALUES
    ('v1', 'dish', 'd_cake', 10, 10);
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
        # autocommit: change_status's own transaction() is then a real BEGIN
        # and COMMIT, and the check queries never leave a transaction open.
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


async def new_order(conn: psycopg.AsyncConnection, ext: str = "o1") -> UUID:
    """One accepted order of one box of 2 cakes; it takes 2 off the counter."""
    result = await place_order(
        conn,
        OrderRequest(
            channel="test",
            external_order_id=ext,
            vendor_id="v1",
            order_day=14,
            order_time=time(12, 3, 29),
            items=[OrderItemIn(product_id="p_cake", quantity=1)],
        ),
        1,
    )
    assert result.outcome == OrderOutcome.ACCEPTED
    assert result.order_id is not None
    return result.order_id


async def move(
    conn: psycopg.AsyncConnection,
    order_id: UUID,
    to_status: OrderStatus,
    *,
    event_id: UUID | None = None,
    vendor: str = "v1",
    epoch: int = 1,
) -> StatusResult:
    return await change_status(conn, vendor, order_id, event_id or uuid4(), to_status, epoch)


async def walk(conn: psycopg.AsyncConnection, order_id: UUID, *steps: OrderStatus) -> None:
    for step in steps:
        assert (await move(conn, order_id, step)).applied


async def fetch_all(conn: psycopg.AsyncConnection, query: str, *params: Any) -> list[tuple]:
    cur = await conn.execute(query, params)
    return await cur.fetchall()


async def stored(conn: psycopg.AsyncConnection, order_id: UUID) -> tuple[str, Any]:
    rows = await fetch_all(
        conn, "SELECT status, status_changed_at FROM orders WHERE order_id = %s", order_id
    )
    return rows[0]


async def events(conn: psycopg.AsyncConnection, event_type: str) -> list[dict]:
    rows = await fetch_all(
        conn, "SELECT payload FROM outbox WHERE event_type = %s ORDER BY seq", event_type
    )
    return [payload for (payload,) in rows]


async def count(conn: psycopg.AsyncConnection, table: str) -> int:
    cur = await conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))
    row = await cur.fetchone()
    assert row is not None
    return row[0]


# --- allowed moves ---------------------------------------------------------------


async def test_allowed_move_updates_the_order_and_writes_one_event(region):
    conn = await region()
    order_id = await new_order(conn)
    _, received_at = await stored(conn, order_id)

    result = await move(conn, order_id, S.ACCEPTED)

    assert result == StatusResult(order_id=order_id, status=S.ACCEPTED, applied=True)
    status, changed_at = await stored(conn, order_id)
    assert status == "accepted"
    assert changed_at > received_at
    (payload,) = await events(conn, "order.status_changed")
    assert payload["order_id"] == str(order_id)
    assert payload["order_day"] == 14
    assert (payload["from_status"], payload["to_status"]) == ("received", "accepted")
    assert payload["changed_at"] is not None
    assert await count(conn, "exception_log") == 0


async def test_the_whole_way_from_received_to_picked_up(region):
    conn = await region()
    order_id = await new_order(conn)

    await walk(conn, order_id, S.ACCEPTED, S.PREPARING, S.READY, S.PICKED_UP)

    assert (await stored(conn, order_id))[0] == "picked_up"
    moves = [(p["from_status"], p["to_status"]) for p in await events(conn, "order.status_changed")]
    assert moves == [
        ("received", "accepted"),
        ("accepted", "preparing"),
        ("preparing", "ready"),
        ("ready", "picked_up"),
    ]


@pytest.mark.parametrize(
    "before",
    [
        (),
        (S.ACCEPTED,),
        (S.ACCEPTED, S.PREPARING),
        (S.ACCEPTED, S.PREPARING, S.READY),
    ],
    ids=["received", "accepted", "preparing", "ready"],
)
async def test_cancel_is_allowed_from_every_state_before_picked_up(region, before):
    conn = await region()
    order_id = await new_order(conn)
    await walk(conn, order_id, *before)

    result = await move(conn, order_id, S.CANCELED)

    assert result.applied
    assert (await stored(conn, order_id))[0] == "canceled"


async def test_a_cancel_does_not_give_stock_back(region):
    conn = await region()
    order_id = await new_order(conn)  # 10 - 1 box x 2 = 8

    await move(conn, order_id, S.CANCELED)

    rows = await fetch_all(conn, "SELECT on_hand FROM stock WHERE item_id = 'd_cake'")
    assert rows == [(8,)]


# --- conflicts -------------------------------------------------------------------


async def test_conflicting_change_lands_in_exception_log_and_first_change_stands(region):
    conn = await region()
    order_id = await new_order(conn)
    await walk(conn, order_id, S.ACCEPTED, S.PREPARING, S.READY, S.PICKED_UP)
    status_before, changed_before = await stored(conn, order_id)
    late = uuid4()

    result = await move(conn, order_id, S.READY, event_id=late)

    assert result == StatusResult(
        order_id=order_id, status=S.PICKED_UP, applied=False, conflict=True
    )
    assert await stored(conn, order_id) == (status_before, changed_before)
    rows = await fetch_all(
        conn,
        "SELECT event_id, order_day, order_id, kind, current_status, current_changed_at,"
        " attempted_status, attempted_at FROM exception_log",
    )
    assert len(rows) == 1
    event_id, order_day, logged_order, kind, current, current_at, attempted, attempted_at = rows[0]
    assert (event_id, order_day, logged_order, kind) == (late, 14, order_id, "status_conflict")
    assert (current, current_at) == ("picked_up", changed_before)
    assert attempted == "ready"
    assert attempted_at >= changed_before
    (payload,) = await events(conn, "exception.logged")
    assert payload["order_id"] == str(order_id)
    assert payload["current_status"] == "picked_up"
    assert payload["attempted_status"] == "ready"
    assert payload["kind"] == "status_conflict"
    assert len(await events(conn, "order.status_changed")) == 4  # none for the loser


@pytest.mark.parametrize(
    ("before", "attempt"),
    [
        ((), S.PREPARING),  # skips accepted
        ((), S.PICKED_UP),  # skips three steps
        ((), S.RECEIVED),  # to the state it is in
        ((S.ACCEPTED,), S.RECEIVED),  # backwards
        ((S.ACCEPTED,), S.ACCEPTED),  # the same move again, as a new event
        ((S.CANCELED,), S.ACCEPTED),  # nothing leaves canceled
        ((S.ACCEPTED, S.PREPARING, S.READY, S.PICKED_UP), S.CANCELED),  # too late to cancel
    ],
)
async def test_moves_outside_the_state_machine_are_conflicts(region, before, attempt):
    conn = await region()
    order_id = await new_order(conn)
    await walk(conn, order_id, *before)
    status_before = (await stored(conn, order_id))[0]

    result = await move(conn, order_id, attempt)

    assert result.conflict
    assert not result.applied
    assert result.status == status_before
    assert (await stored(conn, order_id))[0] == status_before
    assert await count(conn, "exception_log") == 1


async def test_two_changes_racing_the_first_to_commit_wins(region):
    a, b = await region(), await region()
    order_id = await new_order(a)
    await walk(a, order_id, S.ACCEPTED, S.PREPARING, S.READY)

    # From ready both are allowed; whichever commits first makes the other one
    # impossible (canceled -> picked_up and picked_up -> canceled are not moves).
    results = await asyncio.gather(
        move(a, order_id, S.PICKED_UP),
        move(b, order_id, S.CANCELED),
    )

    winners = [r for r in results if r.applied]
    losers = [r for r in results if r.conflict]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0].status == winners[0].status
    assert (await stored(a, order_id))[0] == winners[0].status
    assert await count(a, "exception_log") == 1


# --- idempotency -----------------------------------------------------------------


async def test_same_event_twice_is_applied_once(region):
    conn = await region()
    order_id = await new_order(conn)
    event_id = uuid4()

    first = await move(conn, order_id, S.ACCEPTED, event_id=event_id)
    second = await move(conn, order_id, S.ACCEPTED, event_id=event_id)

    assert first.applied
    assert second == StatusResult(
        order_id=order_id, status=S.ACCEPTED, applied=False, duplicate=True
    )
    assert len(await events(conn, "order.status_changed")) == 1
    assert await count(conn, "processed_events") == 1
    assert await count(conn, "exception_log") == 0


async def test_a_repeated_event_answers_with_the_order_as_it_is_now(region):
    conn = await region()
    order_id = await new_order(conn)
    event_id = uuid4()
    await move(conn, order_id, S.ACCEPTED, event_id=event_id)
    await move(conn, order_id, S.PREPARING)

    result = await move(conn, order_id, S.ACCEPTED, event_id=event_id)

    assert result.duplicate
    assert result.status == S.PREPARING
    assert (await stored(conn, order_id))[0] == "preparing"


async def test_a_repeated_conflict_is_logged_once(region):
    conn = await region()
    order_id = await new_order(conn)
    event_id = uuid4()

    first = await move(conn, order_id, S.READY, event_id=event_id)
    second = await move(conn, order_id, S.READY, event_id=event_id)

    assert first.conflict
    assert second.duplicate
    assert not second.conflict
    assert await count(conn, "exception_log") == 1
    assert len(await events(conn, "exception.logged")) == 1


# --- rejected before anything is written ---------------------------------------------


async def test_stale_epoch_is_rejected_and_changes_nothing(region):
    conn = await region()
    order_id = await new_order(conn)

    with pytest.raises(StaleEpochError):
        async with translate_db_errors():  # what run_on does for real callers
            await move(conn, order_id, S.ACCEPTED, epoch=2)

    assert (await stored(conn, order_id))[0] == "received"
    assert await count(conn, "processed_events") == 0
    assert await events(conn, "order.status_changed") == []


@pytest.mark.parametrize(
    ("vendor", "known_order"),
    [
        ("v1", False),  # no such order
        ("v2", True),  # the order exists, but belongs to v1
    ],
)
async def test_unknown_order_raises_and_records_nothing(region, vendor, known_order):
    conn = await region()
    real_order = await new_order(conn)
    order_id = real_order if known_order else uuid4()

    with pytest.raises(UnknownVendorError):
        await move(conn, order_id, S.ACCEPTED, vendor=vendor)

    assert await count(conn, "processed_events") == 0
    assert (await stored(conn, real_order))[0] == "received"

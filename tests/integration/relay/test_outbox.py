"""Exercise the pending batch query against the region schema."""

import os
from datetime import UTC, datetime, time
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from restaurantos.common.config import Settings
from restaurantos.common.db import apply_schema, make_pool
from restaurantos.common.models import (
    EventType,
    ExceptionLoggedPayload,
    OrderCreatedPayload,
    OrderItemRow,
    OutboxEvent,
    StatusChangedPayload,
)
from restaurantos.relay.outbox import read_pending

pytestmark = pytest.mark.integration


@pytest.fixture
async def outbox():
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")
    schema = f"relay_test_{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(f"CREATE SCHEMA {schema}")
        try:
            await conn.execute(f"SET search_path TO {schema}")
            await apply_schema(conn, "region")
            settings = Settings(_env_file=None, pool_min_size=1, pool_max_size=1)
            pool_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
            async with make_pool(pool_dsn, settings) as pool:
                yield conn, pool, settings
        finally:
            await conn.execute(f"DROP SCHEMA {schema} CASCADE")


async def test_pending_batch_keeps_order_and_uses_shared_envelopes(outbox):
    conn, pool, settings = outbox
    order_id = uuid4()
    stamp = datetime(2026, 11, 14, 19, 0, tzinfo=UTC)
    created = OrderCreatedPayload(
        order_id=order_id,
        order_day=14,
        vendor_id="v001",
        channel="mini",
        external_order_id="o0001",
        order_time=time(12),
        status="accepted",
        menu_version=1,
        committed_at=stamp,
        items=[OrderItemRow(line_no=1, product_id="0017", quantity=2, price_units=30)],
    )
    status = StatusChangedPayload(
        order_id=order_id,
        order_day=14,
        from_status="accepted",
        to_status="preparing",
        changed_at=stamp,
    )
    exception = ExceptionLoggedPayload(
        order_id=order_id,
        order_day=14,
        current_status="preparing",
        current_changed_at=stamp,
        attempted_status="accepted",
        attempted_at=stamp,
    )

    # Insert out of sequence, including a lower sequence already confirmed at HQ.
    for seq, event_type, payload, relayed_at in [
        (4, EventType.EXCEPTION_LOGGED, exception, None),
        (2, EventType.ORDER_CREATED, created, None),
        (1, EventType.ORDER_CREATED, created, stamp),
        (3, EventType.ORDER_STATUS_CHANGED, status, None),
    ]:
        await conn.execute(
            "INSERT INTO outbox (seq, event_type, order_day, payload, relayed_at)"
            " OVERRIDING SYSTEM VALUE VALUES (%s, %s, 14, %s, %s)",
            (seq, event_type, Jsonb(payload.model_dump(mode="json")), relayed_at),
        )

    batch = await read_pending(pool, "r1", 2, settings=settings)
    assert [event.seq for event in batch] == [2, 3]

    events = await read_pending(pool, "r1", 500, settings=settings)
    assert [event.seq for event in events] == [2, 3, 4]
    assert [event.region_id for event in events] == ["r1", "r1", "r1"]
    assert [event.order_day for event in events] == [14, 14, 14]
    assert [
        OutboxEvent.model_validate_json(event.model_dump_json()).parsed_payload()
        for event in events
    ] == [created, status, exception]

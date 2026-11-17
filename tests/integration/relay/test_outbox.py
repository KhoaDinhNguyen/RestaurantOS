"""Exercise the pending batch query against the region schema."""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, time
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from restaurantos.common.cluster import HQ_PRIMARY, Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import apply_schema, make_pool
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.locks import ANALYTICS_EXCEPTIONS_LOCK_KEY
from restaurantos.common.models import (
    EventType,
    ExceptionLoggedPayload,
    OrderCreatedPayload,
    OrderItemRow,
    OutboxEvent,
    StatusChangedPayload,
)
from restaurantos.relay import worker
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.hq_apply import apply_batch
from restaurantos.relay.outbox import read_pending

pytestmark = pytest.mark.integration


@asynccontextmanager
async def relay_schema(kind):
    dsn = os.environ.get("ROS_TEST_DSN")
    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")
    schema = f"relay_test_{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(f"CREATE SCHEMA {schema}")
        try:
            await conn.execute(f"SET search_path TO {schema}")
            await apply_schema(conn, kind)
            settings = Settings(_env_file=None, pool_min_size=1, pool_max_size=1)
            pool_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
            async with make_pool(pool_dsn, settings) as pool:
                yield conn, pool, settings
        finally:
            await conn.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
async def outbox():
    async with relay_schema("region") as db:
        yield db


@pytest.fixture
async def hq():
    async with relay_schema("hq") as db:
        yield db


@pytest.fixture
def payloads():
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

    return created, status, exception


async def seed_outbox(conn, rows):
    for seq, event_type, payload, relayed_at in rows:
        await conn.execute(
            "INSERT INTO outbox (seq, event_type, order_day, payload, relayed_at)"
            " OVERRIDING SYSTEM VALUE VALUES (%s, %s, 14, %s, %s)",
            (seq, event_type, Jsonb(payload.model_dump(mode="json")), relayed_at),
        )


async def test_pending_batch_keeps_order_and_uses_shared_envelopes(outbox, payloads):
    conn, pool, settings = outbox
    created, status, exception = payloads

    # Insert out of sequence, including a lower sequence already confirmed at HQ.
    await seed_outbox(
        conn,
        [
            (4, EventType.EXCEPTION_LOGGED, exception, None),
            (2, EventType.ORDER_CREATED, created, None),
            (1, EventType.ORDER_CREATED, created, created.committed_at),
            (3, EventType.ORDER_STATUS_CHANGED, status, None),
        ],
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


async def test_retries_after_hq_commit_without_duplicating_events(
    outbox, hq, payloads, monkeypatch
):
    region, region_pool, settings = outbox
    headquarters, hq_pool, _ = hq
    created, status, exception = payloads
    await region.execute("INSERT INTO node_state (region_id, epoch) VALUES ('r1', 7)")
    await seed_outbox(
        region,
        [
            (3, EventType.EXCEPTION_LOGGED, exception, None),
            (1, EventType.ORDER_CREATED, created, None),
            (2, EventType.ORDER_STATUS_CHANGED, status, None),
        ],
    )

    async def find_primary(*args, **kwargs):
        return "r1-primary"

    monkeypatch.setattr(worker, "find_local_primary", find_primary)
    monkeypatch.setattr(
        worker,
        "node_conninfo",
        lambda cluster, node, **kwargs: (
            hq_pool.conninfo if node == HQ_PRIMARY else region_pool.conninfo
        ),
    )

    class RelayStopped(Exception):
        pass

    real_mark = worker.mark_relayed
    attempts = []
    inbox_before_retry = []

    async def interrupt_first_ack(pool, region_id, epoch, event_ids, *, settings):
        attempts.append((epoch, event_ids))
        if len(attempts) == 1:
            inbox = await headquarters.execute(
                "SELECT event_id, applied_at FROM hq_inbox ORDER BY seq"
            )
            inbox_before_retry.extend(await inbox.fetchall())
            pending = await read_pending(pool, region_id, 500, settings=settings)
            assert [event.event_id for event in pending] == event_ids
            raise RegionUnreachableError("simulated disconnect before regional ack")
        await real_mark(pool, region_id, epoch, event_ids, settings=settings)
        raise RelayStopped

    monkeypatch.setattr(worker, "mark_relayed", interrupt_first_ack)
    relay = RelaySettings(_env_file=None, role="region", poll_interval_s=0.001)
    with pytest.raises(RelayStopped):
        await worker.run_region_relay(Cluster(standbys=False), "r1", settings, relay)

    assert len(attempts) == 2
    assert attempts[0] == attempts[1] == (7, [row[0] for row in inbox_before_retry])
    inbox = await headquarters.execute("SELECT event_id, applied_at FROM hq_inbox ORDER BY seq")
    assert await inbox.fetchall() == inbox_before_retry
    orders = await headquarters.execute(
        "SELECT order_id, region_id, status, status_changed_at FROM analytics_orders"
    )
    assert await orders.fetchall() == [(created.order_id, "r1", "preparing", status.changed_at)]
    items = await headquarters.execute(
        "SELECT order_id, product_id, quantity FROM analytics_order_items"
    )
    assert await items.fetchall() == [(created.order_id, "0017", 2)]
    exceptions = await headquarters.execute("SELECT order_id, kind FROM analytics_exceptions")
    assert await exceptions.fetchall() == [(created.order_id, exception.kind)]
    assert await read_pending(region_pool, "r1", 500, settings=settings) == []


async def test_hq_rolls_back_inbox_and_analytics_if_apply_fails(outbox, hq, payloads):
    region, region_pool, settings = outbox
    headquarters, hq_pool, _ = hq
    created, status, _ = payloads
    unmatched_status = status.model_copy(update={"order_id": uuid4()})
    await seed_outbox(
        region,
        [
            (1, EventType.ORDER_CREATED, created, None),
            (2, EventType.ORDER_STATUS_CHANGED, unmatched_status, None),
        ],
    )
    events = await read_pending(region_pool, "r1", 500, settings=settings)

    with pytest.raises(ValueError, match="no matching HQ order"):
        await apply_batch(hq_pool, events, settings=settings)

    cursor = await headquarters.execute(
        "SELECT (SELECT count(*) FROM hq_inbox),"
        " (SELECT count(*) FROM analytics_orders),"
        " (SELECT count(*) FROM analytics_order_items)"
    )
    assert await cursor.fetchone() == (0, 0, 0)


async def test_exception_sequence_follows_commit_order(hq, payloads):
    headquarters, hq_pool, settings = hq
    _, _, exception = payloads
    first_id, second_id = uuid4(), uuid4()
    event = OutboxEvent(
        event_id=second_id,
        region_id="r2",
        seq=1,
        event_type=EventType.EXCEPTION_LOGGED,
        order_day=14,
        created_at=exception.attempted_at,
        payload=exception.model_dump(mode="json"),
    )

    # Hold the first writer open while a second regional relay tries to apply.
    # Use a separate connection to observe PostgreSQL's actual lock wait.
    async with await psycopg.AsyncConnection.connect(hq_pool.conninfo, autocommit=True) as observer:
        task = None
        try:
            async with headquarters.transaction():
                await headquarters.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (ANALYTICS_EXCEPTIONS_LOCK_KEY,)
                )
                await headquarters.execute(
                    "INSERT INTO analytics_exceptions "
                    "(event_id, region_id, order_day, order_id, kind, current_status,"
                    " current_changed_at, attempted_status, attempted_at) "
                    "VALUES (%s, 'r1', 14, %s, %s, %s, %s, %s, %s)",
                    (
                        first_id,
                        exception.order_id,
                        exception.kind,
                        exception.current_status,
                        exception.current_changed_at,
                        exception.attempted_status,
                        exception.attempted_at,
                    ),
                )
                task = asyncio.create_task(apply_batch(hq_pool, [event], settings=settings))
                for _ in range(100):
                    locks = await observer.execute(
                        "SELECT count(*) FROM pg_locks "
                        "WHERE locktype = 'advisory' AND objid = %s AND NOT granted",
                        (ANALYTICS_EXCEPTIONS_LOCK_KEY,),
                    )
                    if (await locks.fetchone())[0]:
                        break
                    if task.done():
                        task.result()
                        pytest.fail("second exception committed before the first writer")
                    await asyncio.sleep(0.01)
                else:
                    pytest.fail("second writer did not wait on the shared exception lock")
                assert not task.done()
        finally:
            # The transaction has released its lock. Let the database call finish.
            if task is not None:
                await task

    rows = await headquarters.execute("SELECT event_id, seq FROM analytics_exceptions ORDER BY seq")
    first, second = await rows.fetchall()
    assert first[0] == first_id
    assert second[0] == second_id
    assert first[1] < second[1]

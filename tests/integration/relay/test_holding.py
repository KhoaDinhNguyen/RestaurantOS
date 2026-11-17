"""Exercise the HQ queue with the real schema and a test-only WP1 stand-in."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

from restaurantos.common.cluster import Cluster, proxy_for
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.common.models import OrderRequest, OrderResult
from restaurantos.relay import holding
from restaurantos.relay.config import RelaySettings

pytestmark = pytest.mark.integration

RECEIVED_AT = datetime(2026, 11, 14, 12, 30, tzinfo=UTC)


@pytest.fixture
def relay():
    return RelaySettings(_env_file=None, role="hq", batch_size=2)


@pytest.fixture
def request_body():
    return OrderRequest(
        channel="mini",
        external_order_id="parked-1",
        vendor_id="v001",
        order_day=14,
        order_time="12:00:00",
        items=[{"product_id": "0017", "quantity": 2}],
    )


@pytest.fixture
async def queue(hq, outbox, monkeypatch):
    conn, pool, settings = hq
    _, region_pool, _ = outbox
    await conn.execute(
        "INSERT INTO region_state (region_id, primary_node, epoch)"
        " VALUES ('r1', 'r1-standby', 7), ('r2', 'r2-primary', 1)"
    )
    # Keep name generation real, then route the resulting connections to the
    # isolated region schema. Assert the promoted standby gets the hq proxy.
    real_conninfo = holding.node_conninfo
    destinations = []

    def conninfo(cluster, node, **kwargs):
        destination = real_conninfo(cluster, node, **kwargs)
        destinations.append((node, destination))
        return region_pool.conninfo

    monkeypatch.setattr(holding, "node_conninfo", conninfo)
    return conn, pool, settings.model_copy(update={"client": "hq"}), destinations


async def park(conn, request, *, region_id="r1", received_at=RECEIVED_AT):
    cursor = await conn.execute(
        "INSERT INTO holding_queue"
        " (region_id, channel, external_order_id, request, received_at)"
        " VALUES (%s, %s, %s, %s, %s) RETURNING holding_id",
        (
            region_id,
            request.channel,
            request.external_order_id,
            Jsonb(request.model_dump(mode="json")),
            received_at,
        ),
    )
    return (await cursor.fetchone())[0]


class RegionOrder:
    """Test-only inbox behavior. Actual WP1 stock/order logic is still pending."""

    def __init__(self, outcome="accepted"):
        self.outcome = outcome
        self.calls = []
        self.duplicates = []

    async def __call__(self, conn, req, epoch, *, received_at):
        self.calls.append((req.external_order_id, epoch, received_at))
        order_id = uuid4() if self.outcome == "accepted" else None
        reason = "out_of_stock" if self.outcome == "refused" else None
        inserted = await conn.execute(
            "INSERT INTO inbox (channel, external_order_id, outcome, order_day,"
            " order_id, refusal_reason)"
            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (req.channel, req.external_order_id, self.outcome, req.order_day, order_id, reason),
        )
        self.duplicates.append(inserted.rowcount == 0)
        return OrderResult(
            outcome=self.outcome,
            duplicate=self.duplicates[-1],
            order_id=order_id,
            refusal_reason=reason,
            order_day=req.order_day,
        )


@pytest.mark.parametrize("outcome", ["accepted", "refused"])
async def test_delivers_in_order_preserves_time_and_limits_batch(
    queue, outbox, request_body, relay, outcome
):
    conn, pool, settings, destinations = queue
    place_order = RegionOrder(outcome)
    ids = []
    for index in range(3):
        req = request_body.model_copy(update={"external_order_id": f"parked-{index}"})
        ids.append(await park(conn, req))
    await park(
        conn, request_body.model_copy(update={"external_order_id": "other-region"}), region_id="r2"
    )
    count = await holding.drain_batch(pool, Cluster(regions=2), "r1", settings, relay, place_order)
    assert count == 2
    assert place_order.calls == [
        ("parked-0", 7, RECEIVED_AT),
        ("parked-1", 7, RECEIVED_AT),
    ]
    assert destinations[0][0] == "r1-standby"
    assert conninfo_to_dict(destinations[0][1])["host"] == "toxiproxy"
    assert conninfo_to_dict(destinations[0][1])["port"] == str(
        proxy_for(Cluster(regions=2), "hq", "r1-standby").port
    )
    rows = await conn.execute(
        "SELECT holding_id, delivered_at IS NOT NULL, delivered_outcome"
        " FROM holding_queue WHERE region_id = 'r1' ORDER BY holding_id"
    )
    assert await rows.fetchall() == [
        (ids[0], True, outcome),
        (ids[1], True, outcome),
        (ids[2], False, None),
    ]
    assert (
        await holding.drain_batch(pool, Cluster(regions=2), "r1", settings, relay, place_order) == 1
    )
    assert (
        await holding.drain_batch(pool, Cluster(regions=2), "r1", settings, relay, place_order) == 0
    )
    other = await conn.execute("SELECT delivered_at FROM holding_queue WHERE region_id = 'r2'")
    assert await other.fetchone() == (None,)


async def test_retry_after_regional_commit_uses_inbox(
    queue, outbox, request_body, relay, monkeypatch
):
    conn, pool, settings, _ = queue
    await park(conn, request_body)
    place_order = RegionOrder()
    real_run_on = holding.run_on

    async def lose_hq_ack(pool, work, **kwargs):
        if work.__name__ == "acknowledge":
            raise RegionUnreachableError("lost HQ connection after regional commit")
        return await real_run_on(pool, work, **kwargs)

    monkeypatch.setattr(holding, "run_on", lose_hq_ack)
    with pytest.raises(RegionUnreachableError):
        await holding.drain_batch(pool, Cluster(), "r1", settings, relay, place_order)

    pending = await conn.execute("SELECT delivered_at FROM holding_queue")
    assert await pending.fetchone() == (None,)
    regional_conn, _, _ = outbox
    inbox = await regional_conn.execute("SELECT count(*) FROM inbox")
    assert await inbox.fetchone() == (1,)

    monkeypatch.setattr(holding, "run_on", real_run_on)
    assert await holding.drain_batch(pool, Cluster(), "r1", settings, relay, place_order) == 1
    assert place_order.duplicates == [False, True]
    inbox = await regional_conn.execute("SELECT count(*) FROM inbox")
    assert await inbox.fetchone() == (1,)
    delivered = await conn.execute("SELECT delivered_outcome FROM holding_queue")
    assert await delivered.fetchone() == ("accepted",)


async def test_unreachable_region_keeps_first_and_later_orders_pending(queue, request_body, relay):
    conn, pool, settings, _ = queue
    await park(conn, request_body)
    await park(conn, request_body.model_copy(update={"external_order_id": "parked-2"}))
    calls = []

    async def unreachable(conn, request, epoch, *, received_at):
        calls.append(request.external_order_id)
        raise RegionUnreachableError(region_id="r1")

    with pytest.raises(RegionUnreachableError):
        await holding.drain_batch(pool, Cluster(), "r1", settings, relay, unreachable)
    assert calls == ["parked-1"]
    pending = await conn.execute("SELECT count(*) FROM holding_queue WHERE delivered_at IS NULL")
    assert await pending.fetchone() == (2,)


async def test_stale_epoch_refreshes_node_and_retries_once(queue, request_body, relay):
    conn, pool, settings, destinations = queue
    await park(conn, request_body)
    calls = []

    async def promoted(region_conn, request, epoch, *, received_at):
        calls.append(epoch)
        if epoch == 7:
            await conn.execute(
                "UPDATE region_state SET primary_node = 'r1-primary', epoch = 8"
                " WHERE region_id = 'r1'"
            )
            raise StaleEpochError()
        return OrderResult(outcome="refused", refusal_reason="unavailable")

    assert await holding.drain_batch(pool, Cluster(), "r1", settings, relay, promoted) == 1
    assert calls == [7, 8]
    assert [node for node, _ in destinations] == ["r1-standby", "r1-primary"]


async def test_repeated_stale_epoch_leaves_order_pending(queue, request_body, relay):
    conn, pool, settings, _ = queue
    await park(conn, request_body)
    calls = []

    async def stale(conn, request, epoch, *, received_at):
        calls.append(epoch)
        raise StaleEpochError()

    with pytest.raises(StaleEpochError):
        await holding.drain_batch(pool, Cluster(), "r1", settings, relay, stale)
    assert calls == [7, 7]
    pending = await conn.execute("SELECT delivered_at FROM holding_queue")
    assert await pending.fetchone() == (None,)

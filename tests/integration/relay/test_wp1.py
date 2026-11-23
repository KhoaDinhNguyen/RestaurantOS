"""WP4 using the real WP1 functions and real isolated PostgreSQL schemas."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from restaurantos.common.cluster import HQ_PRIMARY, Cluster
from restaurantos.common.db import run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.models import OrderRequest, OrderStatus
from restaurantos.gateway.orders import park_order
from restaurantos.region import apply_menu_version, change_status, place_order
from restaurantos.relay import holding, menu, worker
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.outbox import read_pending

pytestmark = pytest.mark.integration

RECEIVED_AT = datetime(2026, 11, 14, 12, 30, tzinfo=UTC)

REFERENCE = """
INSERT INTO vendors (vendor_id, geohash) VALUES ('v1', 'w21z7');
INSERT INTO ingredients (ingredient_id, source) VALUES ('rice', 'test');
INSERT INTO dishes (dish_id, scope, stock_mode) VALUES
    ('recipe', 'global', 'per_ingredient'), ('cakes', 'global', 'per_item');
INSERT INTO dish_ingredients (dish_id, ingredient_id, amount, unit)
VALUES ('recipe', 'rice', 2, 'kg');
INSERT INTO products (vendor_id, product_id, name, unit_price, price_units)
VALUES ('v1', '0017', 'Old menu', 0.1234567890123456789, 11);
INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier)
VALUES ('v1', '0017', 'recipe', 3);
"""


@pytest.fixture
async def live(hq, outbox, monkeypatch):
    headquarters, hq_pool, settings = hq
    region, region_pool, _ = outbox
    await headquarters.execute(REFERENCE)
    await region.execute(REFERENCE)
    await headquarters.execute(
        "INSERT INTO region_state (region_id, primary_node, epoch) VALUES ('r1', 'r1-primary', 7)"
    )
    await headquarters.execute("INSERT INTO shard_directory VALUES ('w21z7', 'r1')")
    await headquarters.execute("INSERT INTO menu_versions (version) VALUES (2)")
    await headquarters.execute(
        "UPDATE products SET name = 'New menu', price_units = 20, menu_version = 2"
    )
    await headquarters.execute(
        "INSERT INTO products (vendor_id, product_id, name, unit_price, price_units, menu_version)"
        " VALUES ('v1', 'new-cakes', 'Box of four', 0.2, 25, 2)"
    )
    await headquarters.execute(
        "INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier)"
        " VALUES ('v1', 'new-cakes', 'cakes', 4)"
    )
    await region.execute("INSERT INTO node_state (region_id, epoch) VALUES ('r1', 7)")
    await region.execute(
        "INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level)"
        " VALUES ('v1', 'ingredient', 'rice', 40, 40)"
    )

    def conninfo(cluster, node, **kwargs):
        return hq_pool.conninfo if node == HQ_PRIMARY else region_pool.conninfo

    for module in (holding, menu, worker):
        monkeypatch.setattr(module, "node_conninfo", conninfo)

    async def primary(*args, **kwargs):
        return "r1-primary"

    monkeypatch.setattr(worker, "find_local_primary", primary)
    return headquarters, hq_pool, region, region_pool, settings.model_copy(update={"client": "hq"})


def request(external="accepted", quantity=2):
    return OrderRequest(
        channel="mini",
        external_order_id=external,
        vendor_id="v1",
        customer_id="c1",
        customer_geohash="w21z7",
        order_day=14,
        order_time="12:00:00",
        items=[{"product_id": "0017", "quantity": quantity}],
    )


async def one(conn, statement, params=()):
    cursor = await conn.execute(statement, params)
    return await cursor.fetchone()


async def test_real_menu_parked_orders_status_events_and_relay_retry(live, monkeypatch):
    hq, hq_pool, region, region_pool, settings = live
    cluster = Cluster()
    relay = RelaySettings(_env_file=None, role="hq", poll_interval_s=0.001)
    await menu.publish_menu(hq_pool, cluster, "r1", settings, apply_menu_version)
    await park_order(hq_pool, request(), "r1", RECEIVED_AT)
    await park_order(hq_pool, request("refused", quantity=10), "r1", RECEIVED_AT)
    assert await holding.drain_batch(hq_pool, cluster, "r1", settings, relay, place_order) == 2

    delivered = await hq.execute(
        "SELECT external_order_id, delivered_outcome, delivered_at IS NOT NULL"
        " FROM holding_queue ORDER BY holding_id"
    )
    assert await delivered.fetchall() == [
        ("accepted", "accepted", True),
        ("refused", "refused", True),
    ]
    assert await one(region, "SELECT on_hand FROM stock WHERE item_id = 'rice'") == (28,)
    order_id, received_at, version = await one(
        region, "SELECT order_id, received_at, menu_version FROM orders"
    )
    assert received_at == RECEIVED_AT
    assert version == 2

    # These are real WP1 status updates, producing the other two event types.
    for to_status in (OrderStatus.ACCEPTED, OrderStatus.READY):

        async def change(conn, to_status=to_status):
            return await change_status(conn, "v1", order_id, uuid4(), to_status, 7)

        result = await run_on(region_pool, change, region_id="r1", settings=settings)
        assert result.applied if to_status == OrderStatus.ACCEPTED else result.conflict

    real_mark = worker.mark_relayed
    attempts = 0

    class RelayStopped(Exception):
        pass

    async def interrupted_ack(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            assert await one(hq, "SELECT count(*) FROM hq_inbox") == (3,)
            raise RegionUnreachableError("regional acknowledgement lost after HQ commit")
        await real_mark(*args, **kwargs)
        raise RelayStopped

    monkeypatch.setattr(worker, "mark_relayed", interrupted_ack)
    with pytest.raises(RelayStopped):
        await worker.run_region_relay(cluster, "r1", settings, relay)

    assert attempts == 2
    assert await one(hq, "SELECT count(*) FROM hq_inbox") == (3,)
    assert await one(hq, "SELECT count(*) FROM analytics_orders") == (1,)
    assert await one(
        hq, "SELECT order_id, received_at, status, menu_version FROM analytics_orders"
    ) == (order_id, RECEIVED_AT, "accepted", 2)
    assert await one(hq, "SELECT product_id, quantity, price_units FROM analytics_order_items") == (
        "0017",
        2,
        20,
    )
    assert await one(
        hq, "SELECT order_id, current_status, attempted_status FROM analytics_exceptions"
    ) == (order_id, "accepted", "ready")
    assert await read_pending(region_pool, "r1", 500, settings=settings) == []


@pytest.mark.parametrize("quantity, outcome", [(2, "accepted"), (10, "refused")])
async def test_real_wp1_retry_preserves_stock_and_original_outcome(
    live, monkeypatch, quantity, outcome
):
    hq, hq_pool, region, _, settings = live
    await park_order(hq_pool, request(quantity=quantity), "r1", RECEIVED_AT)
    real_run_on = holding.run_on

    async def lose_ack(pool, work, **kwargs):
        if work.__name__ == "acknowledge":
            raise RegionUnreachableError("HQ acknowledgement lost after regional commit")
        return await real_run_on(pool, work, **kwargs)

    relay = RelaySettings(_env_file=None, role="hq")
    monkeypatch.setattr(holding, "run_on", lose_ack)
    with pytest.raises(RegionUnreachableError):
        await holding.drain_batch(hq_pool, Cluster(), "r1", settings, relay, place_order)
    assert await one(hq, "SELECT delivered_at FROM holding_queue") == (None,)
    stock_after_first = await one(region, "SELECT on_hand FROM stock WHERE item_id = 'rice'")
    assert stock_after_first == ((28,) if outcome == "accepted" else (40,))

    # Change availability before the resend: WP1 must return its original
    # stored decision, even if the product's current state has changed.
    await region.execute(
        "INSERT INTO product_availability (vendor_id, product_id, available, reason)"
        " VALUES ('v1', '0017', false, 'manual')"
    )
    monkeypatch.setattr(holding, "run_on", real_run_on)
    assert await holding.drain_batch(hq_pool, Cluster(), "r1", settings, relay, place_order) == 1
    assert await one(hq, "SELECT delivered_outcome FROM holding_queue") == (outcome,)
    assert (
        await one(region, "SELECT on_hand FROM stock WHERE item_id = 'rice'") == stock_after_first
    )
    assert await one(region, "SELECT count(*) FROM inbox") == (1,)
    expected = 1 if outcome == "accepted" else 0
    assert await one(region, "SELECT count(*) FROM orders") == (expected,)
    assert await one(region, "SELECT count(*) FROM outbox") == (expected,)


async def test_real_menu_rolls_back_on_database_failure_and_recovers_after_promotion(live):
    hq, hq_pool, region, _, settings = live
    # Fail inside the actual WP1 product upsert, rather than replacing its
    # function. Everything before this database error must roll back.
    await region.execute("""
        CREATE FUNCTION fail_menu_update() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'simulated connection failure' USING ERRCODE = '08006';
        END;
        $$;
        CREATE TRIGGER fail_menu AFTER UPDATE ON products
        FOR EACH ROW EXECUTE FUNCTION fail_menu_update();
    """)
    with pytest.raises(RegionUnreachableError):
        await menu.publish_menu(hq_pool, Cluster(), "r1", settings, apply_menu_version)
    assert await one(region, "SELECT menu_version FROM node_state") == (1,)
    assert await one(
        region, "SELECT name, price_units FROM products WHERE product_id = '0017'"
    ) == (
        "Old menu",
        11,
    )
    assert await one(region, "SELECT count(*) FROM products WHERE product_id = 'new-cakes'") == (0,)
    assert await one(hq, "SELECT menu_version FROM region_state") == (1,)

    await region.execute("DROP TRIGGER fail_menu ON products")
    await menu.publish_menu(hq_pool, Cluster(), "r1", settings, apply_menu_version)
    assert await one(region, "SELECT menu_version FROM node_state") == (2,)
    assert await one(
        region, "SELECT name, unit_price, price_units FROM products WHERE product_id = '0017'"
    ) == ("New menu", Decimal("0.1234567890123456789"), 20)
    assert await one(
        region, "SELECT on_hand, par_level >= 4 FROM stock WHERE item_id = 'cakes'"
    ) == (0, True)
    assert await one(
        region, "SELECT available, reason FROM product_availability WHERE product_id = 'new-cakes'"
    ) == (False, "stock")

    # HQ remembers version 2, but a promoted node can have an older marker.
    # Keep stock depleted to catch accidental resets during the resend.
    await region.execute("UPDATE node_state SET menu_version = 1, epoch = 8")
    await region.execute("UPDATE stock SET on_hand = 9 WHERE item_id = 'rice'")
    await hq.execute("UPDATE region_state SET primary_node = 'r1-standby', epoch = 8")
    await menu.publish_menu(hq_pool, Cluster(), "r1", settings, apply_menu_version)
    assert await one(region, "SELECT menu_version FROM node_state") == (2,)
    assert await one(region, "SELECT on_hand FROM stock WHERE item_id = 'rice'") == (9,)
    assert await one(hq, "SELECT menu_version FROM region_state") == (2,)

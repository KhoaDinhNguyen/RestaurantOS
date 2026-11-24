"""Top-dishes queries against real PostgreSQL schemas."""

from __future__ import annotations

import os
from datetime import time
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.sqlfiles import schema_files
from restaurantos.gateway.router.top_dishes import query_top_dishes_node
from restaurantos.gateway.router.window import QueryWindow

pytestmark = pytest.mark.integration


SEED = """
INSERT INTO node_state (region_id) VALUES ('r1');

INSERT INTO dishes (
    dish_id, dish_name, scope, stock_mode, example_name
) VALUES
    ('d1', 'shared dish', 'global', 'none', 'Shared'),
    ('d2', 'other dish', 'global', 'none', 'Other');

INSERT INTO vendors (vendor_id, geohash) VALUES
    ('v1', 'w21z7');

INSERT INTO products (
    vendor_id, product_id, name, unit_price, price_units
) VALUES
    ('v1', 'p1', 'Set of 3', 0.01, 20),
    ('v1', 'p2', 'Other', 0.01, 7);

INSERT INTO product_dish (
    vendor_id, product_id, dish_id, multiplier
) VALUES
    ('v1', 'p1', 'd1', 3),
    ('v1', 'p2', 'd2', 1);
"""


async def insert_order(
    conn,
    *,
    order_id: UUID,
    day: int,
    at: time,
    status: str,
    product_id: str,
    quantity: int,
    price_units: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO orders (
            order_day, order_id, vendor_id, channel,
            external_order_id, order_time, status,
            menu_version, epoch
        )
        VALUES (%s, %s, 'v1', 'test', %s, %s, %s, 1, 1)
        """,
        (
            day,
            order_id,
            str(order_id),
            at,
            status,
        ),
    )

    await conn.execute(
        """
        INSERT INTO order_items (
            order_day, order_id, line_no, vendor_id,
            product_id, quantity, price_units
        )
        VALUES (%s, %s, 1, 'v1', %s, %s, %s)
        """,
        (
            day,
            order_id,
            product_id,
            quantity,
            price_units,
        ),
    )


@pytest.fixture
async def region_pool():
    dsn = os.environ.get("ROS_TEST_DSN")

    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    schema = f"ros_test_wp3_top_{uuid4().hex[:8]}"

    admin = await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
    )
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    setup = await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
        options=f"-c search_path={schema}",
    )

    try:
        for path in schema_files("region"):
            await setup.execute(path.read_text(encoding="utf-8"))

        await setup.execute(SEED)

        await insert_order(
            setup,
            order_id=uuid4(),
            day=14,
            at=time(12, 0),
            status="picked_up",
            product_id="p1",
            quantity=2,
            price_units=20,
        )
        await insert_order(
            setup,
            order_id=uuid4(),
            day=14,
            at=time(13, 0),
            status="picked_up",
            product_id="p1",
            quantity=1,
            price_units=20,
        )
        await insert_order(
            setup,
            order_id=uuid4(),
            day=14,
            at=time(14, 0),
            status="canceled",
            product_id="p1",
            quantity=10,
            price_units=20,
        )

        # Cross-midnight examples.
        await insert_order(
            setup,
            order_id=uuid4(),
            day=20,
            at=time(23, 0),
            status="picked_up",
            product_id="p2",
            quantity=2,
            price_units=7,
        )
        await insert_order(
            setup,
            order_id=uuid4(),
            day=21,
            at=time(2, 0),
            status="picked_up",
            product_id="p2",
            quantity=3,
            price_units=7,
        )
        await insert_order(
            setup,
            order_id=uuid4(),
            day=20,
            at=time(2, 0),
            status="picked_up",
            product_id="p2",
            quantity=100,
            price_units=7,
        )

        pool = AsyncConnectionPool(
            dsn,
            kwargs={
                "autocommit": True,
                "options": f"-c search_path={schema}",
            },
            open=False,
        )
        await pool.open()

        try:
            yield pool
        finally:
            await pool.close()
    finally:
        await setup.close()
        await admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        await admin.close()


async def test_all_metrics_and_canceled_orders(region_pool) -> None:
    rows = await query_top_dishes_node(
        region_pool,
        QueryWindow(14, 14),
        region_id="r1",
    )

    assert rows == [
        {
            "dish_id": "d1",
            "dish_name": "shared dish",
            "example_name": "Shared",
            "units": 9,
            "quantity": 3,
            "order_lines": 2,
            "sales_units": 60,
        }
    ]


async def test_normal_time_window(region_pool) -> None:
    rows = await query_top_dishes_node(
        region_pool,
        QueryWindow(
            14,
            14,
            time(12, 30),
            time(13, 30),
        ),
        region_id="r1",
    )

    assert rows[0]["quantity"] == 1
    assert rows[0]["units"] == 3
    assert rows[0]["order_lines"] == 1


async def test_cross_midnight_window_belongs_to_start_day(
    region_pool,
) -> None:
    rows = await query_top_dishes_node(
        region_pool,
        QueryWindow(
            20,
            20,
            time(22, 0),
            time(4, 0),
        ),
        region_id="r1",
    )

    assert rows == [
        {
            "dish_id": "d2",
            "dish_name": "other dish",
            "example_name": "Other",
            "units": 5,
            "quantity": 5,
            "order_lines": 2,
            "sales_units": 35,
        }
    ]

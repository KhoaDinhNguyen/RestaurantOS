"""Nearby query against a real region PostgreSQL schema."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.sqlfiles import schema_files
from restaurantos.gateway.router.nearby import NearbyCell, query_region

pytestmark = pytest.mark.integration


SEED = """
INSERT INTO node_state (region_id) VALUES ('r1');

INSERT INTO ingredients (ingredient_id, source) VALUES
    ('rice', 'test'),
    ('beef', 'test');

INSERT INTO dishes (
    dish_id,
    dish_name,
    scope,
    stock_mode
) VALUES
    ('d_rice', 'beef rice', 'global', 'per_ingredient'),
    ('d_cake', 'cake', 'global', 'per_item'),
    ('d_pie', 'pie', 'global', 'per_item'),
    ('d_tea', 'tea', 'global', 'none');

INSERT INTO dish_ingredients (
    dish_id,
    ingredient_id,
    amount,
    unit
) VALUES
    ('d_rice', 'rice', 250, 'gram'),
    ('d_rice', 'beef', 2, 'kg');

INSERT INTO vendors (
    vendor_id,
    geohash,
    primary_cuisine
) VALUES
    ('v1', 'w21z7', 'rice'),
    ('v2', 'w21z6', 'tea');

INSERT INTO products (
    vendor_id,
    product_id,
    name,
    unit_price,
    price_units
) VALUES
    ('v1', 'p_rice', 'Beef rice, double', 0.012, 30),
    ('v1', 'p_rice_small', 'Beef rice', 0.006, 15),
    ('v1', 'p_cake', 'Cake, box of 3', 0.0048, 12),
    ('v1', 'p_pie', 'Pie', 0.002, 5),
    ('v1', 'p_tea', 'Tea', 0.0008, 2),
    ('v2', 'p_other', 'Other tea', 0.0004, 1);

INSERT INTO product_dish (
    vendor_id,
    product_id,
    dish_id,
    multiplier
) VALUES
    ('v1', 'p_rice', 'd_rice', 2),
    ('v1', 'p_rice_small', 'd_rice', 1),
    ('v1', 'p_cake', 'd_cake', 3),
    ('v1', 'p_pie', 'd_pie', 1),
    ('v1', 'p_tea', 'd_tea', 1),
    ('v2', 'p_other', 'd_tea', 1);

INSERT INTO stock (
    vendor_id,
    item_kind,
    item_id,
    on_hand,
    par_level
) VALUES
    ('v1', 'ingredient', 'rice', 300, 2000),
    ('v1', 'ingredient', 'beef', 100, 100),
    ('v1', 'dish', 'd_cake', 3, 10);
"""


@pytest.fixture
async def region_pool() -> AsyncIterator[AsyncConnectionPool]:
    dsn = os.environ.get("ROS_TEST_DSN")

    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    schema = f"ros_test_wp3_{uuid4().hex[:8]}"

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


async def query(
    region_pool,
    dish_id: str,
    *,
    limit: int = 10,
):
    return await query_region(
        region_pool,
        "r1",
        [
            NearbyCell("w21z7", 0.0),
            NearbyCell("w21z6", 4885.0),
        ],
        dish_id,
        limit,
    )


async def test_per_ingredient_respects_amount_and_multiplier(
    region_pool,
) -> None:
    rows = await query(region_pool, "d_rice")

    # rice has 300 g. p_rice needs 250 * 2 = 500, while
    # p_rice_small needs 250 * 1 = 250.
    assert [row["product_id"] for row in rows] == ["p_rice_small"]


async def test_per_item_respects_multiplier(region_pool) -> None:
    rows = await query(region_pool, "d_cake")

    # The dish counter holds exactly 3 and the product multiplier is 3.
    assert [row["product_id"] for row in rows] == ["p_cake"]

    async with region_pool.connection() as conn:
        await conn.execute(
            """
            UPDATE stock
            SET on_hand = 2
            WHERE vendor_id = 'v1'
              AND item_kind = 'dish'
              AND item_id = 'd_cake'
            """
        )

    assert await query(region_pool, "d_cake") == []


async def test_missing_stock_row_counts_as_zero(region_pool) -> None:
    assert await query(region_pool, "d_pie") == []


async def test_stock_mode_none_always_passes(region_pool) -> None:
    rows = await query(region_pool, "d_tea")

    assert [(row["vendor_id"], row["product_id"]) for row in rows] == [
        ("v1", "p_tea"),
        ("v2", "p_other"),
    ]


async def test_manual_availability_mark_filters_product(
    region_pool,
) -> None:
    async with region_pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO product_availability (
                vendor_id,
                product_id,
                available,
                reason
            )
            VALUES ('v1', 'p_tea', false, 'manual')
            """
        )

    rows = await query(region_pool, "d_tea")

    assert [(row["vendor_id"], row["product_id"]) for row in rows] == [
        ("v2", "p_other"),
    ]


async def test_region_query_orders_and_limits_rows(
    region_pool,
) -> None:
    rows = await query(
        region_pool,
        "d_tea",
        limit=1,
    )

    assert len(rows) == 1
    assert rows[0]["vendor_id"] == "v1"
    assert rows[0]["distance_m"] == 0.0
    assert rows[0]["region_id"] == "r1"

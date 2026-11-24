"""Ingredient-usage strategies against real PostgreSQL."""

from __future__ import annotations

import os
from datetime import time
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.sqlfiles import schema_files
from restaurantos.gateway.router.ingredient_usage import (
    query_strategy_a_region,
    query_strategy_c_aggregate,
)
from restaurantos.gateway.router.window import QueryWindow

pytestmark = pytest.mark.integration

SEED = """
INSERT INTO node_state (region_id) VALUES ('r1');

INSERT INTO ingredients (ingredient_id, source) VALUES
    ('rice', 'test'),
    ('beef', 'test');

INSERT INTO dishes (
    dish_id, dish_name, scope, stock_mode
) VALUES
    ('d_rice', 'beef rice', 'global', 'per_ingredient');

INSERT INTO dish_ingredients (
    dish_id, ingredient_id, amount, unit
) VALUES
    ('d_rice', 'rice', 250, 'gram'),
    ('d_rice', 'beef', 2, 'kg');

INSERT INTO vendors (vendor_id, geohash) VALUES
    ('v1', 'w21z7');

INSERT INTO products (
    vendor_id, product_id, name, unit_price, price_units
) VALUES
    ('v1', 'p_rice', 'Double beef rice', 0.01, 30);

INSERT INTO product_dish (
    vendor_id, product_id, dish_id, multiplier
) VALUES
    ('v1', 'p_rice', 'd_rice', 2);
"""


@pytest.fixture
async def region_pool():
    dsn = os.environ.get("ROS_TEST_DSN")

    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    schema = f"ros_test_wp3_usage_{uuid4().hex[:8]}"

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

        order_id = uuid4()

        await setup.execute(
            """
            INSERT INTO orders (
                order_day, order_id, vendor_id, channel,
                external_order_id, order_time, status,
                menu_version, epoch
            )
            VALUES (
                14, %s, 'v1', 'test', 'o1',
                %s, 'canceled', 1, 1
            )
            """,
            (order_id, time(12, 0)),
        )

        await setup.execute(
            """
            INSERT INTO order_items (
                order_day, order_id, line_no, vendor_id,
                product_id, quantity, price_units
            )
            VALUES (
                14, %s, 1, 'v1', 'p_rice', 3, 30
            )
            """,
            (order_id,),
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


async def test_strategy_a_multiplies_quantity_multiplier_and_amount(
    region_pool,
) -> None:
    rows = await query_strategy_a_region(
        region_pool,
        QueryWindow(14, 14),
        region_id="r1",
    )

    assert rows == [
        {
            "ingredient_id": "rice",
            "amount": 1500,
            "unit": "gram",
        },
        {
            "ingredient_id": "beef",
            "amount": 12,
            "unit": "kg",
        },
    ]


async def test_strategy_c_works_without_local_recipe_table(
    region_pool,
) -> None:
    recipes = [
        {
            "dish_id": "d_rice",
            "ingredient_id": "rice",
            "amount": 250,
            "unit": "gram",
        },
        {
            "dish_id": "d_rice",
            "ingredient_id": "beef",
            "amount": 2,
            "unit": "kg",
        },
    ]

    # Strategy C must use the recipes shipped from HQ, not the
    # region's local dish_ingredients table.
    async with region_pool.connection() as conn:
        await conn.execute("DELETE FROM dish_ingredients")

    rows = await query_strategy_c_aggregate(
        region_pool,
        QueryWindow(14, 14),
        recipes,
        region_id="r1",
    )

    assert rows == [
        {
            "ingredient_id": "rice",
            "amount": 1500,
            "unit": "gram",
        },
        {
            "ingredient_id": "beef",
            "amount": 12,
            "unit": "kg",
        },
    ]

"""Distributed query router against r1, r2, HQ, and baseline schemas."""

from __future__ import annotations

import os
from dataclasses import dataclass
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.sqlfiles import schema_files
from restaurantos.gateway.directory import DirectoryCache, RegionRoute
from restaurantos.gateway.router.ingredient_usage import (
    ingredient_usage_single,
    join_lines_with_recipes,
    merge_ingredient_usage,
    query_recipes,
    query_strategy_a_region,
    query_strategy_b_lines,
    query_strategy_c_aggregate,
    query_strategy_c_dishes,
)
from restaurantos.gateway.router.nearby import nearby_live
from restaurantos.gateway.router.top_dishes import (
    merge_top_dishes,
    query_top_dishes_node,
)
from restaurantos.gateway.router.window import QueryWindow
from restaurantos.sim.mini import load_mini

pytestmark = pytest.mark.integration


@dataclass
class MiniCluster:
    r1: AsyncConnectionPool
    r2: AsyncConnectionPool
    baseline: AsyncConnectionPool
    hq: AsyncConnectionPool


class MiniRegionPools:
    """Map fake cluster node names to the mini fixture pools."""

    def __init__(self, cluster: MiniCluster):
        self.pools = {
            "r1-primary": cluster.r1,
            "r2-primary": cluster.r2,
        }

    async def get(self, node: str):
        return self.pools[node]


def mini_directory() -> DirectoryCache:
    r1 = RegionRoute(
        region_id="r1",
        epoch=1,
        primary_node="r1-primary",
    )
    r2 = RegionRoute(
        region_id="r2",
        epoch=1,
        primary_node="r2-primary",
    )

    return DirectoryCache(
        vendor_cells={
            "v001": "w21z7",
            "v002": "w21z7",
            "v003": "w21z6",
            "v004": "w21zt",
        },
        cell_routes={
            "w21z7": r1,
            "w21z6": r2,
            "w21zt": r2,
        },
        region_routes={
            "r1": r1,
            "r2": r2,
        },
    )


async def make_schema(
    dsn: str,
    admin: psycopg.AsyncConnection,
    name: str,
    vendors: list[str] | None,
) -> AsyncConnectionPool:
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    setup = await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
        options=f"-c search_path={name}",
    )

    for path in schema_files("region"):
        await setup.execute(path.read_text(encoding="utf-8"))

    # load_mini is synchronous, so load the same schema through a
    # short-lived synchronous connection.
    await setup.close()

    with psycopg.connect(
        dsn,
        autocommit=True,
        options=f"-c search_path={name}",
    ) as sync_conn:
        load_mini(sync_conn, vendors=vendors)

    pool = AsyncConnectionPool(
        dsn,
        kwargs={
            "autocommit": True,
            "options": f"-c search_path={name}",
        },
        open=False,
    )
    await pool.open()

    return pool


async def make_hq_schema(
    dsn: str,
    admin: psycopg.AsyncConnection,
    name: str,
    baseline_name: str,
) -> AsyncConnectionPool:
    """Create HQ analytics containing the same mini facts as baseline."""

    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    setup = await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
        options=f"-c search_path={name}",
    )

    try:
        for path in schema_files("hq"):
            await setup.execute(path.read_text(encoding="utf-8"))

        # HQ needs the shared reference data.
        with psycopg.connect(
            dsn,
            autocommit=True,
            options=f"-c search_path={name}",
        ) as sync_conn:
            load_mini(
                sync_conn,
                with_region_data=False,
            )

        # Copy the mini region facts into HQ's analytics representation.
        await setup.execute(
            sql.SQL(
                """
                INSERT INTO analytics_orders (
                    order_day,
                    order_id,
                    region_id,
                    vendor_id,
                    channel,
                    external_order_id,
                    customer_id,
                    customer_geohash,
                    order_time,
                    status,
                    menu_version,
                    received_at,
                    committed_at
                )
                SELECT
                    order_day,
                    order_id,
                    CASE
                        WHEN vendor_id IN ('v001', 'v002')
                            THEN 'r1'
                        ELSE 'r2'
                    END,
                    vendor_id,
                    channel,
                    external_order_id,
                    customer_id,
                    customer_geohash,
                    order_time,
                    status,
                    menu_version,
                    received_at,
                    committed_at
                FROM {}.orders
                """
            ).format(sql.Identifier(baseline_name))
        )

        await setup.execute(
            sql.SQL(
                """
                INSERT INTO analytics_order_items (
                    order_day,
                    order_id,
                    line_no,
                    vendor_id,
                    product_id,
                    quantity,
                    price_units
                )
                SELECT
                    order_day,
                    order_id,
                    line_no,
                    vendor_id,
                    product_id,
                    quantity,
                    price_units
                FROM {}.order_items
                """
            ).format(sql.Identifier(baseline_name))
        )
    finally:
        await setup.close()

    pool = AsyncConnectionPool(
        dsn,
        kwargs={
            "autocommit": True,
            "options": f"-c search_path={name}",
        },
        open=False,
    )
    await pool.open()

    return pool


@pytest.fixture
async def mini_cluster():
    dsn = os.environ.get("ROS_TEST_DSN")

    if not dsn:
        pytest.skip("ROS_TEST_DSN is not set")

    suffix = uuid4().hex[:8]
    names = {
        "r1": f"ros_wp3_r1_{suffix}",
        "r2": f"ros_wp3_r2_{suffix}",
        "baseline": f"ros_wp3_base_{suffix}",
        "hq": f"ros_wp3_hq_{suffix}",
    }

    admin = await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
    )

    pools: list[AsyncConnectionPool] = []

    try:
        r1 = await make_schema(
            dsn,
            admin,
            names["r1"],
            ["v001", "v002"],
        )
        pools.append(r1)

        r2 = await make_schema(
            dsn,
            admin,
            names["r2"],
            ["v003", "v004"],
        )
        pools.append(r2)

        baseline = await make_schema(
            dsn,
            admin,
            names["baseline"],
            None,
        )
        pools.append(baseline)

        hq = await make_hq_schema(
            dsn,
            admin,
            names["hq"],
            names["baseline"],
        )
        pools.append(hq)

        yield MiniCluster(
            r1=r1,
            r2=r2,
            baseline=baseline,
            hq=hq,
        )
    finally:
        for pool in pools:
            await pool.close()

        for name in names.values():
            await admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name))
            )

        await admin.close()


async def test_top_dishes_cluster_matches_baseline(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    r1 = await query_top_dishes_node(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    r2 = await query_top_dishes_node(
        mini_cluster.r2,
        window,
        region_id="r2",
    )
    baseline = await query_top_dishes_node(
        mini_cluster.baseline,
        window,
    )

    live = merge_top_dishes(
        [r1, r2],
        rank_by="units",
        limit=100,
    )
    expected = merge_top_dishes(
        [baseline],
        rank_by="units",
        limit=100,
    )

    assert live == expected

    # Hand-calculated trap: p001 has quantity 1 and multiplier 2.
    d018 = next(row for row in live if row["dish_id"] == "d018")
    assert d018["units"] == 2
    assert d018["quantity"] == 1
    assert d018["sales_units"] == 24

    assert live[0]["dish_id"] == "d017"
    assert live[0]["units"] == 5


async def test_ingredient_strategy_a_matches_baseline(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    r1 = await query_strategy_a_region(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    r2 = await query_strategy_a_region(
        mini_cluster.r2,
        window,
        region_id="r2",
    )
    baseline = await query_strategy_a_region(
        mini_cluster.baseline,
        window,
    )

    live = merge_ingredient_usage([r1, r2])

    assert live == baseline

    assert {
        (
            row["ingredient_id"],
            row["amount"],
            row["unit"],
        )
        for row in live
    } >= {
        ("caramel", 6, "piece"),
        ("beef", 2, "kg"),
        ("fish", 7, "portion"),
    }


async def test_ingredient_strategy_b_matches_baseline(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    r1_lines = await query_strategy_b_lines(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    r2_lines = await query_strategy_b_lines(
        mini_cluster.r2,
        window,
        region_id="r2",
    )

    lines = [*r1_lines, *r2_lines]
    dish_ids = sorted({row["dish_id"] for row in lines})

    # Baseline has the same shared recipe reference data as HQ.
    recipes = await query_recipes(
        mini_cluster.baseline,
        dish_ids,
    )

    live = join_lines_with_recipes(
        lines,
        recipes,
    )

    baseline = await query_strategy_a_region(
        mini_cluster.baseline,
        window,
    )

    assert live == baseline


async def test_ingredient_strategy_c_matches_baseline(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    r1_dishes = await query_strategy_c_dishes(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    r2_dishes = await query_strategy_c_dishes(
        mini_cluster.r2,
        window,
        region_id="r2",
    )

    all_dishes = sorted(set(r1_dishes) | set(r2_dishes))

    recipes = await query_recipes(
        mini_cluster.baseline,
        all_dishes,
    )

    recipes_by_dish: dict[str, list[dict]] = {}

    for recipe in recipes:
        recipes_by_dish.setdefault(
            recipe["dish_id"],
            [],
        ).append(recipe)

    r1_recipes = [recipe for dish_id in r1_dishes for recipe in recipes_by_dish.get(dish_id, [])]
    r2_recipes = [recipe for dish_id in r2_dishes for recipe in recipes_by_dish.get(dish_id, [])]

    r1 = await query_strategy_c_aggregate(
        mini_cluster.r1,
        window,
        r1_recipes,
        region_id="r1",
    )
    r2 = await query_strategy_c_aggregate(
        mini_cluster.r2,
        window,
        r2_recipes,
        region_id="r2",
    )

    live = merge_ingredient_usage([r1, r2])

    baseline = await query_strategy_a_region(
        mini_cluster.baseline,
        window,
    )

    assert live == baseline


async def test_all_ingredient_strategies_return_same_numbers(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    # A
    a1 = await query_strategy_a_region(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    a2 = await query_strategy_a_region(
        mini_cluster.r2,
        window,
        region_id="r2",
    )
    strategy_a = merge_ingredient_usage([a1, a2])

    # B
    b1 = await query_strategy_b_lines(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    b2 = await query_strategy_b_lines(
        mini_cluster.r2,
        window,
        region_id="r2",
    )
    lines = [*b1, *b2]

    dish_ids = sorted({row["dish_id"] for row in lines})
    recipes = await query_recipes(
        mini_cluster.baseline,
        dish_ids,
    )
    strategy_b = join_lines_with_recipes(
        lines,
        recipes,
    )

    # C
    c1_dishes = await query_strategy_c_dishes(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    c2_dishes = await query_strategy_c_dishes(
        mini_cluster.r2,
        window,
        region_id="r2",
    )

    recipes_by_dish: dict[str, list[dict]] = {}
    for recipe in recipes:
        recipes_by_dish.setdefault(
            recipe["dish_id"],
            [],
        ).append(recipe)

    c1_recipes = [recipe for dish_id in c1_dishes for recipe in recipes_by_dish.get(dish_id, [])]
    c2_recipes = [recipe for dish_id in c2_dishes for recipe in recipes_by_dish.get(dish_id, [])]

    c1 = await query_strategy_c_aggregate(
        mini_cluster.r1,
        window,
        c1_recipes,
        region_id="r1",
    )
    c2 = await query_strategy_c_aggregate(
        mini_cluster.r2,
        window,
        c2_recipes,
        region_id="r2",
    )
    strategy_c = merge_ingredient_usage([c1, c2])

    assert strategy_a == strategy_b == strategy_c


async def test_nearby_interior_contacts_one_region(
    mini_cluster,
) -> None:
    result = await nearby_live(
        "w21zt",
        "d018",
        10,
        mini_directory(),
        MiniRegionPools(mini_cluster),
    )

    assert result.meta.nodes_contacted == ["r2-primary"]
    assert result.meta.missing_regions == []

    assert [(row["vendor_id"], row["product_id"]) for row in result.results] == [
        ("v004", "p001"),
    ]


async def test_nearby_border_contacts_two_regions(
    mini_cluster,
) -> None:
    result = await nearby_live(
        "w21z7",
        "d007",
        10,
        mini_directory(),
        MiniRegionPools(mini_cluster),
    )

    assert result.meta.nodes_contacted == [
        "r1-primary",
        "r2-primary",
    ]
    assert result.meta.missing_regions == []

    assert [(row["vendor_id"], row["product_id"]) for row in result.results] == [
        ("v001", "p003"),
    ]


async def test_top_dishes_hq_matches_cluster_and_baseline(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    r1 = await query_top_dishes_node(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    r2 = await query_top_dishes_node(
        mini_cluster.r2,
        window,
        region_id="r2",
    )

    live = merge_top_dishes(
        [r1, r2],
        rank_by="units",
        limit=100,
    )

    baseline_rows = await query_top_dishes_node(
        mini_cluster.baseline,
        window,
    )
    baseline = merge_top_dishes(
        [baseline_rows],
        rank_by="units",
        limit=100,
    )

    hq_rows = await query_top_dishes_node(
        mini_cluster.hq,
        window,
        hq=True,
    )
    hq = merge_top_dishes(
        [hq_rows],
        rank_by="units",
        limit=100,
    )

    assert live == baseline == hq


async def test_ingredient_usage_hq_matches_cluster_and_baseline(
    mini_cluster,
) -> None:
    window = QueryWindow(14, 14)

    r1 = await query_strategy_a_region(
        mini_cluster.r1,
        window,
        region_id="r1",
    )
    r2 = await query_strategy_a_region(
        mini_cluster.r2,
        window,
        region_id="r2",
    )

    live = merge_ingredient_usage([r1, r2])

    baseline = await query_strategy_a_region(
        mini_cluster.baseline,
        window,
    )

    hq = await ingredient_usage_single(
        mini_cluster.hq,
        window,
        mode="hq",
    )

    assert live == baseline == hq.results

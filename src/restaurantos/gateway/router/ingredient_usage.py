"""Distributed ingredient-usage query strategies."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Literal

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.models import QueryMeta
from restaurantos.common.sqlfiles import load_query
from restaurantos.gateway.directory import DirectoryCache
from restaurantos.gateway.pools import RegionPools
from restaurantos.gateway.router.top_dishes import window_params
from restaurantos.gateway.router.window import QueryWindow


@dataclass(frozen=True)
class IngredientUsageResult:
    """Result and metadata for one ingredient-usage query."""

    results: list[dict[str, Any]]
    meta: QueryMeta


async def query_strategy_a_region(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    *,
    region_id: str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate ingredient usage using the region's co-located recipes."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("router/ingredient_usage_a"),
            window_params(window),
        )
        rows = await cursor.fetchall()

        return [
            {
                "ingredient_id": row[0],
                "unit": row[1],
                "amount": row[2],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


def merge_ingredient_usage(
    partials: list[list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Add regional ingredient aggregates."""

    totals: dict[tuple[str, str], int] = {}

    for partial in partials:
        for row in partial:
            key = (
                row["ingredient_id"],
                row["unit"],
            )
            totals[key] = totals.get(key, 0) + row["amount"]

    rows = [
        {
            "ingredient_id": ingredient_id,
            "amount": amount,
            "unit": unit,
        }
        for (ingredient_id, unit), amount in totals.items()
    ]

    rows.sort(
        key=lambda row: (
            -row["amount"],
            row["ingredient_id"],
            row["unit"],
        )
    )

    return rows


async def query_strategy_b_lines(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    *,
    region_id: str | None = None,
) -> list[dict[str, Any]]:
    """Ship every qualifying raw order line to the router."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("router/ingredient_usage_b_lines"),
            window_params(window),
        )
        rows = await cursor.fetchall()

        return [
            {
                "vendor_id": row[0],
                "product_id": row[1],
                "quantity": row[2],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def query_recipes(
    pool: AsyncConnectionPool,
    dish_ids: list[str],
) -> list[dict[str, Any]]:
    """Read recipe rows for exactly the requested dishes."""

    if not dish_ids:
        return []

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("router/ingredient_recipes"),
            {"dish_ids": dish_ids},
        )
        rows = await cursor.fetchall()

        return [
            {
                "dish_id": row[0],
                "ingredient_id": row[1],
                "amount": row[2],
                "unit": row[3],
            }
            for row in rows
        ]

    return await run_on(pool, work)


def join_lines_with_recipes(
    lines: list[dict[str, Any]],
    recipes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Join shipped order lines with HQ product links and recipes."""

    by_product: dict[
        tuple[str, str],
        list[dict[str, Any]],
    ] = {}

    for recipe in recipes:
        key = (
            recipe["vendor_id"],
            recipe["product_id"],
        )
        by_product.setdefault(key, []).append(recipe)

    totals: dict[tuple[str, str], int] = {}

    for line in lines:
        product_key = (
            line["vendor_id"],
            line["product_id"],
        )

        for recipe in by_product.get(product_key, []):
            key = (
                recipe["ingredient_id"],
                recipe["unit"],
            )

            used = line["quantity"] * recipe["multiplier"] * recipe["amount"]

            totals[key] = totals.get(key, 0) + used

    rows = [
        {
            "ingredient_id": ingredient_id,
            "amount": amount,
            "unit": unit,
        }
        for (ingredient_id, unit), amount in totals.items()
    ]

    rows.sort(
        key=lambda row: (
            -row["amount"],
            row["ingredient_id"],
            row["unit"],
        )
    )

    return rows


async def ingredient_usage_b_live(
    window: QueryWindow,
    *,
    directory: DirectoryCache,
    pools: RegionPools,
    hq_pool,
) -> IngredientUsageResult:
    """Strategy B: ship qualifying order lines and join recipes at the router."""

    started = perf_counter()

    async def query_one(
        region_id: str,
        primary_node: str,
    ) -> tuple[str, str, list[dict[str, Any]] | None]:
        try:
            pool = await pools.get(primary_node)
            rows = await query_strategy_b_lines(
                pool,
                window,
                region_id=region_id,
            )
            return region_id, primary_node, rows
        except RegionUnreachableError:
            return region_id, primary_node, None

    partials = await asyncio.gather(
        *(
            query_one(region_id, route.primary_node)
            for region_id, route in directory.region_routes.items()
        )
    )

    lines: list[dict[str, Any]] = []
    nodes_contacted: list[str] = []
    missing_regions: list[str] = []

    for region_id, node, rows in partials:
        nodes_contacted.append(node)

        if rows is None:
            missing_regions.append(region_id)
            continue

        lines.extend(rows)

    recipes = await query_product_recipes(
        hq_pool,
        lines,
    )

    results = join_lines_with_recipes(
        lines,
        recipes,
    )

    return IngredientUsageResult(
        results=results,
        meta=QueryMeta(
            mode="live",
            nodes_contacted=sorted(nodes_contacted),
            missing_regions=sorted(missing_regions),
            rows_transferred=len(lines),
            data_as_of=datetime.now(UTC),
            elapsed_ms=(perf_counter() - started) * 1000,
        ),
    )


async def query_strategy_c_dishes(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    *,
    region_id: str | None = None,
) -> list[str]:
    """Round 1: return distinct dishes sold in the requested window."""

    async def work(conn) -> list[str]:
        cursor = await conn.execute(
            load_query("router/ingredient_usage_c_dishes"),
            window_params(window),
        )
        rows = await cursor.fetchall()

        return [row[0] for row in rows]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def query_strategy_c_aggregate(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    recipes: list[dict[str, Any]],
    *,
    region_id: str | None = None,
) -> list[dict[str, Any]]:
    """Round 2: aggregate using recipes shipped to the region."""

    if not recipes:
        return []

    params = window_params(window)
    params.update(
        {
            "dish_ids": [row["dish_id"] for row in recipes],
            "ingredient_ids": [row["ingredient_id"] for row in recipes],
            "amounts": [row["amount"] for row in recipes],
            "units": [row["unit"] for row in recipes],
        }
    )

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("router/ingredient_usage_c_aggregate"),
            params,
        )
        rows = await cursor.fetchall()

        return [
            {
                "ingredient_id": row[0],
                "amount": row[2],
                "unit": row[1],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def ingredient_usage_c_live(
    window: QueryWindow,
    *,
    directory: DirectoryCache,
    pools: RegionPools,
    hq_pool,
) -> IngredientUsageResult:
    """Strategy C: semi-join recipes through the router."""

    started = perf_counter()

    async def round_one(
        region_id: str,
        primary_node: str,
    ) -> tuple[str, str, list[str] | None]:
        try:
            pool = await pools.get(primary_node)
            dish_ids = await query_strategy_c_dishes(
                pool,
                window,
                region_id=region_id,
            )
            return region_id, primary_node, dish_ids
        except RegionUnreachableError:
            return region_id, primary_node, None

    first_round = await asyncio.gather(
        *(
            round_one(region_id, route.primary_node)
            for region_id, route in directory.region_routes.items()
        )
    )

    nodes_contacted: list[str] = []
    missing_regions: list[str] = []
    dishes_by_region: dict[str, list[str]] = {}
    nodes_by_region: dict[str, str] = {}
    rows_transferred = 0

    all_dish_ids: set[str] = set()

    for region_id, node, dish_ids in first_round:
        nodes_contacted.append(node)

        if dish_ids is None:
            missing_regions.append(region_id)
            continue

        dishes_by_region[region_id] = dish_ids
        nodes_by_region[region_id] = node
        rows_transferred += len(dish_ids)
        all_dish_ids.update(dish_ids)

    recipes = await query_recipes(
        hq_pool,
        sorted(all_dish_ids),
    )

    recipes_by_dish: dict[str, list[dict[str, Any]]] = {}

    for recipe in recipes:
        recipes_by_dish.setdefault(
            recipe["dish_id"],
            [],
        ).append(recipe)

    async def round_two(
        region_id: str,
        node: str,
        dish_ids: list[str],
    ) -> tuple[str, list[dict[str, Any]] | None, int]:
        region_recipes = [
            recipe for dish_id in dish_ids for recipe in recipes_by_dish.get(dish_id, [])
        ]

        try:
            pool = await pools.get(node)
            rows = await query_strategy_c_aggregate(
                pool,
                window,
                region_recipes,
                region_id=region_id,
            )
            return region_id, rows, len(region_recipes)
        except RegionUnreachableError:
            return region_id, None, len(region_recipes)

    second_round = await asyncio.gather(
        *(
            round_two(
                region_id,
                nodes_by_region[region_id],
                dish_ids,
            )
            for region_id, dish_ids in dishes_by_region.items()
        )
    )

    partials: list[list[dict[str, Any]]] = []

    for region_id, rows, recipe_count in second_round:
        # Recipe rows crossed the region link on the way down,
        # even if the region failed while processing them.
        rows_transferred += recipe_count

        if rows is None:
            if region_id not in missing_regions:
                missing_regions.append(region_id)
            continue

        # Aggregated rows cross the region link on the way back.
        rows_transferred += len(rows)
        partials.append(rows)

    return IngredientUsageResult(
        results=merge_ingredient_usage(partials),
        meta=QueryMeta(
            mode="live",
            nodes_contacted=sorted(nodes_contacted),
            missing_regions=sorted(missing_regions),
            rows_transferred=rows_transferred,
            data_as_of=datetime.now(UTC),
            elapsed_ms=(perf_counter() - started) * 1000,
        ),
    )


async def ingredient_usage_a_live(
    window: QueryWindow,
    *,
    directory: DirectoryCache,
    pools: RegionPools,
) -> IngredientUsageResult:
    """Strategy A: aggregate against co-located recipes in every region."""

    started = perf_counter()

    async def query_one(
        region_id: str,
        primary_node: str,
    ) -> tuple[str, str, list[dict[str, Any]] | None]:
        try:
            pool = await pools.get(primary_node)
            rows = await query_strategy_a_region(
                pool,
                window,
                region_id=region_id,
            )
            return region_id, primary_node, rows
        except RegionUnreachableError:
            return region_id, primary_node, None

    scattered = await asyncio.gather(
        *(
            query_one(region_id, route.primary_node)
            for region_id, route in directory.region_routes.items()
        )
    )

    partials: list[list[dict[str, Any]]] = []
    nodes_contacted: list[str] = []
    missing_regions: list[str] = []
    rows_transferred = 0

    for region_id, node, rows in scattered:
        nodes_contacted.append(node)

        if rows is None:
            missing_regions.append(region_id)
            continue

        partials.append(rows)
        rows_transferred += len(rows)

    return IngredientUsageResult(
        results=merge_ingredient_usage(partials),
        meta=QueryMeta(
            mode="live",
            nodes_contacted=sorted(nodes_contacted),
            missing_regions=sorted(missing_regions),
            rows_transferred=rows_transferred,
            data_as_of=datetime.now(UTC),
            elapsed_ms=(perf_counter() - started) * 1000,
        ),
    )


async def ingredient_usage_single(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    *,
    mode: Literal["hq", "baseline"],
) -> IngredientUsageResult:
    """Run ingredient usage on HQ analytics or the baseline node."""

    started = perf_counter()

    query_name = "router/ingredient_usage_hq" if mode == "hq" else "router/ingredient_usage_a"

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query(query_name),
            window_params(window),
        )
        rows = await cursor.fetchall()

        return [
            {
                "ingredient_id": row[0],
                "amount": row[2],
                "unit": row[1],
            }
            for row in rows
        ]

    rows = await run_on(pool, work)

    return IngredientUsageResult(
        results=rows,
        meta=QueryMeta(
            mode=mode,
            nodes_contacted=["hq-primary" if mode == "hq" else "baseline"],
            missing_regions=[],
            rows_transferred=0,
            data_as_of=datetime.now(UTC),
            elapsed_ms=(perf_counter() - started) * 1000,
        ),
    )


async def query_product_recipes(
    pool: AsyncConnectionPool,
    lines: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Read HQ product links and recipes for shipped order lines."""

    keys = sorted(
        {
            (
                row["vendor_id"],
                row["product_id"],
            )
            for row in lines
        }
    )

    if not keys:
        return []

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("router/ingredient_product_recipes"),
            {
                "vendor_ids": [vendor_id for vendor_id, _ in keys],
                "product_ids": [product_id for _, product_id in keys],
            },
        )
        rows = await cursor.fetchall()

        return [
            {
                "vendor_id": row[0],
                "product_id": row[1],
                "multiplier": row[2],
                "ingredient_id": row[3],
                "amount": row[4],
                "unit": row[5],
            }
            for row in rows
        ]

    return await run_on(pool, work)

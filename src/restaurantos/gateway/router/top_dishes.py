"""City-wide top-dishes distributed query."""

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
from restaurantos.gateway.router.window import QueryWindow

RankBy = Literal[
    "units",
    "quantity",
    "order_lines",
    "sales_units",
]

RANK_FIELDS: frozenset[str] = frozenset(
    {
        "units",
        "quantity",
        "order_lines",
        "sales_units",
    }
)


@dataclass(frozen=True)
class TopDishesResult:
    """Result and metadata for one top-dishes query."""

    results: list[dict[str, Any]]
    meta: QueryMeta


def window_params(window: QueryWindow) -> dict[str, object]:
    """Parameters shared by every analytics query."""

    return {
        "from_day": window.from_day,
        "to_day": window.to_day,
        "from_time": window.from_time,
        "to_time": window.to_time,
        "has_time_window": window.has_time_window,
        "crosses_midnight": window.crosses_midnight,
    }


async def query_top_dishes_node(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    *,
    region_id: str | None = None,
    hq: bool = False,
) -> list[dict[str, Any]]:
    """Return every dish aggregate from one region, baseline, or HQ."""

    query_name = "router/top_dishes_hq" if hq else "router/top_dishes"

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query(query_name),
            window_params(window),
        )
        rows = await cursor.fetchall()

        return [
            {
                "dish_id": row[0],
                "dish_name": row[1],
                "example_name": row[2],
                "units": row[3],
                "quantity": row[4],
                "order_lines": row[5],
                "sales_units": row[6],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


def merge_top_dishes(
    partials: list[list[dict[str, Any]]],
    *,
    rank_by: RankBy,
    limit: int,
) -> list[dict[str, Any]]:
    """Add regional aggregates, globally rank them, and cut to limit."""

    totals: dict[str, dict[str, Any]] = {}

    for partial in partials:
        for row in partial:
            dish_id = row["dish_id"]

            if dish_id not in totals:
                totals[dish_id] = {
                    "dish_id": dish_id,
                    "dish_name": row["dish_name"],
                    "example_name": row["example_name"],
                    "units": 0,
                    "quantity": 0,
                    "order_lines": 0,
                    "sales_units": 0,
                }

            total = totals[dish_id]
            total["units"] += row["units"]
            total["quantity"] += row["quantity"]
            total["order_lines"] += row["order_lines"]
            total["sales_units"] += row["sales_units"]

    rows = list(totals.values())

    rows.sort(
        key=lambda row: (
            -row[rank_by],
            row["dish_id"],
        )
    )

    return rows[:limit]


async def top_dishes_live(
    window: QueryWindow,
    *,
    rank_by: RankBy,
    limit: int,
    directory: DirectoryCache,
    pools: RegionPools,
) -> TopDishesResult:
    """Scatter to every region and merge complete dish aggregates."""

    started = perf_counter()

    async def query_one(
        region_id: str,
        primary_node: str,
    ) -> tuple[str, str, list[dict[str, Any]] | None]:
        try:
            pool = await pools.get(primary_node)
            rows = await query_top_dishes_node(
                pool,
                window,
                region_id=region_id,
            )
            return region_id, primary_node, rows
        except RegionUnreachableError:
            return region_id, primary_node, None

    partials = await asyncio.gather(
        *(
            query_one(
                region_id,
                route.primary_node,
            )
            for region_id, route in directory.region_routes.items()
        )
    )

    successful: list[list[dict[str, Any]]] = []
    nodes_contacted: list[str] = []
    missing_regions: list[str] = []
    rows_transferred = 0

    for region_id, node, rows in partials:
        nodes_contacted.append(node)

        if rows is None:
            missing_regions.append(region_id)
            continue

        successful.append(rows)
        rows_transferred += len(rows)

    results = merge_top_dishes(
        successful,
        rank_by=rank_by,
        limit=limit,
    )

    return TopDishesResult(
        results=results,
        meta=QueryMeta(
            mode="live",
            nodes_contacted=sorted(nodes_contacted),
            missing_regions=sorted(missing_regions),
            rows_transferred=rows_transferred,
            data_as_of=datetime.now(UTC),
            elapsed_ms=(perf_counter() - started) * 1000,
        ),
    )


async def top_dishes_single(
    pool: AsyncConnectionPool,
    window: QueryWindow,
    *,
    mode: Literal["hq", "baseline"],
    rank_by: RankBy,
    limit: int,
) -> TopDishesResult:
    """Run top dishes on HQ analytics or the single-node baseline."""

    started = perf_counter()

    rows = await query_top_dishes_node(
        pool,
        window,
        hq=mode == "hq",
    )

    results = merge_top_dishes(
        [rows],
        rank_by=rank_by,
        limit=limit,
    )

    return TopDishesResult(
        results=results,
        meta=QueryMeta(
            mode=mode,
            nodes_contacted=["hq-primary" if mode == "hq" else "baseline"],
            missing_regions=[],
            rows_transferred=0,
            data_as_of=datetime.now(UTC),
            elapsed_ms=(perf_counter() - started) * 1000,
        ),
    )

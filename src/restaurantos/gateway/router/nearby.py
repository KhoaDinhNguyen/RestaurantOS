"""Nearby distributed-query routing."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

import pygeohash as pgh
from psycopg_pool import AsyncConnectionPool
from pygeohash.neighbor import get_adjacent

from restaurantos.common.db import run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.models import QueryMeta
from restaurantos.common.sqlfiles import load_query
from restaurantos.gateway.directory import DirectoryCache, RegionRoute
from restaurantos.gateway.pools import RegionPools


@dataclass(frozen=True)
class NearbyCell:
    """One searched geohash cell and its distance from the customer."""

    geohash: str
    distance_m: float


@dataclass(frozen=True)
class NearbyResult:
    """Merged result of one live nearby query."""

    results: list[dict[str, Any]]
    meta: QueryMeta


def nearby_cells(geohash: str) -> list[NearbyCell]:
    """Return the customer cell and its eight neighbours with distances."""

    north = get_adjacent(geohash, "top")
    south = get_adjacent(geohash, "bottom")
    east = get_adjacent(geohash, "right")
    west = get_adjacent(geohash, "left")

    cells = [
        geohash,
        north,
        south,
        east,
        west,
        get_adjacent(north, "left"),
        get_adjacent(north, "right"),
        get_adjacent(south, "left"),
        get_adjacent(south, "right"),
    ]

    return [
        NearbyCell(
            geohash=cell,
            distance_m=(
                0.0 if cell == geohash else float(pgh.geohash_haversine_distance(geohash, cell))
            ),
        )
        for cell in cells
    ]


def cells_by_region(
    cells: list[NearbyCell],
    directory: DirectoryCache,
) -> dict[str, tuple[RegionRoute, list[NearbyCell]]]:
    """Group known cells by owning region; silently skip unknown cells."""

    grouped: dict[str, tuple[RegionRoute, list[NearbyCell]]] = {}

    for cell in cells:
        route = directory.cell_routes.get(cell.geohash)

        if route is None:
            continue

        existing = grouped.get(route.region_id)

        if existing is None:
            grouped[route.region_id] = (route, [cell])
        else:
            existing[1].append(cell)

    return grouped


async def query_region(
    pool: AsyncConnectionPool,
    region_id: str,
    cells: list[NearbyCell],
    dish_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Return one region's nearby candidates in global sort order."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("router/nearby"),
            {
                "geohashes": [cell.geohash for cell in cells],
                "distances_m": [cell.distance_m for cell in cells],
                "dish_id": dish_id,
                "row_limit": limit,
            },
        )
        rows = await cursor.fetchall()

        return [
            {
                "vendor_id": row[0],
                "chain_id": row[1],
                "primary_cuisine": row[2],
                "geohash": row[3],
                "distance_m": row[4],
                "product_id": row[5],
                "name": row[6],
                "price_units": row[7],
                "multiplier": row[8],
                "region_id": region_id,
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def nearby_live(
    geohash: str,
    dish_id: str,
    limit: int,
    directory: DirectoryCache,
    pools: RegionPools,
) -> NearbyResult:
    """Scatter a nearby query to owning regions and merge partial results."""

    started = perf_counter()

    cells = nearby_cells(geohash)
    grouped = cells_by_region(cells, directory)

    async def query_one(
        region_id: str,
        route: RegionRoute,
        region_cells: list[NearbyCell],
    ) -> tuple[str, str, list[dict[str, Any]] | None]:
        try:
            pool = await pools.get(route.primary_node)
            rows = await query_region(
                pool,
                region_id,
                region_cells,
                dish_id,
                limit,
            )
            return region_id, route.primary_node, rows
        except RegionUnreachableError:
            return region_id, route.primary_node, None

    partials = await asyncio.gather(
        *(
            query_one(region_id, route, region_cells)
            for region_id, (route, region_cells) in grouped.items()
        )
    )

    rows: list[dict[str, Any]] = []
    nodes_contacted: list[str] = []
    missing_regions: list[str] = []

    for region_id, node, partial in partials:
        nodes_contacted.append(node)

        if partial is None:
            missing_regions.append(region_id)
            continue

        rows.extend(partial)

    rows_transferred = len(rows)

    rows.sort(
        key=lambda row: (
            row["distance_m"],
            row["vendor_id"],
            row["product_id"],
        )
    )

    results = rows[:limit]

    meta = QueryMeta(
        mode="live",
        nodes_contacted=sorted(nodes_contacted),
        missing_regions=sorted(missing_regions),
        rows_transferred=rows_transferred,
        data_as_of=datetime.now(UTC),
        elapsed_ms=(perf_counter() - started) * 1000,
    )

    return NearbyResult(
        results=results,
        meta=meta,
    )

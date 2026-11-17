"""Dish reference-data reads for the gateway and edge API."""

from __future__ import annotations

from typing import Any

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.sqlfiles import load_query
from restaurantos.gateway.vendors import search_pattern


async def search_dishes(
    pool: AsyncConnectionPool,
    query: str,
    limit: int,
    region_id: str | None = None,
) -> list[dict[str, Any]]:
    """Search dishes by dish or example name."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("gateway/search_dishes"),
            {
                "pattern": search_pattern(query),
                "row_limit": limit,
            },
        )
        rows = await cursor.fetchall()

        return [
            {
                "dish_id": row[0],
                "dish_name": row[1],
                "example_name": row[2],
                "scope": row[3],
                "n_vendors": row[4],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def get_dish(
    pool: AsyncConnectionPool,
    dish_id: str,
    region_id: str | None = None,
) -> dict[str, Any] | None:
    """Read one dish by id."""

    async def work(conn) -> dict[str, Any] | None:
        cursor = await conn.execute(
            load_query("gateway/get_dish"),
            {"dish_id": dish_id},
        )
        row = await cursor.fetchone()

        if row is None:
            return None

        return {
            "dish_id": row[0],
            "dish_name": row[1],
            "example_name": row[2],
            "scope": row[3],
            "n_vendors": row[4],
        }

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

"""Regional vendor search for the gateway and edge API."""

from __future__ import annotations

from typing import Any

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.sqlfiles import load_query


def search_pattern(query: str | None) -> str | None:
    """Build an escaped substring pattern for SQL LIKE matching."""
    if query is None:
        return None

    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    return f"%{escaped}%"


async def search_vendors(
    pool: AsyncConnectionPool,
    geohash: str,
    query: str | None,
    region_id: str,
) -> list[dict[str, Any]]:
    """Search vendors in exactly one geohash cell."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("gateway/search_vendors"),
            {
                "geohash": geohash,
                "pattern": search_pattern(query),
            },
        )
        rows = await cursor.fetchall()

        return [
            {
                "vendor_id": row[0],
                "chain_id": row[1],
                "geohash": row[2],
                "primary_cuisine": row[3],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

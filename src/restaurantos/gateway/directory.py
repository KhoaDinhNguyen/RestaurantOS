"""Shard-directory access for the gateway."""

from __future__ import annotations

from typing import Any

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.sqlfiles import load_query


async def get_regions(pool: AsyncConnectionPool) -> list[dict[str, Any]]:
    """Read the shard directory and region state from HQ."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(load_query("gateway/get_regions"))
        rows = await cursor.fetchall()

        return [
            {
                "region_id": row[0],
                "epoch": row[1],
                "primary_node": row[2],
                "standby_node": row[3],
                "menu_version": row[4],
                "updated_at": row[5],
                "geohashes": row[6],
            }
            for row in rows
        ]

    return await run_on(pool, work)

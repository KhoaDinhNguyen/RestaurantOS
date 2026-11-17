"""Regional summary reads for the gateway."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.sqlfiles import load_query


async def get_region_summary(
    pool: AsyncConnectionPool,
    region_id: str,
    day: int,
) -> dict[str, Any]:
    """Read one region's operational summary from its primary."""

    async def work(conn: psycopg.AsyncConnection) -> dict[str, Any]:
        cursor = await conn.execute(
            load_query("gateway/get_region_summary"),
            {"day": day},
        )
        row = await cursor.fetchone()

        return {
            "region_id": row[0],
            "day": row[14],
            "epoch": row[1],
            "menu_version": row[2],
            "vendors": row[15],
            "orders": {
                "total": row[3],
                "received": row[4],
                "accepted": row[5],
                "preparing": row[6],
                "ready": row[7],
                "picked_up": row[8],
                "canceled": row[9],
            },
            "refused": row[10],
            "sales_units": row[11],
            "products_unavailable": row[12],
            "outbox_backlog": row[13],
            "data_as_of": row[16],
        }

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

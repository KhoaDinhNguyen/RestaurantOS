"""Regional menu reads for the gateway."""

from __future__ import annotations

from typing import Any

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.errors import UnknownVendorError
from restaurantos.common.sqlfiles import load_query


async def get_vendor_menu(
    pool: AsyncConnectionPool,
    vendor_id: str,
    region_id: str,
) -> list[dict[str, Any]]:
    """Read one vendor's menu from its owning region."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(
            load_query("gateway/get_vendor_menu"),
            {"vendor_id": vendor_id},
        )
        rows = await cursor.fetchall()

        if not rows:
            raise UnknownVendorError(f"unknown vendor {vendor_id}")

        return [
            {
                "vendor_id": row[0],
                "product_id": row[1],
                "name": row[2],
                "price_units": row[3],
                "menu_version": row[4],
                "dish_id": row[5],
                "reason": row[6],
                "available": row[7],
            }
            for row in rows
        ]

    return await run_on(pool, work, region_id=region_id)

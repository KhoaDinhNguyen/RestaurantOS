"""Regional stock reads for the gateway and edge API."""

from __future__ import annotations

from typing import Any

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.errors import UnknownVendorError
from restaurantos.common.sqlfiles import load_query


async def get_vendor_stock(
    pool: AsyncConnectionPool,
    vendor_id: str,
    region_id: str,
) -> list[dict[str, Any]]:
    """Read one vendor's stock counters from its owning region."""

    async def work(conn) -> list[dict[str, Any]]:
        vendor_cursor = await conn.execute(
            "SELECT 1 FROM vendors WHERE vendor_id = %(vendor_id)s",
            {"vendor_id": vendor_id},
        )

        if await vendor_cursor.fetchone() is None:
            raise UnknownVendorError(f"unknown vendor {vendor_id}")

        cursor = await conn.execute(
            load_query("gateway/get_vendor_stock"),
            {"vendor_id": vendor_id},
        )
        rows = await cursor.fetchall()

        return [
            {
                "item_kind": row[0],
                "item_id": row[1],
                "name": row[5],
                "unit": row[6],
                "on_hand": row[2],
                "par_level": row[3],
                "updated_at": row[4],
            }
            for row in rows
        ]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

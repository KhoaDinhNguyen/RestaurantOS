"""Regional vendor restock writes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import psycopg
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.region import RestockResult

Restock = Callable[..., Awaitable[RestockResult]]


async def restock_region_vendor(
    pool: AsyncConnectionPool,
    vendor_id: str,
    region_id: str,
    epoch: int,
    restock: Restock,
) -> RestockResult:
    """Run WP1's no-body restock transaction on the owning region."""

    async def work(conn: psycopg.AsyncConnection) -> RestockResult:
        return await restock(
            conn,
            vendor_id,
            epoch,
        )

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

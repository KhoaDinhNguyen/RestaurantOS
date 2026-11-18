"""Regional product availability writes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import psycopg
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.region import ProductState

SetAvailability = Callable[..., Awaitable[ProductState]]


async def set_region_availability(
    pool: AsyncConnectionPool,
    vendor_id: str,
    product_id: str,
    available: bool,
    region_id: str,
    epoch: int,
    set_availability: SetAvailability,
) -> ProductState:
    """Run WP1's availability transaction on the owning region."""

    async def work(conn: psycopg.AsyncConnection) -> ProductState:
        return await set_availability(
            conn,
            vendor_id,
            product_id,
            available,
            epoch,
        )

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

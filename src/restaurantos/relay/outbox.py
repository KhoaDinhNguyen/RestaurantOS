"""Read the region's pending events in the shared HQ envelope."""

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.config import Settings
from restaurantos.common.db import run_on
from restaurantos.common.models import OutboxEvent
from restaurantos.common.sqlfiles import load_query


async def read_pending(
    pool: AsyncConnectionPool,
    region_id: str,
    batch_size: int,
    *,
    settings: Settings,
) -> list[OutboxEvent]:
    """Return one ordered batch. Keep it pending until HQ confirms it."""

    async def read(conn: AsyncConnection) -> list[OutboxEvent]:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(load_query("relay/read_pending"), {"batch_size": batch_size})
            return [OutboxEvent(region_id=region_id, **row) for row in await cursor.fetchall()]

    return await run_on(pool, read, region_id=region_id, settings=settings)

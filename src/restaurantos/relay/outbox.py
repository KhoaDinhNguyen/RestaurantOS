"""Read and acknowledge a region's outbox events."""

from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.config import Settings
from restaurantos.common.db import run_on
from restaurantos.common.errors import RegionUnreachableError
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


async def read_epoch(pool: AsyncConnectionPool, region_id: str, *, settings: Settings) -> int:
    """Capture the local epoch before reading a batch."""

    async def read(conn: AsyncConnection) -> int:
        cursor = await conn.execute(load_query("relay/read_epoch"))
        row = await cursor.fetchone()
        if row is None:
            raise RegionUnreachableError("no local epoch", region_id=region_id)
        return row[0]

    return await run_on(pool, read, region_id=region_id, settings=settings)


async def mark_relayed(
    pool: AsyncConnectionPool,
    region_id: str,
    epoch: int,
    event_ids: list[UUID],
    *,
    settings: Settings,
) -> None:
    """Acknowledge the exact events HQ committed, under the original epoch."""

    async def mark(conn: AsyncConnection) -> None:
        await conn.execute(load_query("relay/check_epoch"), {"epoch": epoch})
        await conn.execute(load_query("relay/mark_relayed"), {"event_ids": event_ids})

    await run_on(pool, mark, region_id=region_id, settings=settings)

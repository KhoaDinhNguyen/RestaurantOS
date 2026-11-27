"""Publish the newest affected HQ menu slice to a region."""

from collections.abc import Awaitable, Callable

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.common.sqlfiles import load_query

ApplyMenuVersion = Callable[..., Awaitable[None]]


async def publish_menu(
    hq_pool: AsyncConnectionPool,
    cluster: Cluster,
    region_id: str,
    settings: Settings,
    apply_menu_version: ApplyMenuVersion,
) -> None:
    """Check regional state, apply a coherent HQ slice, then record delivery."""

    async def target(conn: AsyncConnection):
        cursor = await conn.execute(load_query("relay/region_target"), {"region_id": region_id})
        row = await cursor.fetchone()
        if row is None:
            raise RegionUnreachableError("no HQ state for region", region_id=region_id)
        return row

    async def local_version(conn: AsyncConnection) -> int:
        cursor = await conn.execute(load_query("relay/read_menu_version"))
        row = await cursor.fetchone()
        if row is None:
            raise RegionUnreachableError("no regional menu state", region_id=region_id)
        return row[0]

    async def newest_version(conn: AsyncConnection) -> int | None:
        cursor = await conn.execute(
            load_query("relay/newest_menu_version"), {"region_id": region_id}
        )
        return (await cursor.fetchone())[0]

    async def snapshot(conn: AsyncConnection):
        # One statement selects the delta and its newest version from the same
        # PostgreSQL snapshot. Keep numeric prices as Decimal, without JSON.
        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                load_query("relay/read_menu_slice"),
                {"region_id": region_id, "after_version": current},
            )
            return await cursor.fetchall()

    for attempt in range(2):
        node, epoch = await run_on(hq_pool, target, region_id=region_id, settings=settings)
        conninfo = node_conninfo(cluster, node, settings=settings)
        async with make_pool(conninfo, settings) as region_pool:
            current = await run_on(
                region_pool, local_version, region_id=region_id, settings=settings
            )
            newest = await run_on(hq_pool, newest_version, region_id=region_id, settings=settings)
            if newest is None:
                return

            if newest > current:
                rows = await run_on(hq_pool, snapshot, region_id=region_id, settings=settings)
                if not rows:
                    return
                version = rows[0]["publish_version"]
                for row in rows:
                    del row["publish_version"]

                async def apply(conn, version=version, rows=rows, epoch=epoch):
                    await apply_menu_version(conn, version, rows, epoch)
                    return await local_version(conn)

                try:
                    current = await run_on(
                        region_pool, apply, region_id=region_id, settings=settings
                    )
                except StaleEpochError:
                    if attempt == 1:
                        raise
                    continue

            async def acknowledge(conn, current=current, node=node, epoch=epoch):
                await conn.execute(
                    load_query("relay/record_menu_version"),
                    {"region_id": region_id, "node": node, "epoch": epoch, "version": current},
                )

            # A failed HQ acknowledgement is repaired next poll from the
            # region's own version. A promoted, behind standby gets a resend.
            await run_on(hq_pool, acknowledge, region_id=region_id, settings=settings)
            return

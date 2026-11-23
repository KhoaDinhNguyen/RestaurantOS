"""Deliver HQ's parked orders through WP1's regional order transaction."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.common.hq import HQPool
from restaurantos.common.models import OrderRequest, OrderResult
from restaurantos.common.sqlfiles import load_query
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.menu import ApplyMenuVersion, publish_menu

PlaceOrder = Callable[..., Awaitable[OrderResult]]
logger = logging.getLogger(__name__)


async def drain_batch(
    hq_pool: AsyncConnectionPool,
    cluster: Cluster,
    region_id: str,
    settings: Settings,
    relay: RelaySettings,
    place_order: PlaceOrder,
) -> int:
    """Deliver one ordered batch, acknowledging each committed regional result."""

    async def read(conn: AsyncConnection):
        cursor = await conn.execute(
            load_query("relay/read_holding"),
            {"region_id": region_id, "batch_size": relay.batch_size},
        )
        return await cursor.fetchall()

    async def target(conn: AsyncConnection):
        cursor = await conn.execute(load_query("relay/region_target"), {"region_id": region_id})
        row = await cursor.fetchone()
        if row is None:
            raise RegionUnreachableError("no HQ state for region", region_id=region_id)
        return row

    rows = await run_on(hq_pool, read, region_id=region_id, settings=settings)
    async with AsyncExitStack() as stack:
        pools: dict[str, AsyncConnectionPool] = {}
        for holding_id, body, received_at in rows:
            request = OrderRequest.model_validate(body)
            for attempt in range(2):
                # Re-read the coordinator's target for every order and retry:
                # promotion may make the node named "-standby" the primary.
                node, epoch = await run_on(hq_pool, target, region_id=region_id, settings=settings)
                if node not in pools:
                    conninfo = node_conninfo(cluster, node, settings=settings)
                    pools[node] = await stack.enter_async_context(make_pool(conninfo, settings))

                # Bind values so an abandoned call keeps its original request
                # and epoch even while the worker moves to its next attempt.
                async def place(conn, request=request, epoch=epoch, received_at=received_at):
                    return await place_order(conn, request, epoch, received_at=received_at)

                try:
                    result = await run_on(
                        pools[node], place, region_id=region_id, settings=settings
                    )
                    break
                except StaleEpochError:
                    if attempt == 1:
                        raise

            async def acknowledge(conn, holding_id=holding_id, outcome=result.outcome):
                await conn.execute(
                    load_query("relay/mark_delivered"),
                    {"holding_id": holding_id, "region_id": region_id, "outcome": outcome},
                )

            # Regional commit precedes this HQ update. A failure here leaves
            # the row pending; WP1's inbox makes the subsequent resend safe.
            await run_on(hq_pool, acknowledge, region_id=region_id, settings=settings)

    return len(rows)


async def run_hq_worker(
    cluster: Cluster,
    settings: Settings,
    relay: RelaySettings,
    place_order: PlaceOrder,
    apply_menu_version: ApplyMenuVersion,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Drain orders and publish menus independently for every region."""

    async def drain_region(region_id: str) -> None:
        async with HQPool(cluster, settings, direct=True) as hq_pool:
            while stop_event is None or not stop_event.is_set():
                try:
                    await publish_menu(hq_pool, cluster, region_id, settings, apply_menu_version)
                except (RegionUnreachableError, StaleEpochError) as exc:
                    logger.warning("Menu publisher %s will retry: %s", region_id, exc)
                try:
                    count = await drain_batch(
                        hq_pool, cluster, region_id, settings, relay, place_order
                    )
                    if count == relay.batch_size:
                        continue
                except (RegionUnreachableError, StaleEpochError) as exc:
                    logger.warning("Holding queue %s will retry pending orders: %s", region_id, exc)
                await asyncio.sleep(relay.poll_interval_s)

    await asyncio.gather(*(drain_region(region_id) for region_id in cluster.region_ids))

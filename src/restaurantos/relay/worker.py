"""Poll the local outbox and deliver pending events to HQ."""

import asyncio
import logging

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.cluster import HQ_PRIMARY, Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import find_local_primary, make_pool, node_conninfo
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.hq_apply import apply_batch
from restaurantos.relay.outbox import mark_relayed, read_epoch, read_pending

logger = logging.getLogger(__name__)


async def run_region_relay(
    cluster: Cluster, region_id: str, settings: Settings, relay: RelaySettings
) -> None:
    loop = asyncio.get_running_loop()
    current = None
    region_pool: AsyncConnectionPool | None = None
    next_check = next_poll = loop.time()

    async def refresh_primary() -> None:
        nonlocal current, region_pool, next_check
        if loop.time() < next_check:
            return

        node = await find_local_primary(cluster, region_id, current, settings=settings)
        next_check = loop.time() + relay.local_check_s
        if node == current:
            return
        if region_pool is not None:
            await region_pool.close()
            region_pool = None
        current = node
        if node is not None:
            conninfo = node_conninfo(cluster, node, direct=True, settings=settings)
            region_pool = make_pool(conninfo, settings)
            await region_pool.open()

    hq_conninfo = node_conninfo(cluster, HQ_PRIMARY, settings=settings)
    try:
        async with make_pool(hq_conninfo, settings) as hq_pool:
            while True:
                await refresh_primary()
                pool = region_pool
                if pool is not None and loop.time() >= next_poll:
                    try:
                        epoch = await read_epoch(pool, region_id, settings=settings)
                        events = await read_pending(
                            pool, region_id, relay.batch_size, settings=settings
                        )
                        if events:
                            await apply_batch(hq_pool, events, settings=settings)
                            await refresh_primary()
                            if region_pool is not pool:
                                continue
                            await mark_relayed(
                                pool,
                                region_id,
                                epoch,
                                [e.event_id for e in events],
                                settings=settings,
                            )
                        if len(events) == relay.batch_size:
                            continue
                    except (RegionUnreachableError, StaleEpochError) as exc:
                        logger.warning("Relay %s will retry pending events: %s", region_id, exc)
                        next_check = loop.time()
                    next_poll = loop.time() + relay.poll_interval_s

                wake = min(next_check, next_poll) if region_pool is not None else next_check
                await asyncio.sleep(max(0, wake - loop.time()))
    finally:
        if region_pool is not None:
            await region_pool.close()

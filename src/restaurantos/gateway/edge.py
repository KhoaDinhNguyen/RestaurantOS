"""Local-primary tracking for the regional edge API."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from time import monotonic

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import find_local_primary
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.gateway.pools import RegionPools


class EdgePrimary:
    """Track the current writable database node for one region."""

    def __init__(
        self,
        cluster: Cluster,
        region_id: str,
        settings: Settings,
    ) -> None:
        self.cluster = cluster
        self.region_id = region_id
        self.settings = settings
        self.current: str | None = None
        self._refresh_lock = asyncio.Lock()
        self._last_refresh_completed = 0.0

    async def refresh(self) -> str | None:
        """Probe the local nodes, coalescing overlapping refreshes."""
        waiting_since = monotonic()

        async with self._refresh_lock:
            # Another refresh completed while this caller was waiting.
            # Its result is fresh for this caller, so reuse it.
            if self._last_refresh_completed >= waiting_since:
                return self.current

            self.current = await find_local_primary(
                self.cluster,
                self.region_id,
                self.current,
                self.settings,
            )
            self._last_refresh_completed = monotonic()

            return self.current

    async def get_pool(
        self,
        pools: RegionPools,
    ) -> AsyncConnectionPool:
        """Return a pool for the current local primary."""
        if self.current is None:
            raise RegionUnreachableError(region_id=self.region_id)

        return await pools.get(self.current)


async def run_edge[T](
    primary: EdgePrimary,
    pools: RegionPools,
    work: Callable[[AsyncConnectionPool], Awaitable[T]],
) -> T:
    """Run edge work and re-probe immediately after a database routing error."""
    if primary.current is None:
        raise RegionUnreachableError(region_id=primary.region_id)

    node = primary.current
    pool = await pools.get(node)

    try:
        return await work(pool)
    except RegionUnreachableError, StaleEpochError:
        await pools.drop_if_same(node, pool)
        await primary.refresh()
        raise

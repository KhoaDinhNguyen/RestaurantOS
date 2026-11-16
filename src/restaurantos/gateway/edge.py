"""Local-primary tracking for the regional edge API."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

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

    async def refresh(self) -> str | None:
        """Probe the local nodes and update the current primary."""
        self.current = await find_local_primary(
            self.cluster,
            self.region_id,
            self.current,
            self.settings,
        )
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
    work: Callable[[], Awaitable[T]],
) -> T:
    """Run edge work and re-probe immediately after a database routing error."""
    try:
        return await work()
    except RegionUnreachableError, StaleEpochError:
        failed_node = primary.current

        if failed_node is not None:
            await pools.drop(failed_node)

        await primary.refresh()
        raise

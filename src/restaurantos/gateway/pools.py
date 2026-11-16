"""Database pool management for gateway region nodes."""

from __future__ import annotations

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo


class RegionPools:
    """Lazily create and reuse one pool per region database node."""

    def __init__(self, cluster: Cluster, settings: Settings) -> None:
        self.cluster = cluster
        self.settings = settings
        self._pools: dict[str, AsyncConnectionPool] = {}

    async def get(self, node: str) -> AsyncConnectionPool:
        """Return the reusable pool for a node."""
        pool = self._pools.get(node)

        if pool is None:
            pool = make_pool(
                node_conninfo(
                    self.cluster,
                    node,
                    settings=self.settings,
                ),
                self.settings,
            )
            await pool.open()
            self._pools[node] = pool

        return pool

    async def close(self) -> None:
        """Close every pool created by this manager."""
        for pool in self._pools.values():
            await pool.close()

        self._pools.clear()

"""HQ connections follow its sole writable node; promotion remains manual.

HQ has no regional epoch. The operator physically fences the old primary.
Two writable nodes are ambiguous and must never trigger client selection.
"""

import asyncio
from contextlib import asynccontextmanager

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.sqlfiles import load_query


class HQPool:
    """Pool-compatible facade, usable by existing run_on call sites.

    Probe on every checkout so a live stale primary cannot win via a cached
    target. Probes use a fraction of the request deadline and never cancel
    psycopg. Writes are never replayed here: callers retain their idempotency.
    """

    def __init__(self, cluster: Cluster, settings: Settings, *, direct=False, pools=None):
        self.settings = settings
        self.nodes = [HQ_PRIMARY, HQ_STANDBY] if cluster.standbys else [HQ_PRIMARY]
        self.owned = pools is None
        self.pools = (
            pools
            if pools is not None
            else {
                n: make_pool(node_conninfo(cluster, n, direct=direct, settings=settings), settings)
                for n in self.nodes
            }
        )
        self.probe_settings = settings.model_copy(
            update={
                "request_deadline_s": min(
                    settings.connect_timeout_s, settings.request_deadline_s / 3
                )
            }
        )

    async def open(self):
        if self.owned:
            await asyncio.gather(*(self.pools[n].open() for n in self.nodes))

    async def close(self):
        if self.owned:
            await asyncio.gather(*(self.pools[n].close() for n in self.nodes))

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def select(self) -> str:
        async def look(node):
            async def read(conn):
                row = await (await conn.execute(load_query("common/hq_probe"))).fetchone()
                return row is not None and row[0] is False

            try:
                return (
                    node
                    if await run_on(self.pools[node], read, settings=self.probe_settings)
                    else None
                )
            except RegionUnreachableError:
                return None

        writable = [n for n in await asyncio.gather(*(look(n) for n in self.nodes)) if n]
        if len(writable) != 1:
            raise RegionUnreachableError(
                "HQ must have exactly one reachable writable node", region_id="hq"
            )
        return writable[0]

    @asynccontextmanager
    async def connection(self):
        node = await self.select()
        async with self.pools[node].connection() as conn:
            # Recheck on the actual transaction connection, before yielding
            # it to the caller. A role change between probe and checkout must
            # fail the request, not send a write to a standby.
            if await (await conn.execute(load_query("common/hq_probe"))).fetchone() != (False,):
                raise RegionUnreachableError("HQ role changed before checkout", region_id="hq")
            yield conn

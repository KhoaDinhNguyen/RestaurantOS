"""Bounded node probes, cached figures, and health events persisted at HQ."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial
from uuid import uuid4

import httpx
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from restaurantos.common.cluster import HQ_PRIMARY, Cluster, service_port
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, probe_node, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.localnode import NodeView
from restaurantos.common.locks import CLUSTER_EVENTS_LOCK_KEY
from restaurantos.common.sqlfiles import load_query
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.health import NodeHealth
from restaurantos.coordinator.models import (
    ClusterEvent,
    ClusterStatus,
    ExceptionEntry,
    NodeStatus,
    RegionStatus,
)

logger = logging.getLogger(__name__)


class Coordinator:
    def __init__(self, cluster: Cluster, settings: Settings, config: CoordinatorSettings) -> None:
        self.cluster = cluster
        self.settings = settings.model_copy(update={"pool_min_size": 0})
        self.config = config
        self.probe_settings = self.settings.model_copy(
            update={"request_deadline_s": config.heartbeat_timeout_s}
        )
        host = "127.0.0.1" if settings.network == "host" else "gateway"
        self.gateway_url = (
            str(config.gateway_url)
            if config.gateway_url is not None
            else f"http://{host}:{service_port(cluster, 'gateway')}"
        )
        self.http = httpx.AsyncClient(timeout=config.gateway_timeout_s)
        self.pools = {
            node: make_pool(node_conninfo(cluster, node, settings=self.settings), self.settings)
            for node in cluster.nodes
        }
        self.health = {
            node: NodeHealth(
                NodeStatus(
                    node_id=node,
                    region_id=None if node.startswith("hq-") else node.rsplit("-", 1)[0],
                    role="standby" if node.endswith("-standby") else "primary",
                )
            )
            for node in cluster.nodes
        }
        self.regions: dict[str, RegionStatus] = {}
        self.samples: dict[str, tuple[str, int, float]] = {}
        self.pending_events: list[dict] = []
        self.stop = asyncio.Event()
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        for pool in self.pools.values():
            await pool.open()
        self.tasks = [
            asyncio.create_task(self._repeat(partial(self._heartbeat, node)))
            for node in self.cluster.nodes
        ]
        self.tasks.append(asyncio.create_task(self._repeat(self._sample_figures)))

    async def close(self) -> None:
        self.stop.set()
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        finally:
            await self.http.aclose()
            await asyncio.gather(*(pool.close() for pool in self.pools.values()))

    async def _repeat(self, work: Callable[[], Awaitable[None]]) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                await work()
            except Exception:
                logger.exception("Coordinator sampling task failed")
                raise
            # This wait contains no database work. Shutdown waits for bounded
            # in-flight calls to finish instead of cancelling psycopg tasks.
            await asyncio.sleep(
                max(0, self.config.heartbeat_interval_s - (time.monotonic() - started))
            )

    async def _gateway_probe(self, node: str) -> bool | None:
        try:
            response = await self.http.get(f"{self.gateway_url.rstrip('/')}/internal/probe/{node}")
            response.raise_for_status()
            body = response.json()
            if (
                isinstance(body, dict)
                and body.get("node_id") == node
                and isinstance(body.get("reachable"), bool)
            ):
                # The current shared gateway probe queries node_state,
                # which only regions have. A negative HQ reply is ambiguous
                # until WP0/WP2 adds an HQ-aware probe.
                if node.startswith("hq-") and not body["reachable"]:
                    return None
                return body["reachable"]
        except httpx.HTTPError, ValueError:
            logger.warning("Gateway could not confirm health of %s", node)
        # An unavailable gateway has supplied no independent observation.
        return None

    async def _heartbeat(self, node: str) -> None:
        if node.startswith("hq-"):

            async def role(conn: AsyncConnection) -> NodeView:
                cursor = await conn.execute(load_query("coordinator/hq_role"))
                return NodeView(node, reachable=True, in_recovery=(await cursor.fetchone())[0])

            try:
                view = await run_on(self.pools[node], role, settings=self.probe_settings)
            except RegionUnreachableError:
                view = NodeView(node)
        else:
            view = await probe_node(
                node, node_conninfo(self.cluster, node, settings=self.settings), self.probe_settings
            )

        health = self.health[node]
        transitions = health.observe(view, datetime.now(UTC), self.config.heartbeat_misses, None)
        self._record(node, transitions)
        if not view.reachable and health.misses >= self.config.heartbeat_misses:
            confirmation = await self._gateway_probe(node)
            self._record(node, health.confirm_gateway(confirmation))

    def _record(self, node: str, transitions: list[str]) -> None:
        health = self.health[node]
        for kind in transitions:
            event_key = str(uuid4())
            self.pending_events.append(
                {
                    "occurred_at": datetime.now(UTC),
                    "kind": kind,
                    "region_id": health.response.region_id,
                    "node_id": node,
                    "event_key": event_key,
                    "detail": Jsonb({"coordinator_event_id": event_key, "misses": health.misses}),
                }
            )

    async def _directory(self) -> None:
        async def read(conn: AsyncConnection):
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(load_query("coordinator/regions"))
                return await cursor.fetchall()

        try:
            rows = await run_on(
                self.pools[HQ_PRIMARY], read, region_id="hq", settings=self.settings
            )
        except RegionUnreachableError:
            return
        current = {}
        for row in rows:
            region_id = row["region_id"]
            previous = self.regions.get(region_id)
            previous = RegionStatus(**row) if previous is None else previous.model_copy(update=row)
            current[region_id] = previous
        self.regions = current

    async def _figures(self, region: RegionStatus) -> None:
        node = region.primary_node
        if node not in self.pools or self.health[node].response.role != "primary":
            return

        async def read(conn: AsyncConnection):
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(load_query("coordinator/figures"))
                return await cursor.fetchone()

        try:
            row = await run_on(
                self.pools[node], read, region_id=region.region_id, settings=self.settings
            )
        except RegionUnreachableError:
            return

        sampled = time.monotonic()
        total = int(row.pop("total_orders"))
        row["replication"] = row.pop("replication_mode")
        previous = self.samples.get(region.region_id)
        rate = None
        if previous is not None:
            old_node, old_total, old_time = previous
            elapsed = sampled - old_time
            if node == old_node and total >= old_total and elapsed > 0:
                rate = (total - old_total) / elapsed
        self.samples[region.region_id] = (node, total, sampled)
        self.regions[region.region_id] = region.model_copy(update={**row, "orders_per_s": rate})

    async def flush_events(self) -> None:
        if not self.pending_events:
            return
        batch = list(self.pending_events)

        async def write(conn: AsyncConnection) -> None:
            await conn.execute(
                load_query("coordinator/lock_events"), {"lock_key": CLUSTER_EVENTS_LOCK_KEY}
            )
            for event in batch:
                await conn.execute(load_query("coordinator/record_event"), event)

        try:
            await run_on(self.pools[HQ_PRIMARY], write, region_id="hq", settings=self.settings)
        except RegionUnreachableError:
            return
        del self.pending_events[: len(batch)]

    async def _sample_figures(self) -> None:
        await self._directory()
        await asyncio.gather(*(self._figures(region) for region in self.regions.values()))
        await self.flush_events()

    def snapshot(self) -> ClusterStatus:
        return ClusterStatus(
            as_of=datetime.now(UTC),
            nodes=[health.response for health in self.health.values()],
            regions=list(self.regions.values()),
        )

    async def _page(self, kind: str, since: int | None, limit: int):
        name = f"coordinator/{kind}_{'latest' if since is None else 'after'}"

        async def read(conn: AsyncConnection):
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(load_query(name), {"since": since, "page_size": limit})
                return await cursor.fetchall()

        return await run_on(self.pools[HQ_PRIMARY], read, region_id="hq", settings=self.settings)

    async def events(self, since: int | None, limit: int) -> list[ClusterEvent]:
        return [ClusterEvent(**row) for row in await self._page("events", since, limit)]

    async def exceptions(self, since: int | None, limit: int) -> list[ExceptionEntry]:
        return [ExceptionEntry(**row) for row in await self._page("exceptions", since, limit)]

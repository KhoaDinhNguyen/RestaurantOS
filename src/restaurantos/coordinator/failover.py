"""Resumable regional promotion. HQ's locked directory row serializes decisions.

PostgreSQL promotion cannot be rolled back. The database role and node epoch
are therefore the recovery journal, including when a commit acknowledgement
is lost. No process-local flag is needed to recover after a restart.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from restaurantos.common.cluster import HQ_PRIMARY
from restaurantos.common.db import node_conninfo, probe_node, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.localnode import NodeView
from restaurantos.common.locks import CLUSTER_EVENTS_LOCK_KEY
from restaurantos.common.sqlfiles import load_query

if TYPE_CHECKING:
    from restaurantos.coordinator.service import Coordinator

logger = logging.getLogger(__name__)


class Failover:
    def __init__(self, runtime: Coordinator) -> None:
        self.runtime = runtime
        self.lock = asyncio.Lock()
        self.partitions: dict[str, str] = {}
        # HQ may be idle while we check the independent path or update a node.
        self.settings = runtime.settings.model_copy(
            update={
                "request_deadline_s": runtime.config.failover_timeout_s,
                "idle_in_transaction_timeout_ms": int(runtime.config.failover_timeout_s * 1000),
            }
        )

    async def probe(self, node: str) -> NodeView:
        runtime = self.runtime
        return await probe_node(
            node,
            node_conninfo(runtime.cluster, node, settings=runtime.settings),
            runtime.probe_settings,
        )

    async def step(self, region_id: str) -> None:
        """Reconcile before deciding, using fresh HQ state and fresh node probes."""
        runtime = self.runtime

        async def reconcile(conn):
            await conn.execute(
                load_query("coordinator/promotion_limits"),
                {"timeout_ms": str(int(runtime.config.failover_timeout_s * 1000))},
            )
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    load_query("coordinator/lock_region"), {"region_id": region_id}
                )
                row = await cursor.fetchone()
            # SKIP LOCKED also excludes a still-running abandoned decision.
            if row is None or row["standby_node"] is None:
                return
            primary, standby = row["primary_node"], row["standby_node"]
            if primary not in runtime.pools or standby not in runtime.pools:
                return
            candidate = await self.probe(standby)
            isolated = all(runtime.health[n].response.status == "down" for n in (primary, standby))
            if candidate.reachable or not isolated:
                self.partitions.pop(region_id, None)
            if not candidate.reachable:
                if isolated:
                    # Keep retries idempotent while allowing a later outage
                    # in the same epoch to appear separately on the timeline.
                    marker = self.partitions.setdefault(region_id, str(uuid4()))
                    await self.event(
                        conn, row, "partition", standby, row["epoch"], {}, marker=marker
                    )
                return

            epoch = row["epoch"] + 1
            if candidate.writable:
                # A lower epoch belongs to a completed promotion's old primary.
                # Never promote it back, even if it is still writable.
                if candidate.epoch not in (row["epoch"], epoch):
                    return
            else:
                if candidate.epoch != row["epoch"]:
                    return
                if runtime.health[primary].response.status != "down":
                    return
                # A cached 'down' is insufficient authorization. Check both
                # paths again while holding the directory lock.
                old, gateway = await asyncio.gather(
                    self.probe(primary), runtime._gateway_probe(primary)
                )
                if old.reachable or gateway is not False:
                    return
                await self.node_call(standby, "promote", {})
                candidate = await self.probe(standby)
                if not candidate.writable:
                    return  # promotion is asynchronous; retry from the role

            updated = await self.node_call(
                standby,
                "promotion_epoch",
                {"region_id": region_id, "old_epoch": row["epoch"], "epoch": epoch},
            )
            if updated != (epoch,):
                return
            # HQ's swap and event commit together. A retry reads the swapped
            # row and cannot increment again or insert a second failover event.
            await conn.execute(
                load_query("coordinator/finish_promotion"),
                {
                    "region_id": region_id,
                    "new_primary_node": standby,
                    "new_standby_node": primary,
                    "epoch": epoch,
                },
            )
            await self.event(
                conn,
                row,
                "failover",
                standby,
                epoch,
                {"old_primary": primary, "new_primary": standby, "epoch": epoch},
            )

        async with self.lock:
            try:
                await run_on(
                    runtime.pools[HQ_PRIMARY], reconcile, region_id="hq", settings=self.settings
                )
            except RegionUnreachableError:
                # The next iteration must inspect the role/epoch, not assume
                # that a timed-out promotion or write did not happen.
                logger.warning("Failover reconciliation deferred for %s", region_id)

    async def node_call(self, node: str, query: str, params: dict) -> tuple | None:
        async def execute(conn):
            cursor = await conn.execute(load_query(f"coordinator/{query}"), params)
            return await cursor.fetchone()

        return await run_on(self.runtime.pools[node], execute, settings=self.runtime.settings)

    async def event(
        self,
        conn: AsyncConnection,
        row: dict,
        kind: str,
        node: str,
        epoch: int,
        detail: dict,
        *,
        marker: str | None = None,
    ) -> None:
        marker = marker or f"{kind}:{row['region_id']}:{epoch}"
        await conn.execute(
            load_query("coordinator/lock_events"), {"lock_key": CLUSTER_EVENTS_LOCK_KEY}
        )
        await conn.execute(
            load_query("coordinator/record_event"),
            {
                "occurred_at": datetime.now(UTC),
                "kind": kind,
                "region_id": row["region_id"],
                "node_id": node,
                "event_key": marker,
                "detail": Jsonb({**detail, "coordinator_event_id": marker}),
            },
        )

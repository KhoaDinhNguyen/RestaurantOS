"""Resumable regional promotion. HQ's locked directory row serializes decisions.

PostgreSQL promotion cannot be rolled back. The database role and node epoch
are therefore the recovery journal, including when a commit acknowledgement
is lost. No process-local flag is needed to recover after a restart.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from restaurantos.common.cluster import HQ_PRIMARY
from restaurantos.common.db import node_conninfo, probe_node, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.localnode import NodeView, local_nodes
from restaurantos.common.locks import CLUSTER_EVENTS_LOCK_KEY
from restaurantos.common.sqlfiles import load_query
from restaurantos.coordinator.models import ManualFailoverResult

if TYPE_CHECKING:
    from restaurantos.coordinator.service import Coordinator

logger = logging.getLogger(__name__)


class ManualFailoverError(Exception):
    def __init__(self, error: str) -> None:
        super().__init__(error)
        self.error = error
        self.status_code = 404 if error == "not_found" else 409


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
        """Background decisions defer safely on an unavailable database."""
        async with self.lock:
            try:
                await self._run(region_id, manual=False)
            except RegionUnreachableError:
                logger.warning("Failover reconciliation deferred for %s", region_id)

    async def manual(self, region_id: str) -> ManualFailoverResult:
        """Use the same promotion sequence, with operator authorization."""
        if region_id not in self.runtime.cluster.region_ids:
            raise ManualFailoverError("not_found")
        result = await self._run(region_id, manual=True)
        if result is None:
            raise RegionUnreachableError("promotion did not complete", region_id=region_id)
        return result

    async def _run(self, region_id: str, *, manual: bool) -> ManualFailoverResult | None:
        """Reconcile under the database row lock, shared across processes."""
        runtime = self.runtime
        deadline = time.monotonic() + runtime.config.failover_timeout_s

        async def reconcile(conn):
            await conn.execute(
                load_query("coordinator/promotion_limits"),
                {"timeout_ms": str(int(runtime.config.failover_timeout_s * 1000))},
            )
            if manual:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        load_query("coordinator/failover_region"), {"region_id": region_id}
                    )
                    if await cursor.fetchone() is None:
                        raise ManualFailoverError("not_found")
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    load_query("coordinator/lock_region"), {"region_id": region_id}
                )
                row = await cursor.fetchone()
            # SKIP LOCKED also excludes a still-running abandoned decision.
            if row is None:
                if manual:
                    raise ManualFailoverError("failover_in_progress")
                return
            if row["standby_node"] is None:
                if manual:
                    raise ManualFailoverError("standby_unavailable")
                return
            primary, standby = row["primary_node"], row["standby_node"]
            if (
                primary not in runtime.pools
                or standby not in runtime.pools
                or {primary, standby} != set(local_nodes(runtime.cluster, region_id))
            ):
                if manual:
                    raise ManualFailoverError("standby_unavailable")
                return
            candidate = await self.probe(standby)
            if manual:
                if candidate.writable and candidate.epoch >= row["epoch"]:
                    raise ManualFailoverError("failover_in_progress")
                if (
                    runtime.health[standby].response.status != "up"
                    or not candidate.reachable
                    or not candidate.in_recovery
                    or candidate.epoch != row["epoch"]
                ):
                    raise ManualFailoverError("standby_unavailable")
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
                if not manual and runtime.health[primary].response.status != "down":
                    return
                # A cached 'down' is insufficient authorization. Check both
                # paths again while holding the directory lock.
                if not manual:
                    old, gateway = await asyncio.gather(
                        self.probe(primary), runtime._gateway_probe(primary)
                    )
                    if old.reachable or gateway is not False:
                        return
                await self.node_call(standby, "promote", {})
                candidate = await self.probe(standby)
                if manual:
                    while not candidate.writable and time.monotonic() < deadline:
                        await asyncio.sleep(0.05)
                        candidate = await self.probe(standby)
                    if not candidate.writable:
                        raise RegionUnreachableError(
                            "promotion did not finish", region_id=region_id
                        )
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

            if manual:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        load_query("coordinator/failover_event"),
                        {"marker": f"failover:{region_id}:{epoch}"},
                    )
                    event = await cursor.fetchone()
                if event is None:
                    raise RegionUnreachableError("promotion event missing", region_id="hq")
                return ManualFailoverResult(
                    region_id=region_id,
                    epoch=epoch,
                    primary_node=standby,
                    standby_node=primary,
                    event_id=event["event_id"],
                )

        # A 200 result leaves this method only after HQ commits its swap/event.
        return await run_on(
            runtime.pools[HQ_PRIMARY], reconcile, region_id="hq", settings=self.settings
        )

    async def node_call(self, node: str, query: str, params: dict) -> tuple | None:
        async def execute(conn):
            if query == "promotion_epoch":
                # The old primary cannot acknowledge WAL from the promoted
                # node until it rejoins. Flush this fencing write locally;
                # waiting for it would prevent the promotion from finishing.
                await conn.execute(load_query("coordinator/local_epoch_commit"))
            cursor = await conn.execute(load_query(f"coordinator/{query}"), params)
            return await cursor.fetchone()

        return await run_on(
            self.runtime.pools[node],
            execute,
            region_id=node.rsplit("-", 1)[0],
            settings=self.runtime.settings,
        )

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

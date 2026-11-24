"""Resumable sync-to-async fallback, journaled at HQ before configuration changes."""

import logging
from typing import TYPE_CHECKING
from uuid import uuid4

from psycopg import Error
from psycopg.rows import dict_row

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, slot_name
from restaurantos.common.db import make_pool, node_conninfo, probe_node, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.sqlfiles import load_query

if TYPE_CHECKING:
    from restaurantos.coordinator.service import Coordinator

logger = logging.getLogger(__name__)


class ReplicationFallback:
    def __init__(self, runtime: Coordinator) -> None:
        self.runtime = runtime
        self.misses: dict[tuple[str, str, int], int] = {}

    async def status(self, row: dict) -> dict | None:
        async def read(conn):
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    load_query("coordinator/sync_status"),
                    {"region_id": row["region_id"], "slot": slot_name(row["standby_node"])},
                )
                return await cursor.fetchone()

        return await run_on(
            self.runtime.pools[row["primary_node"]], read, settings=self.runtime.settings
        )

    async def disable(self, node: str) -> None:
        # ALTER SYSTEM cannot run inside a transaction. Keep autocommit away
        # from the service pools. It is irreversible, so an HQ intent must
        # already be committed before this method is called.
        runtime = self.runtime
        pool = make_pool(
            node_conninfo(runtime.cluster, node, settings=runtime.settings), runtime.settings
        )
        await pool.open()
        try:

            async def change(conn):
                await conn.set_autocommit(True)
                await conn.execute(load_query("coordinator/disable_sync"))
                await conn.execute(load_query("coordinator/reload_config"))

            await run_on(pool, change, settings=runtime.settings)
        finally:
            await pool.close()

    async def step_hq(self) -> None:
        """HQ sync loss must not block its own recovery journal's commit."""
        runtime = self.runtime
        if not runtime.cluster.standbys:
            return
        try:
            primary = await runtime.hq_pool.select()
            standby = HQ_STANDBY if primary == HQ_PRIMARY else HQ_PRIMARY

            async def reconcile(conn):
                locked = await (
                    await conn.execute(load_query("coordinator/hq_promotion_lock"))
                ).fetchone()
                if locked != (True,):
                    return

                async def read():
                    async with conn.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute(
                            load_query("coordinator/hq_sync_status"), {"slot": slot_name(standby)}
                        )
                        return await cursor.fetchone()

                state = await read()
                if state["in_recovery"]:
                    return
                # Read business commit policy before the transaction-local
                # control override; otherwise every status would say local.
                await conn.execute(load_query("coordinator/local_epoch_commit"))
                row = {"region_id": None, "primary_node": primary, **state}
                async with conn.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(load_query("coordinator/pending_hq_sync_downgrade"), row)
                    pending = await cursor.fetchone()
                key = ("hq", primary, state["timeline"])
                if pending is not None:
                    if state["standby_names"]:
                        await self.disable(primary)
                        state = await read()
                    if state["standby_names"] or state["in_recovery"]:
                        return
                    detail = {
                        **pending["detail"],
                        "request_id": pending["detail"]["coordinator_event_id"],
                        "replication": "async",
                    }
                    await runtime.failover.event(
                        conn,
                        row,
                        "sync_downgrade",
                        primary,
                        0,
                        detail,
                        marker=f"sync-complete:{detail['request_id']}",
                    )
                    self.misses.pop(key, None)
                    return
                if (
                    not state["standby_names"]
                    or state["commit_mode"] in ("off", "local")
                    or state["sync_connected"]
                ):
                    self.misses.pop(key, None)
                    return
                self.misses[key] = self.misses.get(key, 0) + 1
                if self.misses[key] < runtime.config.heartbeat_misses:
                    return
                await runtime.failover.event(
                    conn,
                    row,
                    "sync_downgrade_requested",
                    primary,
                    0,
                    {
                        "scope": "hq",
                        "primary_node": primary,
                        "system_id": state["system_id"],
                        "timeline": state["timeline"],
                    },
                    marker=str(uuid4()),
                )

            await run_on(
                runtime.pools[primary], reconcile, region_id="hq", settings=runtime.settings
            )
        except RegionUnreachableError:
            logger.warning("HQ synchronous fallback deferred")
        except Error:
            logger.exception("HQ synchronous fallback failed; intent retained for retry")

    async def step(self, region_id: str) -> None:
        runtime = self.runtime

        async def reconcile(conn):
            await conn.execute(load_query("coordinator/local_epoch_commit"))
            await conn.execute(
                load_query("coordinator/promotion_limits"),
                {"timeout_ms": str(int(runtime.config.failover_timeout_s * 1000))},
            )
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    load_query("coordinator/lock_region"), {"region_id": region_id}
                )
                row = await cursor.fetchone()
            if row is None or row["standby_node"] is None:
                return
            primary, standby = row["primary_node"], row["standby_node"]
            if primary not in runtime.pools or standby not in runtime.pools:
                return
            key = (region_id, primary, row["epoch"])
            if runtime.health[primary].response.status != "up":
                self.misses.pop(key, None)
                return
            # Promotion recovery takes precedence. Do not change the old
            # primary's configuration during an unfinished promotion.
            candidate = await probe_node(
                standby,
                node_conninfo(runtime.cluster, standby, settings=runtime.settings),
                runtime.probe_settings,
            )
            if candidate.writable and candidate.epoch >= row["epoch"]:
                self.misses.pop(key, None)
                return
            state = await self.status(row)
            if state is None or state["in_recovery"] or state["epoch"] != row["epoch"]:
                self.misses.pop(key, None)
                return
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(load_query("coordinator/pending_sync_downgrade"), row)
                pending = await cursor.fetchone()
            if pending is not None:
                if state["standby_names"]:
                    await self.disable(primary)
                    state = await self.status(row)
                # Reload is asynchronous. Verify the actual setting, role and
                # epoch before committing the completion event.
                if (
                    state is None
                    or state["standby_names"]
                    or state["in_recovery"]
                    or state["epoch"] != row["epoch"]
                ):
                    return
                detail = {
                    **pending["detail"],
                    "request_id": pending["detail"]["coordinator_event_id"],
                    "replication": "async",
                }
                await runtime.failover.event(
                    conn,
                    row,
                    "sync_downgrade",
                    primary,
                    row["epoch"],
                    detail,
                    marker=f"sync-complete:{detail['request_id']}",
                )
                self.misses.pop(key, None)
                return
            if not state["standby_names"] or state["commit_mode"] in ("off", "local"):
                self.misses.pop(key, None)
                return
            confirmed_down = (
                not candidate.reachable
                and runtime.health[standby].response.status == "down"
                and await runtime._gateway_probe(standby) is False
            )
            if state["sync_connected"] and not confirmed_down:
                self.misses.pop(key, None)
                return
            if state["setting_source"] == "command line":
                logger.error(
                    "%s sync policy is a command-line override; configure it in a reloadable file",
                    primary,
                )
                return
            self.misses[key] = (
                runtime.config.heartbeat_misses if confirmed_down else self.misses.get(key, 0) + 1
            )
            if self.misses[key] < runtime.config.heartbeat_misses:
                return
            # This transaction only records intent. A later cycle can see it
            # committed, including after a lost acknowledgement or restart.
            await runtime.failover.event(
                conn,
                row,
                "sync_downgrade_requested",
                primary,
                row["epoch"],
                {
                    "epoch": row["epoch"],
                    "primary_node": primary,
                    "standby_node": standby,
                    "previous_standby_names": state["standby_names"],
                    "reason": "standby_unreachable"
                    if confirmed_down
                    else "no_eligible_sync_standby",
                },
                marker=str(uuid4()),
            )

        try:
            await run_on(
                runtime.hq_pool,
                reconcile,
                region_id="hq",
                settings=runtime.failover.settings,
            )
        except RegionUnreachableError, Error:
            for key in list(self.misses):
                if key[0] == region_id:
                    self.misses.pop(key)
            logger.exception("Sync fallback reconciliation deferred for %s", region_id)

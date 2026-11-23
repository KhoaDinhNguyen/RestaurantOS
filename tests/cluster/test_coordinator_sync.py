"""Sync fallback on a disposable dev topology (ROS_SYNC_TEST_CLUSTER=1).

No Docker control is required by the test. Toxiproxy cuts the replication
link and then both health paths to demonstrate disappearance of the standby.
"""

import asyncio
import os

import httpx
import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import node_conninfo, probe_node, run_on
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.service import Coordinator

pytestmark = pytest.mark.cluster


class CoordinatorStopped(Exception):
    pass


async def eventually(check):
    for _ in range(150):
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.1)
    raise AssertionError("synchronous fallback did not reach the expected state")


@pytest.fixture
async def live():
    if os.environ.get("ROS_SYNC_TEST_CLUSTER") != "1":
        pytest.skip("requires an explicitly disposable cluster: ROS_SYNC_TEST_CLUSTER=1")
    cluster = load_cluster()
    settings = Settings(
        _env_file=None,
        client="coordinator",
        network=os.environ.get("ROS_NETWORK", "host"),
        connect_timeout_s=3,
        request_deadline_s=5,
    )
    config = CoordinatorSettings(_env_file=None, heartbeat_interval_s=0.5, heartbeat_timeout_s=0.8)
    runtime = Coordinator(cluster, settings, config)
    for pool in runtime.pools.values():
        await pool.open()
    writer = None
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=3) as api:
        names = [
            proxy_name("r1-standby", "r1-primary"),
            proxy_name("coordinator", "r1-standby"),
            proxy_name("gateway", "r1-standby"),
        ]
        originals = [
            (await api.get(f"/proxies/{name}")).raise_for_status().json() for name in names
        ]
        assert all(p["enabled"] and not p["toxics"] for p in originals)
        try:

            async def setup(conn):
                await conn.execute(
                    "INSERT INTO region_state (region_id, primary_node, standby_node)"
                    " VALUES ('r1', 'r1-primary', 'r1-standby')"
                )

            await run_on(runtime.pools[HQ_PRIMARY], setup, settings=settings)

            async def schema(conn):
                await conn.execute("CREATE TABLE sync_probe (id integer PRIMARY KEY)")

            await run_on(runtime.pools["r1-primary"], schema, settings=settings)
            async with await psycopg.AsyncConnection.connect(
                node_conninfo(cluster, "r1-primary", settings=settings), autocommit=True
            ) as admin:
                await admin.execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (*)'")
                await admin.execute("SELECT pg_reload_conf()")
            runtime.health["r1-primary"].response.status = "up"
            runtime.health["r1-standby"].response.status = "up"

            # Exercise the real second database path. The gateway HTTP route
            # and transport are covered by the existing coordinator health tests.
            async def gateway_probe(node):
                gateway_settings = settings.model_copy(
                    update={"client": "gateway", "request_deadline_s": 0.8}
                )
                view = await probe_node(
                    node, node_conninfo(cluster, node, settings=gateway_settings), gateway_settings
                )
                return view.reachable

            runtime._gateway_probe = gateway_probe
            writer = await psycopg.AsyncConnection.connect(
                make_conninfo(
                    node_conninfo(cluster, "r1-primary", settings=settings),
                    application_name="sync-fallback-test-writer",
                    connect_timeout=3,
                )
            )
            yield cluster, settings, config, runtime, api, names, writer
        finally:
            for name in names:
                proxy = (await api.get(f"/proxies/{name}")).raise_for_status().json()
                for toxic in proxy["toxics"]:
                    (await api.delete(f"/proxies/{name}/toxics/{toxic['name']}")).raise_for_status()
                (await api.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
            # Release any synchronous waiter before closing test connections.
            async with await psycopg.AsyncConnection.connect(
                node_conninfo(cluster, "r1-primary", settings=settings), autocommit=True
            ) as admin:
                await admin.execute("ALTER SYSTEM SET synchronous_standby_names = ''")
                await admin.execute("SELECT pg_reload_conf()")
            if writer is not None:
                await writer.close()
            await runtime.close()


async def test_sync_waiter_resumes_and_restart_preserves_audit(live, monkeypatch):
    cluster, settings, config, runtime, api, names, writer = live
    row = {
        "region_id": "r1",
        "epoch": 1,
        "primary_node": "r1-primary",
        "standby_node": "r1-standby",
    }

    async def state():
        return await runtime.replication.status(row)

    async def ready():
        return (await state())["sync_connected"]

    await eventually(ready)
    for _ in range(config.heartbeat_misses + 1):
        await runtime.replication.step("r1")
    assert not [e for e in await runtime.events(0, 100) if e.kind.startswith("sync_downgrade")]
    # Only replication disappears; the standby is still reachable. Waiting
    # commits demonstrate why node health alone cannot detect this condition.
    (await api.post(f"/proxies/{names[0]}", json={"enabled": False})).raise_for_status()

    async def disconnected():
        return not (await state())["sync_connected"]

    await eventually(disconnected)
    await writer.execute("INSERT INTO sync_probe VALUES (1)")
    commit = asyncio.create_task(writer.commit())

    async def waiting():
        async def read(conn):
            cur = await conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity"
                " WHERE application_name = 'sync-fallback-test-writer'"
                " AND wait_event = 'SyncRep')"
            )
            return (await cur.fetchone())[0]

        return await run_on(runtime.pools["r1-primary"], read, settings=settings)

    await eventually(waiting)
    for _ in range(config.heartbeat_misses - 1):
        await runtime.replication.step("r1")
    assert not commit.done()
    assert not [e for e in await runtime.events(0, 100) if e.kind == "sync_downgrade_requested"]
    await runtime.replication.step("r1")
    assert (
        len([e for e in await runtime.events(0, 100) if e.kind == "sync_downgrade_requested"]) == 1
    )
    assert not commit.done()  # intent is durable before ALTER SYSTEM
    original = runtime.failover.event

    async def crash_before_completion(conn, row, kind, *args, **kwargs):
        if kind == "sync_downgrade":
            raise CoordinatorStopped
        return await original(conn, row, kind, *args, **kwargs)

    monkeypatch.setattr(runtime.failover, "event", crash_before_completion)

    async def change():
        try:
            await runtime.replication.step("r1")
        except CoordinatorStopped:
            return True
        return False

    await eventually(change)
    done, _ = await asyncio.wait({commit}, timeout=5)
    assert commit in done
    await commit
    assert (await state())["standby_names"] == ""
    assert not [e for e in await runtime.events(0, 100) if e.kind == "sync_downgrade"]
    # A fresh coordinator has no in-memory operation ID or failure counters.
    restarted = Coordinator(cluster, settings, config)
    for pool in restarted.pools.values():
        await pool.open()
    restarted.health["r1-primary"].response.status = "up"
    restarted._gateway_probe = runtime._gateway_probe
    try:
        await restarted.replication.step("r1")
        await restarted.replication.step("r1")
        events = [e for e in await restarted.events(0, 100) if e.kind == "sync_downgrade"]
        assert len(events) == 1
        assert events[0].detail["replication"] == "async"
        await restarted._sample_figures()
        assert restarted.regions["r1"].replication == "async"

        async def new_write(conn):
            await conn.execute("INSERT INTO sync_probe VALUES (2)")

        await run_on(restarted.pools["r1-primary"], new_write, settings=settings)

        async def check(conn):
            cur = await conn.execute("SELECT id FROM sync_probe ORDER BY id")
            return await cur.fetchall()

        assert await run_on(restarted.pools["r1-primary"], check, settings=settings) == [(1,), (2,)]
        # Healing does not silently re-enable synchronous commit waits.
        (await api.post(f"/proxies/{names[0]}", json={"enabled": True})).raise_for_status()
        for _ in range(config.heartbeat_misses + 1):
            await restarted.replication.step("r1")
        assert (await state())["standby_names"] == ""
        assert len([e for e in await restarted.events(0, 100) if e.kind == "sync_downgrade"]) == 1
        # Whole-standby disappearance produces a new event after an operator
        # explicitly starts a new synchronous experiment in the same epoch.
        async with await psycopg.AsyncConnection.connect(
            node_conninfo(cluster, "r1-primary", settings=settings), autocommit=True
        ) as admin:
            await admin.execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (*)'")
            await admin.execute("SELECT pg_reload_conf()")
        await eventually(ready)
        for stream in ("upstream", "downstream"):
            (
                await api.post(
                    f"/proxies/{names[0]}/toxics",
                    json={
                        "name": f"sync-silence-{stream}",
                        "type": "timeout",
                        "stream": stream,
                        "attributes": {"timeout": 0},
                    },
                )
            ).raise_for_status()
        for name in names[1:]:
            (await api.post(f"/proxies/{name}", json={"enabled": False})).raise_for_status()
        assert (await state())["sync_connected"]  # stale until WAL sender timeout
        await restarted.start()

        async def finished():
            return (
                len([e for e in await restarted.events(0, 100) if e.kind == "sync_downgrade"]) == 2
            )

        await eventually(finished)
        assert restarted.regions["r1"].epoch == 1
    finally:
        await restarted.close()

"""Manual API on an explicitly fresh disposable cluster.

Set ROS_MANUAL_FAILOVER_TEST_CLUSTER=1 and ROS_NETWORK for the test process.
A physical PostgreSQL standby is promoted; recreate this cluster afterward.
"""

import asyncio
import os
from unittest.mock import AsyncMock

import httpx
import psycopg
import pytest

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import find_local_primary, node_conninfo, run_on
from restaurantos.common.errors import StaleEpochError
from restaurantos.coordinator.app import create_app
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.service import Coordinator

pytestmark = pytest.mark.cluster


@pytest.fixture
async def live():
    if os.environ.get("ROS_MANUAL_FAILOVER_TEST_CLUSTER") != "1":
        pytest.skip("requires ROS_MANUAL_FAILOVER_TEST_CLUSTER=1 on a disposable cluster")
    cluster = load_cluster()
    settings = Settings(
        _env_file=None,
        client="coordinator",
        network=os.environ.get("ROS_NETWORK", "host"),
        connect_timeout_s=3,
        request_deadline_s=5,
    )
    config = CoordinatorSettings(_env_file=None, heartbeat_timeout_s=0.8)
    runtime = Coordinator(cluster, settings, config)
    for pool in runtime.pools.values():
        await pool.open()
    name = proxy_name("coordinator", "r1-standby")
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=3) as toxiproxy:
        original = (await toxiproxy.get(f"/proxies/{name}")).raise_for_status().json()
        assert original["enabled"] and not original["toxics"]
        try:
            async with await psycopg.AsyncConnection.connect(
                node_conninfo(cluster, HQ_PRIMARY, settings=settings), autocommit=True
            ) as hq:
                await hq.execute(
                    "INSERT INTO region_state (region_id, primary_node, standby_node)"
                    " VALUES ('r1', 'r1-primary', 'r1-standby')"
                )
            await runtime._heartbeat("r1-primary")
            await runtime._heartbeat("r1-standby")
            app = create_app(runtime)
            # Pools and probes are controlled here so the concurrency test can
            # pause at an exact promotion step without unrelated sampling.
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://coordinator.test"
            ) as api:
                yield cluster, settings, runtime, toxiproxy, api, name
        finally:
            (await toxiproxy.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
            await runtime.close()


async def test_manual_api_promotes_healthy_primary_and_serializes_attempts(live, monkeypatch):
    cluster, settings, runtime, toxiproxy, api, name = live
    assert (await api.post("/admin/regions/r99/failover")).json() == {"error": "not_found"}
    assert (await api.post("/admin/regions/hq/failover")).status_code == 404
    runtime.health["r1-standby"].response.status = "suspect"
    response = await api.post("/admin/regions/r1/failover")
    assert response.status_code == 409
    assert response.json() == {"error": "standby_unavailable"}
    await runtime._heartbeat("r1-standby")
    # A cached up flag cannot authorize promotion over a newly broken path.
    (await toxiproxy.post(f"/proxies/{name}", json={"enabled": False})).raise_for_status()
    response = await api.post("/admin/regions/r1/failover")
    assert response.status_code == 409
    assert response.json() == {"error": "standby_unavailable"}
    (await toxiproxy.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
    await runtime._heartbeat("r1-standby")
    assert runtime.health["r1-primary"].response.status == "up"
    # A promoted node may retain synchronous settings. Its epoch transaction
    # must finish before the old partner has been physically rejoined.
    async with await psycopg.AsyncConnection.connect(
        node_conninfo(cluster, "r1-standby", settings=settings), autocommit=True
    ) as admin:
        await admin.execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (*)'")
        await admin.execute("SELECT pg_reload_conf()")

    async def policy(conn):
        cur = await conn.execute("SHOW synchronous_standby_names")
        return (await cur.fetchone())[0]

    for _ in range(100):
        if await run_on(runtime.pools["r1-standby"], policy, settings=settings):
            break
        await asyncio.sleep(0.05)
    assert await run_on(runtime.pools["r1-standby"], policy, settings=settings)
    # Hold the first real promotion after pg_promote and before its epoch.
    # Another coordinator instance must see the database lock, not rely on
    # process-local flags or a blocked asyncio lock.
    promoted, release = asyncio.Event(), asyncio.Event()
    original = runtime.failover.node_call

    async def hold(node, query, params):
        result = await original(node, query, params)
        if query == "promote":
            promoted.set()
            await release.wait()
        return result

    spy = AsyncMock(side_effect=hold)
    monkeypatch.setattr(runtime.failover, "node_call", spy)
    first = asyncio.create_task(api.post("/admin/regions/r1/failover"))
    other = Coordinator(cluster, settings, runtime.config)
    for pool in other.pools.values():
        await pool.open()
    try:
        for _ in range(100):
            if promoted.is_set():
                break
            assert not first.done(), first.result() if first.done() else None
            await asyncio.sleep(0.05)
        assert promoted.is_set()
        other_app = create_app(other)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=other_app), base_url="http://coordinator.other"
        ) as other_api:
            conflict = await other_api.post("/admin/regions/r1/failover")
        assert conflict.status_code == 409
        assert conflict.json() == {"error": "failover_in_progress"}
        await other.failover.step("r1")  # automatic path also cannot enter
        assert [call.args[1] for call in spy.await_args_list] == ["promote"]
    finally:
        release.set()
        await other.close()
    response = await first
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {"region_id", "epoch", "primary_node", "standby_node", "event_id"}
    assert body["region_id"] == "r1" and body["epoch"] == 2
    assert body["primary_node"] == "r1-standby" and body["standby_node"] == "r1-primary"

    async def durability(conn):
        cur = await conn.execute("SHOW synchronous_commit")
        return await cur.fetchone()

    assert await run_on(runtime.pools["r1-standby"], durability, settings=settings) == ("on",)
    events = (await api.get("/admin/events?since=0")).json()
    failovers = [e for e in events if e["kind"] == "failover"]
    assert len(failovers) == 1 and failovers[0]["event_id"] == body["event_id"]
    assert failovers[0]["node_id"] == body["primary_node"]

    async def directory(conn):
        cur = await conn.execute("SELECT epoch, primary_node, standby_node FROM region_state")
        return await cur.fetchone()

    assert await run_on(runtime.pools[HQ_PRIMARY], directory, settings=settings) == (
        2,
        "r1-standby",
        "r1-primary",
    )

    async def fence(conn):
        await conn.execute("SELECT assert_epoch(2)")

    with pytest.raises(StaleEpochError):
        await run_on(runtime.pools["r1-primary"], fence, settings=settings)
    await run_on(runtime.pools["r1-standby"], fence, settings=settings)
    assert await find_local_primary(cluster, "r1", "r1-primary", settings) == "r1-standby"
    # The old writable primary has not rejoined. A repeat POST cannot use it
    # as a standby and cannot create another event or bump the epoch again.
    repeat = await api.post("/admin/regions/r1/failover")
    assert repeat.status_code == 409
    assert repeat.json() == {"error": "standby_unavailable"}
    assert len([e for e in await runtime.events(0, 100) if e.kind == "failover"]) == 1

"""Coordinator admin reads and two-path health checks on the dev cluster.

Run with the baseline cluster up. ROS_NETWORK=compose also works when the
test process shares its Docker network. All writes use unique test databases.
"""

import asyncio
import os
import socket
import sys
import tempfile
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import apply_schema, node_conninfo
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.coordinator import service
from restaurantos.coordinator.app import create_app
from restaurantos.coordinator.config import CoordinatorSettings

pytestmark = pytest.mark.cluster


async def eventually(check):
    for _ in range(200):
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("coordinator did not reach the expected state")


@pytest.fixture
async def live():
    cluster = load_cluster()
    settings = Settings(
        _env_file=None,
        network=os.environ.get("ROS_NETWORK", "host"),
        client="coordinator",
        db_name=f"coordinator_test_{uuid4().hex[:12]}",
        request_deadline_s=0.8,
    )
    admin_settings = settings.model_copy(update={"db_name": "restaurantos"})
    admins, connections, proxies = {}, {}, []
    gateway = None
    with tempfile.TemporaryFile() as gateway_log:
        async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=2) as api:
            try:
                try:
                    (await api.get("/version")).raise_for_status()
                except httpx.HTTPError:
                    pytest.skip("the dev cluster Toxiproxy API is not available")
                for client in ("coordinator", "gateway"):
                    name = proxy_name(client, "r1-primary")
                    original = (await api.get(f"/proxies/{name}")).raise_for_status().json()
                    if not original["enabled"] or original["toxics"]:
                        pytest.skip("coordinator tests need idle dev proxies")
                    proxies.append(name)

                for node, kind in ((HQ_PRIMARY, "hq"), ("r1-primary", "region")):
                    admin = await psycopg.AsyncConnection.connect(
                        node_conninfo(cluster, node, direct=True, settings=admin_settings),
                        autocommit=True,
                    )
                    admins[node] = admin
                    await admin.execute(
                        sql.SQL("CREATE DATABASE {}").format(sql.Identifier(settings.db_name))
                    )
                    conn = await psycopg.AsyncConnection.connect(
                        node_conninfo(cluster, node, direct=True, settings=settings),
                        autocommit=True,
                    )
                    connections[node] = conn
                    await apply_schema(conn, kind)
                hq, region = connections[HQ_PRIMARY], connections["r1-primary"]
                await hq.execute(
                    "INSERT INTO region_state (region_id, primary_node, standby_node)"
                    " VALUES ('r1', 'r1-primary', 'r1-standby')"
                )
                await hq.execute(
                    "INSERT INTO holding_queue (region_id, channel, external_order_id, request)"
                    " VALUES ('r1', 'test', 'one', '{}'::jsonb)"
                )
                await region.execute("INSERT INTO node_state (region_id) VALUES ('r1')")
                await region.execute(
                    "INSERT INTO outbox (event_type, payload, created_at)"
                    " VALUES ('exception.logged', '{}', clock_timestamp() - INTERVAL '2 seconds'),"
                    " ('exception.logged', '{}', clock_timestamp() - INTERVAL '2 seconds')"
                )
                for _ in range(3):
                    await hq.execute(
                        "INSERT INTO analytics_exceptions"
                        " (event_id, region_id, order_day, order_id, kind, current_status,"
                        " current_changed_at, attempted_status, attempted_at)"
                        " VALUES (%s, 'r1', 14, %s, 'status_conflict', 'accepted',"
                        " clock_timestamp(), 'ready', clock_timestamp())",
                        (uuid4(), uuid4()),
                    )

                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    port = listener.getsockname()[1]
                url = f"http://127.0.0.1:{port}"
                gateway = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "restaurantos.gateway.app:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    env=dict(
                        os.environ,
                        ROS_CLIENT="gateway",
                        ROS_DB_NAME=settings.db_name,
                        ROS_NETWORK=settings.network,
                        ROS_REQUEST_DEADLINE_S="0.4",
                    ),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=gateway_log,
                )
                async with httpx.AsyncClient(base_url=url, timeout=1) as gateway_api:

                    async def ready():
                        if gateway.returncode is not None:
                            gateway_log.seek(0)
                            pytest.fail(gateway_log.read().decode())
                        try:
                            return (await gateway_api.get("/health")).status_code == 200
                        except httpx.HTTPError:
                            return False

                    await eventually(ready)

                config = CoordinatorSettings(
                    _env_file=None,
                    heartbeat_interval_s=0.1,
                    heartbeat_misses=2,
                    heartbeat_timeout_s=0.4,
                    gateway_timeout_s=1,
                    gateway_url=url,
                )
                yield cluster, settings, config, hq, region, api
            finally:
                if gateway is not None:
                    if gateway.returncode is None:
                        gateway.terminate()
                    await gateway.wait()
                for name in proxies:
                    (await api.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
                for conn in connections.values():
                    await conn.close()
                for admin in admins.values():
                    await admin.execute(
                        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                            sql.Identifier(settings.db_name)
                        )
                    )
                    await admin.close()


async def test_two_path_health_cached_figures_and_admin_paging(live):
    cluster, settings, config, hq, region, toxiproxy = live
    runtime = service.Coordinator(cluster, settings, config)
    app = create_app(runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://coordinator.test"
        ) as api,
    ):

        async def initial():
            data = (await api.get("/admin/cluster")).json()
            if (
                len(data["nodes"]) == len(cluster.nodes)
                and all(node["status"] == "up" for node in data["nodes"])
                and data["regions"]
                and data["regions"][0]["outbox_backlog"] == 2
            ):
                return data
            return False

        data = await eventually(initial)
        assert data["regions"][0]["holding_queue"] == 1
        assert data["regions"][0]["hq_lag_s"] >= 2
        assert data["regions"][0]["replication"] == "async"
        assert {n["role"] for n in data["nodes"]} == {"primary", "standby"}

        async def node_has(status):
            data = (await api.get("/admin/cluster")).json()
            return (
                next(n for n in data["nodes"] if n["node_id"] == "r1-primary")["status"] == status
            )

        coordinator_proxy = proxy_name("coordinator", "r1-primary")
        gateway_proxy = proxy_name("gateway", "r1-primary")
        (
            await toxiproxy.post(f"/proxies/{coordinator_proxy}", json={"enabled": False})
        ).raise_for_status()

        async def suspect():
            return await node_has("suspect")

        await eventually(suspect)
        cached = (await api.get("/admin/cluster")).json()["regions"][0]
        await region.execute(
            "INSERT INTO outbox (event_type, payload) VALUES ('exception.logged', '{}')"
        )
        await asyncio.sleep(0.3)
        stale = (await api.get("/admin/cluster")).json()["regions"][0]
        assert stale["figures_as_of"] == cached["figures_as_of"]
        assert stale["outbox_backlog"] == 2

        (
            await toxiproxy.post(f"/proxies/{gateway_proxy}", json={"enabled": False})
        ).raise_for_status()

        async def down():
            return await node_has("down")

        await eventually(down)
        for name in (coordinator_proxy, gateway_proxy):
            (await toxiproxy.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()

        async def recovered():
            data = (await api.get("/admin/cluster")).json()
            node = next(n for n in data["nodes"] if n["node_id"] == "r1-primary")
            return node["status"] == "up" and data["regions"][0]["outbox_backlog"] == 3

        await eventually(recovered)

        async def events_written():
            events = (await api.get("/admin/events?since=0&limit=100")).json()
            kinds = [e["kind"] for e in events if e["node_id"] == "r1-primary"]
            return events if "node_down" in kinds and kinds[-1] == "node_up" else False

        events = await eventually(events_written)
        assert [e["event_id"] for e in events] == sorted(e["event_id"] for e in events)
        latest = (await api.get("/admin/events?limit=2")).json()
        assert len(latest) == 2
        assert latest[0]["event_id"] < latest[1]["event_id"]

        exceptions = (await api.get("/admin/exceptions?limit=2")).json()
        assert [entry["seq"] for entry in exceptions] == [2, 3]
        assert [
            entry["seq"] for entry in (await api.get("/admin/exceptions?since=1&limit=1")).json()
        ] == [2]


async def test_health_event_retry_after_commit_is_idempotent(live, monkeypatch):
    cluster, settings, config, hq, _, _ = live
    runtime = service.Coordinator(cluster, settings, config)
    await runtime.pools[HQ_PRIMARY].open()
    marker = str(uuid4())
    runtime.pending_events.append(
        {
            "occurred_at": datetime.now(UTC),
            "kind": "node_suspect",
            "region_id": "r1",
            "node_id": "r1-primary",
            "event_key": marker,
            "detail": Jsonb({"coordinator_event_id": marker}),
        }
    )
    real_run_on = service.run_on

    async def committed_but_unconfirmed(*args, **kwargs):
        await real_run_on(*args, **kwargs)
        raise RegionUnreachableError(region_id="hq")

    try:
        monkeypatch.setattr(service, "run_on", committed_but_unconfirmed)
        await runtime.flush_events()
        assert len(runtime.pending_events) == 1
        monkeypatch.setattr(service, "run_on", real_run_on)
        await runtime.flush_events()
        assert runtime.pending_events == []
        cursor = await hq.execute(
            "SELECT count(*) FROM cluster_events WHERE detail ->> 'coordinator_event_id' = %s",
            (marker,),
        )
        assert await cursor.fetchone() == (1,)
    finally:
        await runtime.close()


async def test_slow_heartbeat_is_bounded_and_gateway_prevents_false_down(live):
    cluster, settings, config, _, _, api = live
    runtime = service.Coordinator(cluster, settings, config)
    name = proxy_name("coordinator", "r1-primary")
    toxic_name = "coordinator-test-latency"
    try:
        await runtime._heartbeat("r1-primary")
        assert runtime.health["r1-primary"].response.status == "up"
        (
            await api.post(
                f"/proxies/{name}/toxics",
                json={
                    "name": toxic_name,
                    "type": "latency",
                    "stream": "downstream",
                    "attributes": {"latency": 2000, "jitter": 0},
                },
            )
        ).raise_for_status()
        loop = asyncio.get_running_loop()
        for _ in range(config.heartbeat_misses):
            started = loop.time()
            await runtime._heartbeat("r1-primary")
            assert loop.time() - started < config.heartbeat_timeout_s + 1
        assert runtime.health["r1-primary"].response.status == "suspect"
        assert all(event["kind"] != "node_down" for event in runtime.pending_events)
    finally:
        response = await api.delete(f"/proxies/{name}/toxics/{toxic_name}")
        assert response.status_code in (204, 404)
        from restaurantos.common.deadline import abandoned_calls

        async def settled():
            return abandoned_calls() == 0

        await eventually(settled)
        await runtime.close()

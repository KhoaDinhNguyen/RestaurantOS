"""T1/T2 and restart recovery on an explicitly disposable dev cluster.

Promotion changes a physical PostgreSQL server, not just a test database.
Run only on a separate freshly created cluster, with
ROS_FAILOVER_TEST_CLUSTER=1 ROS_NETWORK=compose uv run pytest -m cluster
tests/cluster/test_coordinator_failover.py. Recreate that cluster afterwards.
"""

import asyncio
import os
import socket
import sys
import tempfile
from unittest.mock import AsyncMock

import httpx
import psycopg
import pytest

from restaurantos.common.cluster import HQ_PRIMARY, link_groups, load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import find_local_primary, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
from restaurantos.coordinator import failover
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.service import Coordinator

pytestmark = pytest.mark.cluster


class CoordinatorStopped(Exception):
    """Simulated process death after a durable database step."""


async def eventually(check):
    for _ in range(200):
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("failover did not reach the expected state")


@pytest.fixture
async def disposable():
    if os.environ.get("ROS_FAILOVER_TEST_CLUSTER") != "1":
        pytest.skip("requires a disposable cluster: ROS_FAILOVER_TEST_CLUSTER=1")
    cluster = load_cluster()
    settings = Settings(
        _env_file=None,
        client="coordinator",
        network=os.environ.get("ROS_NETWORK", "compose"),
        request_deadline_s=0.8,
    )
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=2) as api:
        originals = (await api.get("/proxies")).raise_for_status().json()
        assert all(p["enabled"] and not p["toxics"] for p in originals.values())
        async with await psycopg.AsyncConnection.connect(
            node_conninfo(cluster, HQ_PRIMARY, settings=settings), autocommit=True
        ) as hq:
            await hq.execute(
                "INSERT INTO region_state (region_id, primary_node, standby_node)"
                " VALUES ('r1', 'r1-primary', 'r1-standby') ON CONFLICT DO NOTHING"
            )
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        with tempfile.TemporaryFile() as log:
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
                    ROS_NETWORK=settings.network,
                    ROS_REQUEST_DEADLINE_S="0.4",
                ),
                stdout=log,
                stderr=log,
            )
            try:
                async with httpx.AsyncClient(base_url=url) as client:

                    async def ready():
                        if gateway.returncode is not None:
                            log.seek(0)
                            pytest.fail(log.read().decode())
                        try:
                            return (await client.get("/health")).status_code == 200
                        except httpx.HTTPError:
                            return False

                    await eventually(ready)
                config = CoordinatorSettings(
                    _env_file=None,
                    heartbeat_interval_s=0.1,
                    heartbeat_timeout_s=0.3,
                    gateway_timeout_s=1,
                    gateway_url=url,
                )
                yield cluster, settings, config, api
            finally:
                for name in originals:
                    (await api.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
                if gateway.returncode is None:
                    gateway.terminate()
                await gateway.wait()


async def test_T1_T2_safe_promotion_and_restart_after_every_step(disposable, monkeypatch):
    cluster, settings, config, api = disposable

    async def new_runtime():
        runtime = Coordinator(cluster, settings, config)
        for pool in runtime.pools.values():
            await pool.open()
        return runtime

    async def enabled(names, value):
        for name in names:
            (await api.post(f"/proxies/{name}", json={"enabled": value})).raise_for_status()

    async def directory(runtime):
        await runtime._directory()
        row = runtime.regions["r1"]
        return row.epoch, row.primary_node

    runtime = await new_runtime()
    try:
        assert await directory(runtime) == (1, "r1-primary")
        assert (await runtime.failover.probe("r1-standby")).in_recovery
        # Another coordinator (or an abandoned call) owns this decision.
        async with await psycopg.AsyncConnection.connect(
            node_conninfo(cluster, HQ_PRIMARY, settings=settings)
        ) as owner:
            await owner.execute("SELECT 1 FROM region_state WHERE region_id = 'r1' FOR UPDATE")
            with monkeypatch.context() as patch:
                probe = AsyncMock(wraps=runtime.failover.probe)
                patch.setattr(runtime.failover, "probe", probe)
                await runtime._reconcile_regions()
                probe.assert_not_awaited()
            assert (await runtime.failover.probe("r1-standby")).in_recovery
        coordinator_primary = proxy_name("coordinator", "r1-primary")
        gateway_primary = proxy_name("gateway", "r1-primary")
        # Below the miss threshold, both paths being cut still cannot promote.
        await enabled([coordinator_primary, gateway_primary], False)
        await runtime._heartbeat("r1-primary")
        await runtime._reconcile_regions()
        assert (await runtime.failover.probe("r1-standby")).in_recovery
        # Independent reachable gateway prevents false failover.
        await enabled([gateway_primary], True)
        for _ in range(config.heartbeat_misses):
            await runtime._heartbeat("r1-primary")
        assert runtime.health["r1-primary"].response.status == "suspect"
        await runtime._reconcile_regions()
        assert (await runtime.failover.probe("r1-standby")).in_recovery

        # T2: neither node reachable from either path. Record, never promote.
        names = [
            gateway_primary,
            proxy_name("coordinator", "r1-standby"),
            proxy_name("gateway", "r1-standby"),
        ]
        await enabled(names, False)
        for node in ("r1-primary", "r1-standby"):
            for _ in range(config.heartbeat_misses):
                await runtime._heartbeat(node)
        await runtime._reconcile_regions()
        await runtime._reconcile_regions()
        assert await directory(runtime) == (1, "r1-primary")
        assert len([e for e in await runtime.events(0, 100) if e.kind == "partition"]) == 1

        # T1: cut every link to the old primary, restore standby access.
        await enabled(
            [proxy_name("coordinator", "r1-standby"), proxy_name("gateway", "r1-standby")], True
        )
        await enabled(link_groups(cluster)["node:r1-primary"], False)
        original = runtime.failover.node_call

        async def stop_after_promote(node, query, params):
            result = await original(node, query, params)
            if query == "promote":
                raise CoordinatorStopped
            return result

        monkeypatch.setattr(runtime.failover, "node_call", stop_after_promote)
        with pytest.raises(CoordinatorStopped):
            await runtime._reconcile_regions()
    finally:
        await runtime.close()

    runtime = await new_runtime()
    try:

        async def promoted():
            return (await runtime.failover.probe("r1-standby")).writable

        await eventually(promoted)
        assert (await runtime.failover.probe("r1-standby")).epoch == 1
        assert await directory(runtime) == (1, "r1-primary")
        original = runtime.failover.node_call

        async def stop_after_epoch(node, query, params):
            result = await original(node, query, params)
            if query == "promotion_epoch":
                raise CoordinatorStopped
            return result

        monkeypatch.setattr(runtime.failover, "node_call", stop_after_epoch)
        with pytest.raises(CoordinatorStopped):
            await runtime._reconcile_regions()
        assert (await runtime.failover.probe("r1-standby")).epoch == 2
        assert await directory(runtime) == (1, "r1-primary")
    finally:
        await runtime.close()

    runtime = await new_runtime()
    try:
        # HQ must not expose the new directory without its matching event.
        async def stop_before_event(*args):
            raise CoordinatorStopped

        monkeypatch.setattr(runtime.failover, "event", stop_before_event)
        with pytest.raises(CoordinatorStopped):
            await runtime._reconcile_regions()
        assert await directory(runtime) == (1, "r1-primary")
        assert not [e for e in await runtime.events(0, 100) if e.kind == "failover"]
    finally:
        await runtime.close()

    runtime = await new_runtime()
    try:
        real_run_on = failover.run_on

        async def lost_commit_ack(*args, **kwargs):
            result = await real_run_on(*args, **kwargs)
            if args[0] is runtime.pools[HQ_PRIMARY]:
                raise RegionUnreachableError(region_id="hq")
            return result

        with monkeypatch.context() as patch:
            patch.setattr(failover, "run_on", lost_commit_ack)
            await runtime._reconcile_regions()
        assert await directory(runtime) == (2, "r1-standby")
    finally:
        await runtime.close()

    runtime = await new_runtime()
    try:
        await enabled(link_groups(cluster)["node:r1-primary"], True)
        # Actual background scheduling after restart, both servers writable.
        # The old primary's lower epoch must never cause a promotion back.
        await runtime.start()
        await asyncio.sleep(0.6)
        assert await directory(runtime) == (2, "r1-standby")
        events = [e for e in await runtime.events(0, 100) if e.kind == "failover"]
        assert len(events) == 1
        assert events[0].node_id == "r1-standby"
        assert await find_local_primary(cluster, "r1", "r1-primary", settings) == "r1-standby"

        async def fence(conn):
            await conn.execute("SELECT assert_epoch(2)")

        # Current gateway epoch is rejected by the old node after healing.
        with pytest.raises(StaleEpochError):
            await run_on(runtime.pools["r1-primary"], fence, settings=settings)
        await run_on(runtime.pools["r1-standby"], fence, settings=settings)
    finally:
        await runtime.close()

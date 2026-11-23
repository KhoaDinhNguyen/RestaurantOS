"""Real crash, rewind, missing rewind prerequisites, and lost-WAL rebuild.

Requires a fresh disposable dev topology with no command-line override for max_slot_wal_keep_size,
Docker CLI access, ROS_REJOIN_TEST_COMPOSE pointing at
its Compose file, and ROS_NETWORK matching where this test runs. Never point
this at a populated development cluster: its primary will be killed.
"""

import asyncio
import json
import os
import socket
import sys
import tempfile
from pathlib import Path

import httpx
import psycopg
import pytest

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster, proxy_for, slot_name
from restaurantos.common.config import Settings
from restaurantos.common.db import node_conninfo, probe_node, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.rejoin import RecoveryError, command, recover
from restaurantos.coordinator.service import Coordinator

pytestmark = pytest.mark.cluster


async def eventually(check):
    for _ in range(200):
        value = await check()
        if value:
            return value
        await asyncio.sleep(0.1)
    raise AssertionError("recovery did not reach the expected state")


@pytest.fixture
async def recovery_cluster():
    file = os.environ.get("ROS_REJOIN_TEST_COMPOSE")
    if not file:
        pytest.skip("requires ROS_REJOIN_TEST_COMPOSE pointing at a disposable cluster")
    compose = ["docker", "compose", "-f", file]
    definition = json.loads(await command([*compose, "config", "--format", "json"]))
    assert definition["name"] != "restaurantos", "use a separate disposable Compose project"
    cluster = load_cluster()
    settings = Settings(
        _env_file=None,
        client="coordinator",
        network=os.environ.get("ROS_NETWORK", "host"),
        request_deadline_s=5,
        connect_timeout_s=3,
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    config = CoordinatorSettings(
        _env_file=None,
        heartbeat_interval_s=0.5,
        heartbeat_misses=2,
        heartbeat_timeout_s=0.8,
        gateway_timeout_s=5,
        gateway_url=url,
        rejoin_timeout_s=60,
    )
    runtime = Coordinator(cluster, settings, config)
    for pool in runtime.pools.values():
        await pool.open()

    async def sql(node, statement, params=None):
        async def execute(conn):
            cursor = await conn.execute(statement, params)
            return await cursor.fetchall() if cursor.description else []

        return await run_on(runtime.pools[node], execute, settings=settings)

    gateway = None
    with tempfile.TemporaryFile() as log:
        try:
            assert (await sql("r1-primary", "SELECT epoch FROM node_state")) == [(1,)]
            for node in ("r1-primary", "r1-standby"):
                assert await sql(
                    node, "SELECT source FROM pg_settings WHERE name = 'max_slot_wal_keep_size'"
                ) != [("command line",)]
            assert (await sql("r1-standby", "SELECT pg_is_in_recovery()")) == [(True,)]
            await sql(
                HQ_PRIMARY,
                "INSERT INTO region_state (region_id, primary_node, standby_node)"
                " VALUES ('r1', 'r1-primary', 'r1-standby')",
            )
            # Keep the pre-fork checkpoint WAL for the successful-rewind case.
            # Normal deployments may have recycled it; that must rebuild.
            for node in ("r1-primary", "r1-standby"):
                async with await psycopg.AsyncConnection.connect(
                    node_conninfo(cluster, node, settings=settings), autocommit=True
                ) as admin:
                    await admin.execute("ALTER SYSTEM SET wal_keep_size = '64MB'")
                    await admin.execute("SELECT pg_reload_conf()")
            await sql("r1-primary", "CREATE TABLE recovery_probe (id integer PRIMARY KEY)")
            await sql("r1-primary", "INSERT INTO recovery_probe VALUES (1)")
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
                    ROS_REQUEST_DEADLINE_S="3",
                ),
                stdout=log,
                stderr=log,
            )
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
            yield cluster, settings, config, runtime, sql, compose, Path(file)
        finally:
            await runtime.close()
            if gateway is not None:
                if gateway.returncode is None:
                    gateway.terminate()
                await gateway.wait()


async def test_T1b_crash_rewind_and_rebuild_fallbacks(recovery_cluster):
    cluster, settings, config, runtime, sql, compose, file = recovery_cluster
    primary, old = "r1-standby", "r1-primary"
    slot = slot_name(old)
    reverse = proxy_for(cluster, old, primary)

    async def marker_on(node, marker):
        try:
            return (await sql(node, "SELECT id FROM recovery_probe WHERE id = %s", (marker,))) == [
                (marker,)
            ]
        except psycopg.errors.UndefinedTable, RegionUnreachableError:
            return False

    async def first_replicated():
        return await marker_on(primary, 1)

    await eventually(first_replicated)
    # This is a real unclean server stop, not a simulated database error.
    await command([*compose, "kill", "-s", "SIGKILL", old])
    await runtime.start()

    async def promoted():
        return (await sql(HQ_PRIMARY, "SELECT epoch, primary_node FROM region_state")) == [
            (2, primary)
        ]

    await eventually(promoted)
    await sql(primary, "INSERT INTO recovery_probe VALUES (2)")
    result = await recover(cluster, settings, config, "r1", file)
    assert "rewind completed" in result
    assert "rebuild completed" not in result

    async def assert_joined(marker):
        async def caught_up():
            return await marker_on(old, marker)

        await eventually(caught_up)
        view = await probe_node(old, node_conninfo(cluster, old, settings=settings), settings)
        assert view.reachable and view.in_recovery and view.epoch == 2
        assert await sql(
            primary, "SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,)
        ) == [(True,)]
        assert await sql(old, "SELECT sender_host, sender_port FROM pg_stat_wal_receiver") == [
            ("toxiproxy", reverse.port)
        ]
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            await sql(old, "INSERT INTO recovery_probe VALUES (999)")

    await assert_joined(2)
    await sql(primary, "INSERT INTO recovery_probe VALUES (3)")
    await assert_joined(3)
    # Demonstrate that replication really uses the cuttable reverse proxy.
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=2) as api:
        (await api.post(f"/proxies/{reverse.name}", json={"enabled": False})).raise_for_status()
        try:
            await sql(primary, "INSERT INTO recovery_probe VALUES (4)")
            await asyncio.sleep(0.3)
            assert not await marker_on(old, 4)
        finally:
            (await api.post(f"/proxies/{reverse.name}", json={"enabled": True})).raise_for_status()
    await assert_joined(4)

    # Remove a genuine pg_rewind prerequisite from the stopped target.
    await command([*compose, "stop", "--timeout", "30", old])
    await command(
        [
            *compose,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "bash",
            old,
            "-c",
            'gosu postgres pg_checksums --disable -D "$PGDATA"',
        ]
    )
    result = await recover(cluster, settings, config, "r1", file)
    assert "rewind failed" in result and "rebuild completed" in result
    await assert_joined(4)

    # Exhaust the small test-only retention limit and check a genuinely lost
    # physical slot. Production retains its documented 2GB limit unchanged.
    await command([*compose, "stop", "--timeout", "30", old])
    async with await psycopg.AsyncConnection.connect(
        node_conninfo(cluster, primary, settings=settings), autocommit=True
    ) as admin:
        await admin.execute("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
        await admin.execute("ALTER SYSTEM SET wal_keep_size = '0'")
        await admin.execute("SELECT pg_reload_conf()")

    async def smaller_limit():
        return (await sql(primary, "SHOW max_slot_wal_keep_size")) == [("1MB",)]

    await eventually(smaller_limit)
    for _ in range(4):
        await sql(primary, "SELECT pg_switch_wal()")
    await sql(primary, "CHECKPOINT")
    assert await sql(
        primary, "SELECT wal_status FROM pg_replication_slots WHERE slot_name = %s", (slot,)
    ) == [("lost",)]
    result = await recover(cluster, settings, config, "r1", file)
    assert "lost slot requires a fresh clone" in result
    assert "rebuild completed" in result
    assert "rewind completed" not in result
    await sql(primary, "INSERT INTO recovery_probe VALUES (5)")
    await assert_joined(5)

    # An unavailable source path must leave the target stopped, not start
    # either an unrecovered primary or a partial backup.
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=2) as api:
        (await api.post(f"/proxies/{reverse.name}", json={"enabled": False})).raise_for_status()
        try:
            with pytest.raises(RecoveryError):
                await recover(
                    cluster, settings, config.model_copy(update={"rejoin_timeout_s": 3}), "r1", file
                )
            assert not (await runtime.failover.probe(old)).reachable
            assert (await runtime.failover.probe(primary)).writable
        finally:
            (await api.post(f"/proxies/{reverse.name}", json={"enabled": True})).raise_for_status()
    await recover(cluster, settings, config, "r1", file)
    await assert_joined(5)
    assert await sql(HQ_PRIMARY, "SELECT epoch, primary_node FROM region_state") == [(2, primary)]

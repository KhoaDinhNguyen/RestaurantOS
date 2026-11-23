"""D10 on a fresh disposable cluster; physically crashes/stops the HQ primary.

Requires ROS_HQ_TEST_COMPOSE, Docker access and ROS_NETWORK. Recreate the
scratch cluster after the run. Never use a populated development topology.
"""

import asyncio
import json
import os
from pathlib import Path

import httpx
import pytest

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.sqlfiles import load_query
from restaurantos.coordinator import hq_promotion as hq
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.rejoin import command as lifecycle_command

pytestmark = pytest.mark.cluster


@pytest.mark.parametrize("crash", [False, True], ids=["healthy", "crashed"])
async def test_hq_promotion_lock_and_restart_recovery(monkeypatch, crash):
    file = os.environ.get("ROS_HQ_TEST_COMPOSE")
    if not file:
        pytest.skip("requires ROS_HQ_TEST_COMPOSE pointing at a fresh disposable cluster")
    compose = ["docker", "compose", "-f", file]
    definition = json.loads(await hq.command([*compose, "config", "--format", "json"]))
    assert definition["name"].startswith("restaurantos-hq-"), "use an HQ scratch project"
    # Each case needs the original physical roles and its own empty volumes.
    # The explicit opt-in and project guard above protect the normal cluster.
    await lifecycle_command([*compose, "down", "--volumes"])
    await lifecycle_command([*compose, "up", "-d", "--wait"])
    cluster = load_cluster()
    settings = Settings(
        _env_file=None,
        client="coordinator",
        network=os.environ.get("ROS_NETWORK", "host"),
        connect_timeout_s=2,
        request_deadline_s=4,
    )
    config = CoordinatorSettings(_env_file=None)
    pools = {
        n: make_pool(node_conninfo(cluster, n, settings=settings), settings)
        for n in (HQ_PRIMARY, HQ_STANDBY)
    }
    for pool in pools.values():
        await pool.open()

    async def sql(node, statement):
        async def query(conn):
            cursor = await conn.execute(statement)
            return await cursor.fetchall() if cursor.description else None

        return await run_on(pools[node], query, settings=settings)

    async def promote():
        return await hq.promote(cluster, settings, config, Path(file))

    old_id = (await hq.command([*compose, "ps", "--all", "--quiet", HQ_PRIMARY])).strip()
    name = proxy_name("coordinator", HQ_STANDBY)
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=3) as toxiproxy:
        original = (await toxiproxy.get(f"/proxies/{name}")).raise_for_status().json()
        assert original["enabled"] and not original["toxics"]
        try:
            assert await sql(HQ_STANDBY, "SELECT pg_is_in_recovery()") == [(True,)]
            await sql(
                HQ_PRIMARY, "INSERT INTO menu_versions (version, note) VALUES (2, 'hq-crash-probe')"
            )
            for _ in range(100):
                if await sql(HQ_STANDBY, "SELECT note FROM menu_versions WHERE version=2"):
                    break
                await asyncio.sleep(0.05)
            assert await sql(HQ_STANDBY, "SELECT note FROM menu_versions WHERE version=2") == [
                ("hq-crash-probe",)
            ]
            # The process-local lock is insufficient: hold the candidate's
            # database lock using a different session, as another CLI would.
            async with pools[HQ_STANDBY].connection() as conn:
                assert await (
                    await conn.execute(load_query("coordinator/hq_promotion_lock"))
                ).fetchone() == (True,)
                with pytest.raises(hq.HQPromotionError, match="already running"):
                    await promote()
            assert (await hq.inspect_container(old_id))["State"]["Running"]
            # Refused target must not stop the old primary.
            (await toxiproxy.post(f"/proxies/{name}", json={"enabled": False})).raise_for_status()
            with pytest.raises(RegionUnreachableError):
                await promote()
            assert (await hq.inspect_container(old_id))["State"]["Running"]
            (await toxiproxy.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
            # Toxiproxy closed the test observer's existing connection too.
            # Observe promotion using a fresh pool after healing the link.
            await pools[HQ_STANDBY].close()
            pools[HQ_STANDBY] = make_pool(
                node_conninfo(cluster, HQ_STANDBY, settings=settings), settings
            )
            await pools[HQ_STANDBY].open()
            if crash:
                await hq.command([*compose, "kill", "-s", "KILL", HQ_PRIMARY])
                assert not (await hq.inspect_container(old_id))["State"]["Running"]
            original_run = hq.run_on
            interrupted_at_record = False

            async def interrupted(pool, work, **kwargs):
                nonlocal interrupted_at_record
                if work.__name__ == "record":
                    interrupted_at_record = True
                    raise RegionUnreachableError(
                        "simulated CLI restart before event", region_id="hq"
                    )
                return await original_run(pool, work, **kwargs)

            monkeypatch.setattr(hq, "run_on", interrupted)
            with pytest.raises(RegionUnreachableError):
                await promote()
            assert interrupted_at_record
            assert await sql(HQ_STANDBY, "SELECT pg_is_in_recovery()") == [(False,)]
            stopped = await hq.inspect_container(old_id)
            assert not stopped["State"]["Running"]
            assert stopped["HostConfig"]["RestartPolicy"]["Name"] == "no"
            monkeypatch.setattr(hq, "run_on", original_run)
            result = await promote()
            assert result["primary_node"] == HQ_STANDBY
            assert result["old_primary_stopped"] and result["client_switch_required"]
            assert await sql(HQ_STANDBY, "SELECT note FROM menu_versions WHERE version=2") == [
                ("hq-crash-probe",)
            ]
            # A real write proves the promoted database, not just a role flag.
            await sql(
                HQ_STANDBY,
                "INSERT INTO menu_versions (version, note) VALUES (3, 'after-promotion')",
            )
            assert (await promote())["event_id"] == result["event_id"]
            events = await sql(
                HQ_STANDBY,
                "SELECT cluster_event_id, region_id, node_id FROM cluster_events "
                "WHERE detail->>'scope'='hq'",
            )
            assert events == [(result["event_id"], None, HQ_STANDBY)]
        finally:
            (await toxiproxy.post(f"/proxies/{name}", json={"enabled": True})).raise_for_status()
            for pool in pools.values():
                await pool.close()

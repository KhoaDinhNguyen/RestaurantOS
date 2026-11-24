"""Real services under concurrent orders, on an explicitly disposable topology.

ROS_WP5_ACCEPTANCE_COMPOSE must name a generated synchronous one-region
project whose name begins restaurantos-wp5-. This test deletes its volumes.
Uses only the public mini fixture. It is functional acceptance, not a
performance benchmark or the full-data workload experiment.
"""

import asyncio
import csv
import json
import os
import time
from pathlib import Path

import httpx
import pytest
from tests.cluster.wp5_support import cleanup_acceptance, produce_orders

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster, link_groups, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.hq import HQPool
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.hq_promotion import HQPromotionError, inspect_container, promote
from restaurantos.coordinator.hq_rejoin import recover as rejoin_hq
from restaurantos.coordinator.rejoin import command
from restaurantos.coordinator.rejoin import recover as rejoin_region
from restaurantos.coordinator.service import Coordinator
from restaurantos.gateway import app as gateway
from restaurantos.gateway.config import GatewaySettings
from restaurantos.region import apply_menu_version, place_order
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.holding import run_hq_worker
from restaurantos.relay.worker import run_region_relay
from restaurantos.sim.bootstrap import _load_reference
from restaurantos.sim.loading import copy_async, reference_rows
from restaurantos.sim.mini import mini_dir

pytestmark = pytest.mark.cluster


async def eventually(check, seconds=40):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.1)
    raise AssertionError(f"condition not met within {seconds} seconds")


async def test_wp5_recovery_under_orders(monkeypatch, tmp_path):
    file = os.environ.get("ROS_WP5_ACCEPTANCE_COMPOSE")
    if not file:
        pytest.skip("requires ROS_WP5_ACCEPTANCE_COMPOSE on a disposable generated sync topology")
    compose = ["docker", "compose", "-f", file]
    definition = json.loads(await command([*compose, "config", "--format", "json"]))
    assert definition["name"].startswith("restaurantos-wp5-")
    assert definition["services"]["r1-primary"]["environment"]["ROS_SYNC_STANDBY"] == "r1_standby"
    assert definition["services"][HQ_PRIMARY]["environment"]["ROS_SYNC_STANDBY"] == "hq_standby"
    await command([*compose, "down", "--volumes"])
    await command([*compose, "up", "-d", "--wait"])
    cluster = Cluster(synchronous=True)
    network = os.environ.get("ROS_NETWORK", "host")
    settings = Settings(
        _env_file=None,
        client="coordinator",
        network=network,
        connect_timeout_s=2,
        request_deadline_s=5,
    )
    pools = {
        n: make_pool(node_conninfo(cluster, n, settings=settings), settings) for n in cluster.nodes
    }
    for pool in pools.values():
        await pool.open()

    async def sql(node, text, params=None):
        async def work(conn):
            cur = await conn.execute(text, params)
            return await cur.fetchall() if cur.description else None

        return await run_on(pools[node], work, settings=settings)

    async def rows(node, text, expected):
        try:
            return await sql(node, text) == expected
        except RegionUnreachableError:
            # Faults close the observer pool's connections too. A subsequent
            # checkout reconnects; never weaken the expected final state.
            return False

    async def enable_sync(node, standby):
        async with make_pool(
            node_conninfo(cluster, node, settings=settings), settings
        ) as config_pool:

            async def enable(conn):
                await conn.set_autocommit(True)
                await conn.execute(
                    f"ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 ({standby})'"
                )
                await conn.execute("SELECT pg_reload_conf()")

            await run_on(config_pool, enable, settings=settings)
        await eventually(
            lambda: rows(
                node, "SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'", [(1,)]
            )
        )

    async def seed(node, hq=False):
        async def load(conn):
            await _load_reference(conn, reference_rows(mini_dir()))
            if hq:
                await conn.execute(
                    "INSERT INTO region_state (region_id,primary_node,standby_node) "
                    "VALUES ('r1','r1-primary','r1-standby')"
                )
                await conn.execute(
                    "INSERT INTO shard_directory SELECT DISTINCT geohash,'r1' FROM vendors"
                )
            else:
                with (mini_dir() / "stock.csv").open() as source:
                    stock = [
                        (r["vendor_id"], r["item_kind"], r["item_id"], 10000, 10000)
                        for r in csv.DictReader(source)
                    ]
                await copy_async(
                    conn,
                    "stock",
                    ("vendor_id", "item_kind", "item_id", "on_hand", "par_level"),
                    stock,
                )

        await run_on(pools[node], load, settings=settings)

    await seed(HQ_PRIMARY, True)
    await seed("r1-primary")
    assert await sql(
        HQ_PRIMARY, "SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'"
    ) == [(1,)]
    assert await sql(
        "r1-primary", "SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'"
    ) == [(1,)]
    assert await sql(
        "r1-primary", "SELECT source FROM pg_settings WHERE name='synchronous_standby_names'"
    ) == [("configuration file",)]
    monkeypatch.setattr(gateway, "load_cluster", lambda *a: cluster)
    monkeypatch.setattr(
        gateway,
        "get_gateway_settings",
        lambda: GatewaySettings(_env_file=None, directory_refresh_s=0.2, local_check_s=0.2),
    )
    monkeypatch.setattr(
        gateway,
        "get_settings",
        lambda: settings.model_copy(update={"client": "gateway", "mode": "gateway"}),
    )
    app = gateway.create_app()
    monkeypatch.setattr(
        gateway,
        "get_settings",
        lambda: settings.model_copy(
            update={"client": "relay-r1", "mode": "edge", "region_id": "r1"}
        ),
    )
    edge = gateway.create_app()
    config = CoordinatorSettings(
        _env_file=None,
        heartbeat_interval_s=0.2,
        heartbeat_misses=3,
        heartbeat_timeout_s=0.8,
        gateway_timeout_s=1,
        gateway_url="http://gateway.test",
        rejoin_timeout_s=120,
    )
    runtime = Coordinator(cluster, settings, config)
    await runtime.http.aclose()
    runtime.http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gateway.test"
    )
    workers_stop, producers_stop = asyncio.Event(), asyncio.Event()
    workers, producers, acknowledgements = [], [], {}
    retry_count = 0
    async with httpx.AsyncClient(base_url=settings.toxiproxy_url, timeout=3) as tox:

        async def cut(names, enabled):
            for name in names:
                (await tox.post(f"/proxies/{name}", json={"enabled": enabled})).raise_for_status()

        try:
            async with app.router.lifespan_context(app), edge.router.lifespan_context(edge):
                await runtime.start()
                workers = [
                    asyncio.create_task(
                        run_region_relay(
                            cluster,
                            "r1",
                            settings.model_copy(update={"client": "relay-r1"}),
                            RelaySettings(
                                _env_file=None,
                                role="region",
                                poll_interval_s=0.1,
                                local_check_s=0.2,
                            ),
                            stop_event=workers_stop,
                        )
                    ),
                    asyncio.create_task(
                        run_hq_worker(
                            cluster,
                            settings.model_copy(update={"client": "hq"}),
                            RelaySettings(_env_file=None, role="hq", poll_interval_s=0.1),
                            place_order,
                            apply_menu_version,
                            stop_event=workers_stop,
                        )
                    ),
                ]
                async with (
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="http://gateway.test"
                    ) as api,
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=edge), base_url="http://edge.test"
                    ) as local,
                ):
                    producers = [
                        asyncio.create_task(
                            produce_orders(api, "gateway", producers_stop, acknowledgements)
                        ),
                        asyncio.create_task(
                            produce_orders(local, "edge", producers_stop, acknowledgements)
                        ),
                    ]

                    async def progress(minimum):
                        for task in producers + workers + runtime.tasks:
                            if task.done():
                                task.result()
                                raise AssertionError("load task ended before shutdown")
                        return len(acknowledgements) >= minimum

                    await eventually(lambda: progress(10))
                    # Both generated sync policies actually release waiting
                    # writes when their standby disappears; HQ journals its
                    # own downgrade locally, before the config change.
                    for primary, standby, _scope in [
                        (HQ_PRIMARY, HQ_STANDBY, "hq"),
                        ("r1-primary", "r1-standby", "r1"),
                    ]:
                        before = len(acknowledgements)
                        await cut([proxy_name(standby, primary)], False)
                        await eventually(
                            lambda primary=primary: rows(
                                primary, "SHOW synchronous_standby_names", [("",)]
                            )
                        )
                        await eventually(lambda minimum=before + 4: progress(minimum))
                        await cut([proxy_name(standby, primary)], True)
                        await eventually(
                            lambda primary=primary: rows(
                                primary,
                                "SELECT count(*) FROM pg_stat_replication WHERE state='streaming'",
                                [(1,)],
                            )
                        )
                    print("HQ and region sync fallback passed")
                    # Re-enable explicitly for durable acknowledgements during
                    # promotion. Fallback never silently re-enables sync.
                    await enable_sync("r1-primary", "r1_standby")
                    await enable_sync(HQ_PRIMARY, "hq_standby")
                    # T1: old region primary remains running but is isolated
                    # from HQ. Edge and relay must move via the higher epoch.
                    links = link_groups(cluster)["node:r1-primary"]
                    await cut(links, False)
                    await eventually(
                        lambda: rows(
                            HQ_PRIMARY,
                            "SELECT epoch,primary_node FROM region_state WHERE region_id='r1'",
                            [(2, "r1-standby")],
                        )
                    )
                    await eventually(
                        lambda: asyncio.sleep(
                            0, result=edge.state.edge_primary.current == "r1-standby"
                        )
                    )
                    await cut(links, True)
                    output = await rejoin_region(cluster, settings, config, "r1", Path(file))
                    assert "rejoined as standby" in output
                    print("Regional T1 and rejoin passed")
                    # T1b: a real crash with synchronous acknowledgements.
                    await enable_sync("r1-standby", "r1_primary")
                    await command([*compose, "kill", "-s", "KILL", "r1-standby"])
                    await eventually(
                        lambda: rows(
                            HQ_PRIMARY,
                            "SELECT epoch,primary_node FROM region_state WHERE region_id='r1'",
                            [(3, "r1-primary")],
                        )
                    )
                    await eventually(
                        lambda: asyncio.sleep(
                            0, result=edge.state.edge_primary.current == "r1-primary"
                        )
                    )
                    output = await rejoin_region(cluster, settings, config, "r1", Path(file))
                    assert "rejoined as standby" in output
                    region_primary = "r1-primary"
                    print("Regional T1b crash and rejoin passed")
                    # T2: local edge continues, HQ parks orders, no promotion.
                    links = link_groups(cluster)["link:r1<->hq"]
                    edge_before = sum(k.startswith("edge-") for k in acknowledgements)
                    await cut(links, False)
                    await eventually(
                        lambda: asyncio.sleep(
                            0,
                            result=sum(k.startswith("edge-") for k in acknowledgements)
                            > edge_before + 2,
                        )
                    )
                    await eventually(
                        lambda: rows(
                            HQ_PRIMARY,
                            "SELECT count(*) > 0 FROM holding_queue WHERE delivered_at IS NULL",
                            [(True,)],
                        )
                    )
                    assert await sql(
                        HQ_PRIMARY, "SELECT epoch FROM region_state WHERE region_id='r1'"
                    ) == [(3,)]
                    await cut(links, True)
                    print("Regional T2 passed")
                    # T5: a two-second coordinator-only delay must not
                    # promote while the independent gateway path is healthy.
                    name = proxy_name("coordinator", region_primary)
                    (
                        await tox.post(
                            f"/proxies/{name}/toxics",
                            json={
                                "name": "wp5-latency",
                                "type": "latency",
                                "stream": "downstream",
                                "attributes": {"latency": 2000, "jitter": 0},
                            },
                        )
                    ).raise_for_status()
                    await asyncio.sleep(3.2)
                    assert await sql(
                        HQ_PRIMARY, "SELECT epoch FROM region_state WHERE region_id='r1'"
                    ) == [(3,)]
                    (await tox.delete(f"/proxies/{name}/toxics/wp5-latency")).raise_for_status()
                    print("Regional T5 latency without false promotion passed")
                    # HQ promotion while writes are still in flight.
                    before = len(acknowledgements)
                    result = await promote(cluster, settings, config, Path(file))
                    assert result["client_switch_required"] is False
                    assert await runtime.hq_pool.select() == HQ_STANDBY
                    await eventually(
                        lambda: asyncio.sleep(0, result=len(acknowledgements) > before + 5)
                    )
                    async with HQPool(cluster, settings) as hq:

                        async def menu(conn):
                            await conn.execute("UPDATE products SET menu_version=2")
                            await conn.execute(
                                "INSERT INTO menu_versions (version,note) "
                                "VALUES (2,'after HQ promotion')"
                            )

                        await run_on(hq, menu, settings=settings)
                    await eventually(
                        lambda: rows(region_primary, "SELECT menu_version FROM node_state", [(2,)])
                    )
                    print("HQ client switching and menu publishing passed")
                    output = await rejoin_hq(cluster, settings, config, Path(file))
                    assert "rejoined as standby" in output
                    await eventually(
                        lambda: rows(HQ_PRIMARY, "SELECT pg_is_in_recovery()", [(True,)])
                    )
                    assert await runtime.hq_pool.select() == HQ_STANDBY
                    from restaurantos.coordinator.app import create_app as admin_app

                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=admin_app(runtime)),
                        base_url="http://coordinator.test",
                    ) as admin:
                        events = (
                            (await admin.get("/admin/events?since=0&limit=1000"))
                            .raise_for_status()
                            .json()
                        )
                        assert any(e["event_id"] == result["event_id"] for e in events)
                    # A repeat with an inconclusive role probe must preserve
                    # the now-running rejoined HQ standby.
                    old_id = (
                        await command([*compose, "ps", "--all", "--quiet", HQ_PRIMARY])
                    ).strip()
                    name = proxy_name("coordinator", HQ_PRIMARY)
                    await cut([name], False)
                    try:
                        with pytest.raises(HQPromotionError, match="role is unconfirmed"):
                            await promote(cluster, settings, config, Path(file))
                        assert (await inspect_container(old_id))["State"]["Running"]
                    finally:
                        await cut([name], True)
                    print("Unconfirmed repeat preserved the rejoined HQ standby")
                    producers_stop.set()
                    retry_count = sum(await asyncio.gather(*producers))

                    async def drained():
                        return await sql(
                            region_primary, "SELECT count(*) FROM outbox WHERE relayed_at IS NULL"
                        ) == [(0,)] and await sql(
                            HQ_STANDBY,
                            "SELECT count(*) FROM holding_queue WHERE delivered_at IS NULL",
                        ) == [(0,)]

                    await eventually(drained, 60)
                    stored = await sql(
                        region_primary,
                        "SELECT external_order_id FROM orders WHERE channel='wp5-load'",
                    )
                    copied = await sql(
                        HQ_STANDBY,
                        "SELECT external_order_id FROM analytics_orders WHERE channel='wp5-load'",
                    )
                    assert len(stored) == len(copied) == len(acknowledgements)
                    assert {r[0] for r in stored} == {r[0] for r in copied} == set(acknowledgements)
                    assert await sql(
                        HQ_STANDBY,
                        "SELECT count(*) FROM cluster_events WHERE kind='sync_downgrade' "
                        "AND detail->>'scope'='hq' AND detail->>'primary_node'='hq-primary'",
                    ) == [(1,)]
                    assert await sql(
                        HQ_STANDBY,
                        "SELECT count(*) FROM cluster_events WHERE kind='failover' "
                        "AND region_id='r1'",
                    ) == [(2,)]
                    for task in workers:
                        assert not task.done(), task.exception() if task.done() else None
                    report = {
                        "acknowledged": len(acknowledgements),
                        "copied_to_hq": len(copied),
                        "retries": retry_count,
                        "region_epoch": 3,
                        "hq_primary": HQ_STANDBY,
                        "fixture": "mini",
                        "synchronous_bootstrap": True,
                    }
                    (tmp_path / "acceptance.json").write_text(json.dumps(report, indent=2) + "\n")
                    print(json.dumps(report))
                workers_stop.set()
                await asyncio.gather(*workers)
                await runtime.close()
        finally:
            await cleanup_acceptance(
                tox, producers_stop, workers_stop, producers, workers, runtime, pools
            )

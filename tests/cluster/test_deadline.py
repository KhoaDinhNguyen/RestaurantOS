"""The request deadline against real Toxiproxy faults. Run locally, never in CI.

Needs the dev baseline:  docker compose -f infra/compose.yaml up -d
The test runs on the host and goes through the published proxy port, exactly
as a service started with ROS_NETWORK=host does.

    uv run pytest -m cluster tests/cluster/test_deadline.py
"""

import asyncio
import time

import httpx
import pytest

from restaurantos.common.cluster import load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.deadline import abandoned_calls
from restaurantos.common.errors import RegionUnreachableError

pytestmark = pytest.mark.cluster

NODE = "r1-primary"
PROXY = proxy_name("gateway", NODE)
DEADLINE_S = 1.0

# Both directions, as WP6b's `silence` and `latency` faults will do it.
SILENCE = {"type": "timeout", "attributes": {"timeout": 0}}
LATENCY = {"type": "latency", "attributes": {"latency": 2500, "jitter": 0}}


@pytest.fixture
def settings():
    return Settings(_env_file=None, network="host", client="gateway", request_deadline_s=DEADLINE_S)


@pytest.fixture
def toxiproxy(settings):
    api = httpx.Client(base_url=settings.toxiproxy_url, timeout=5)
    try:
        api.get("/version").raise_for_status()
    except httpx.HTTPError:
        api.close()
        pytest.skip(f"Toxiproxy is not answering on {settings.toxiproxy_url}")

    def clear():
        for toxic in api.get(f"/proxies/{PROXY}/toxics").json():
            api.delete(f"/proxies/{PROXY}/toxics/{toxic['name']}")

    clear()
    yield api
    clear()
    api.close()


def add(api, fault):
    for stream in ("upstream", "downstream"):
        body = {
            "name": f"test_{fault['type']}_{stream}",
            "stream": stream,
            "toxicity": 1.0,
            **fault,
        }
        api.post(f"/proxies/{PROXY}/toxics", json=body).raise_for_status()


async def one(conn):
    cur = await conn.execute("SELECT 1")
    return (await cur.fetchone())[0]


@pytest.mark.parametrize("fault", [SILENCE, LATENCY], ids=["silence", "latency"])
async def test_a_faulty_link_fails_at_the_deadline_and_recovers(toxiproxy, settings, fault):
    pool = make_pool(node_conninfo(load_cluster(), NODE, settings=settings), settings)
    await pool.open(wait=True, timeout=10)
    try:
        assert await run_on(pool, one, settings=settings) == 1

        # The fault lands under a connection that is already open.
        add(toxiproxy, fault)
        started = time.monotonic()
        with pytest.raises(RegionUnreachableError):
            await run_on(pool, one, region_id="r1", settings=settings)
        waited = time.monotonic() - started
        assert waited < DEADLINE_S + 0.5, f"gave up after {waited:.2f} s"

        # A second caller is bounded as well, whatever state the pool is in.
        started = time.monotonic()
        with pytest.raises(RegionUnreachableError):
            await run_on(pool, one, region_id="r1", settings=settings)
        assert time.monotonic() - started < DEADLINE_S + 0.5

        # Remove the fault: calls work again and nothing stays abandoned.
        for toxic in toxiproxy.get(f"/proxies/{PROXY}/toxics").json():
            toxiproxy.delete(f"/proxies/{PROXY}/toxics/{toxic['name']}")
        healed_by = time.monotonic() + 30
        while True:
            try:
                assert await run_on(pool, one, settings=settings) == 1
                break
            except RegionUnreachableError:
                if time.monotonic() > healed_by:
                    raise
                await asyncio.sleep(0.5)
        while abandoned_calls() and time.monotonic() < healed_by:
            await asyncio.sleep(0.5)
        assert abandoned_calls() == 0
    finally:
        await pool.close()

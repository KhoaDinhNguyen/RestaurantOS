"""The fault layer against the real Toxiproxy. Run locally, never in CI.

Needs a bootstrapped cluster (its proxies in Toxiproxy):

    uv run pytest -m cluster tests/cluster/test_faults.py

Every test ends with heal-all, so Toxiproxy is left clean.
"""

from __future__ import annotations

import httpx
import pytest

from restaurantos.common.cluster import link_groups, load_cluster, proxy_name
from restaurantos.common.config import Settings
from restaurantos.sim.faults import FaultLayer
from restaurantos.sim.toxiproxy import Toxiproxy

pytestmark = pytest.mark.cluster

SHARED = proxy_name("r1-standby", "r1-primary")  # in node:r1-primary and repl:r1


@pytest.fixture
async def layer():
    settings = Settings(_env_file=None, network="host")
    try:
        httpx.get(f"{settings.toxiproxy_url}/version", timeout=5).raise_for_status()
    except httpx.HTTPError:
        pytest.skip(f"Toxiproxy is not answering on {settings.toxiproxy_url}")
    cluster = load_cluster()
    client = Toxiproxy(settings.toxiproxy_url)
    names = {entry["name"] for entry in await client.list_proxies()}
    if SHARED not in names:
        await client.aclose()
        pytest.skip("the cluster's proxies are not in Toxiproxy: run rosctl bootstrap first")
    fault_layer = FaultLayer(client, link_groups(cluster))
    await fault_layer.heal_all()
    yield fault_layer
    await fault_layer.heal_all()
    await client.aclose()


async def _proxy(layer: FaultLayer, name: str) -> dict:
    return next(entry for entry in await layer._toxiproxy.list_proxies() if entry["name"] == name)


async def test_latency_toxics_land_named_in_both_directions_and_heal(layer) -> None:
    fault = await layer.create("repl:r1", "latency", {"latency_ms": 250, "jitter_ms": 10})
    for proxy in fault.proxies:
        toxics = await layer._toxiproxy.list_toxics(proxy)
        assert sorted(toxic["name"] for toxic in toxics) == [
            f"{fault.fault_id}-latency-down",
            f"{fault.fault_id}-latency-up",
        ]
        assert all(toxic["attributes"]["latency"] == 250 for toxic in toxics)
    await layer.heal(fault.fault_id)
    for proxy in fault.proxies:
        assert await layer._toxiproxy.list_toxics(proxy) == []


async def test_overlapping_cuts_hold_the_shared_proxy(layer) -> None:
    node = await layer.create("node:r1-primary", "cut")
    repl = await layer.create("repl:r1", "cut")
    await layer.heal(node.fault_id)
    assert (await _proxy(layer, SHARED))["enabled"] is False
    await layer.heal(repl.fault_id)
    assert (await _proxy(layer, SHARED))["enabled"] is True


async def test_a_new_layer_sees_what_the_old_one_left(layer) -> None:
    await layer.create("repl:r1", "silence")
    fresh = FaultLayer(layer._toxiproxy, layer.groups)
    links = {entry["group"]: entry for entry in await fresh.links()}
    assert len(links["repl:r1"]["unexplained"]["toxics"]) == 4
    assert await fresh.heal_all() == 0
    links = {entry["group"]: entry for entry in await fresh.links()}
    assert links["repl:r1"]["unexplained"]["toxics"] == []

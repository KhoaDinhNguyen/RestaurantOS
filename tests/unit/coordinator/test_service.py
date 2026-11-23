import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.localnode import NodeView
from restaurantos.coordinator import service
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.models import RegionStatus


@pytest.fixture
async def runtime():
    instance = service.Coordinator(
        Cluster(),
        Settings(_env_file=None, network="host", client="coordinator"),
        CoordinatorSettings(_env_file=None),
    )
    yield instance
    await instance.close()


@pytest.mark.parametrize("network, host", [("host", "127.0.0.1"), ("compose", "gateway")])
async def test_gateway_address_follows_network(network, host):
    instance = service.Coordinator(
        Cluster(),
        Settings(_env_file=None, network=network, client="coordinator"),
        CoordinatorSettings(_env_file=None),
    )
    try:
        assert instance.gateway_url == f"http://{host}:8000"
    finally:
        await instance.close()


async def test_gateway_is_only_asked_at_threshold(runtime, monkeypatch):
    probe = AsyncMock(return_value=NodeView("r1-primary"))
    monkeypatch.setattr(service, "probe_node", probe)
    await runtime.http.aclose()
    requests = []

    def reply(request):
        requests.append(request)
        return httpx.Response(200, json={"node_id": "r1-primary", "reachable": False})

    runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(reply))
    runtime.health["r1-primary"].response.status = "up"

    await runtime._heartbeat("r1-primary")
    await runtime._heartbeat("r1-primary")
    assert requests == []
    await runtime._heartbeat("r1-primary")
    assert len(requests) == 1
    assert runtime.health["r1-primary"].response.status == "down"
    assert [event["kind"] for event in runtime.pending_events] == ["node_suspect", "node_down"]


async def test_unknown_gateway_reply_is_not_a_negative_probe(runtime):
    await runtime.http.aclose()
    runtime.http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503))
    )
    assert await runtime._gateway_probe("r1-primary") is None


async def test_missing_region_keeps_previous_figures(runtime, monkeypatch):
    previous = RegionStatus(
        region_id="r1",
        epoch=1,
        menu_version=2,
        primary_node="r1-primary",
        standby_node="r1-standby",
        outbox_backlog=9,
        figures_as_of=datetime(2026, 11, 22, tzinfo=UTC),
    )
    runtime.regions["r1"] = previous
    monkeypatch.setattr(service, "run_on", AsyncMock(side_effect=RegionUnreachableError()))
    await runtime._figures(previous)
    assert runtime.regions["r1"] == previous


async def test_slow_figures_do_not_delay_node_heartbeats(runtime, monkeypatch):
    figure_started = asyncio.Event()
    heartbeats = {node: 0 for node in runtime.cluster.nodes}
    runtime.config.heartbeat_interval_s = 0.001
    pools = {node: SimpleNamespace(open=AsyncMock(), close=AsyncMock()) for node in runtime.pools}
    monkeypatch.setattr(runtime, "pools", pools)

    async def figures():
        figure_started.set()
        await runtime.stop.wait()

    async def heartbeat(node):
        await figure_started.wait()
        heartbeats[node] += 1
        if min(heartbeats.values()) >= 3:
            runtime.stop.set()

    monkeypatch.setattr(runtime, "_heartbeat", heartbeat)
    monkeypatch.setattr(runtime, "_sample_figures", figures)
    monkeypatch.setattr(runtime, "_reconcile_regions", AsyncMock())
    await runtime.start()
    await asyncio.gather(*runtime.tasks)
    assert min(heartbeats.values()) >= 3


@pytest.mark.parametrize(
    "field, value",
    [
        ("heartbeat_interval_s", 0),
        ("heartbeat_misses", 0),
        ("heartbeat_timeout_s", float("inf")),
    ],
)
def test_invalid_heartbeat_settings(field, value):
    with pytest.raises(ValueError):
        CoordinatorSettings(_env_file=None, **{field: value})


async def test_negative_hq_gateway_probe_is_confirmed(runtime):
    await runtime.http.aclose()
    runtime.http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"node_id": "hq-primary", "reachable": False})
        )
    )
    assert await runtime._gateway_probe("hq-primary") is False


async def test_order_rate_uses_sample_difference_and_resets_after_stats_reset(runtime, monkeypatch):
    region = RegionStatus(
        region_id="r1",
        epoch=1,
        menu_version=1,
        primary_node="r1-primary",
        standby_node="r1-standby",
    )
    runtime.regions["r1"] = region
    base = {
        "figures_as_of": datetime(2026, 11, 22, tzinfo=UTC),
        "outbox_backlog": 4,
        "hq_lag_s": 1.2,
        "standby_lag_s": None,
        "replication_mode": "async",
    }
    monkeypatch.setattr(
        service,
        "run_on",
        AsyncMock(side_effect=[{**base, "total_orders": n} for n in (10, 16, 1)]),
    )
    times = iter([100.0, 102.0, 104.0])
    monkeypatch.setattr(service, "time", SimpleNamespace(monotonic=lambda: next(times)))
    await runtime._figures(runtime.regions["r1"])
    assert runtime.regions["r1"].orders_per_s is None
    await runtime._figures(runtime.regions["r1"])
    assert runtime.regions["r1"].orders_per_s == 3.0
    await runtime._figures(runtime.regions["r1"])
    assert runtime.regions["r1"].orders_per_s is None

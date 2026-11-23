from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from restaurantos.common.errors import RegionUnreachableError
from restaurantos.coordinator.app import create_app
from restaurantos.coordinator.models import ClusterStatus, NodeStatus, RegionStatus

AT = datetime(2026, 11, 22, tzinfo=UTC)


@pytest.fixture
def runtime():
    class Runtime:
        start = AsyncMock()
        close = AsyncMock()
        events = AsyncMock(return_value=[])
        exceptions = AsyncMock(return_value=[])

        def snapshot(self):
            return ClusterStatus(
                as_of=AT,
                nodes=[
                    NodeStatus(node_id="r1-primary", region_id="r1", role="primary", status="down")
                ],
                regions=[
                    RegionStatus(
                        region_id="r1",
                        epoch=1,
                        menu_version=2,
                        primary_node="r1-primary",
                        standby_node="r1-standby",
                        outbox_backlog=12,
                        figures_as_of=AT,
                    )
                ],
            )

    return Runtime()


def test_cluster_shape_keeps_old_figures_and_unknown_measurements(runtime):
    with TestClient(create_app(runtime)) as client:
        response = client.get("/admin/cluster")
    assert response.status_code == 200
    data = response.json()
    assert set(data) == {"as_of", "nodes", "regions"}
    assert data["nodes"][0]["status"] == "down"
    region = data["regions"][0]
    assert region["outbox_backlog"] == 12
    assert region["figures_as_of"] is not None
    assert region["standby_lag_s"] is None
    assert region["orders_per_s"] is None
    runtime.start.assert_awaited_once()
    runtime.close.assert_awaited_once()


@pytest.mark.parametrize("kind", ["events", "exceptions"])
def test_paging_defaults_and_cursor_are_forwarded(runtime, kind):
    with TestClient(create_app(runtime)) as client:
        assert client.get(f"/admin/{kind}").json() == []
        assert client.get(f"/admin/{kind}?since=4&limit=2").json() == []
    method = getattr(runtime, kind)
    assert method.await_args_list[0].args == (None, 100)
    assert method.await_args_list[1].args == (4, 2)


@pytest.mark.parametrize("query", ["since=-1", "limit=0", "limit=1001", "since=bad"])
def test_invalid_paging_is_rejected(runtime, query):
    with TestClient(create_app(runtime)) as client:
        assert client.get(f"/admin/events?{query}").status_code == 422
    runtime.events.assert_not_awaited()


def test_unreachable_hq_has_a_bounded_error_response(runtime):
    runtime.events.side_effect = RegionUnreachableError(region_id="hq")
    with TestClient(create_app(runtime)) as client:
        response = client.get("/admin/events")
    assert response.status_code == 503
    assert response.json() == {"error": "region_unreachable", "region_id": "hq"}

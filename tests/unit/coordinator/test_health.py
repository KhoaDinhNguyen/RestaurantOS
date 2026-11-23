from datetime import UTC, datetime

import pytest

from restaurantos.common.localnode import NodeView
from restaurantos.coordinator.health import NodeHealth
from restaurantos.coordinator.models import NodeStatus

AT = datetime(2026, 11, 22, tzinfo=UTC)


def healthy():
    state = NodeHealth(NodeStatus(node_id="r1-primary", region_id="r1", role="primary"))
    assert state.observe(NodeView("r1-primary", True, False, 1), AT, 3, None) == ["node_up"]
    return state


@pytest.mark.parametrize(
    "gateway, expected", [(True, "suspect"), (False, "down"), (None, "suspect")]
)
def test_three_misses_require_second_path_confirmation(gateway, expected):
    state = healthy()
    for _ in range(2):
        assert state.observe(NodeView("r1-primary"), AT, 3, None) == []
        assert state.response.status == "up"
    events = state.observe(NodeView("r1-primary"), AT, 3, gateway)
    assert state.response.status == expected
    assert events[0] == "node_suspect"
    assert state.response.last_heartbeat_at == AT


def test_down_node_does_not_flap_and_success_resets_misses():
    state = healthy()
    for _ in range(3):
        state.observe(NodeView("r1-primary"), AT, 3, False)
    assert state.response.status == "down"
    assert state.observe(NodeView("r1-primary"), AT, 3, False) == []
    assert state.observe(NodeView("r1-primary", True, True, 2), AT, 3, None) == ["node_up"]
    assert state.misses == 0
    assert state.response.role == "standby"
    assert state.observe(NodeView("r1-primary"), AT, 3, None) == []
    assert state.response.status == "up"

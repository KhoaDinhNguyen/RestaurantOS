"""Decision guards, separate from the real promotion/restart cluster test."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.localnode import NodeView
from restaurantos.coordinator import failover
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.service import Coordinator


@pytest.fixture
async def setup(monkeypatch):
    runtime = Coordinator(
        Cluster(),
        Settings(_env_file=None, client="coordinator", network="host"),
        CoordinatorSettings(_env_file=None),
    )
    row = {
        "region_id": "r1",
        "epoch": 1,
        "primary_node": "r1-primary",
        "standby_node": "r1-standby",
    }
    cursor = AsyncMock()
    cursor.__aenter__.return_value = cursor
    cursor.fetchone.return_value = row
    conn = SimpleNamespace(execute=AsyncMock(), cursor=lambda **kw: cursor)

    async def run(pool, work, **kw):
        return await work(conn)

    monkeypatch.setattr(failover, "run_on", run)
    runtime.health["r1-primary"].response.status = "down"
    runtime._gateway_probe = AsyncMock(return_value=False)
    runtime.failover.node_call = AsyncMock(return_value=(2,))
    runtime.failover.event = AsyncMock()
    yield runtime, row, conn, cursor
    await runtime.close()


@pytest.mark.parametrize("status", ["up", "suspect"])
async def test_primary_must_cross_down_threshold(setup, status):
    runtime, _, _, _ = setup
    runtime.health["r1-primary"].response.status = status
    runtime.failover.probe = AsyncMock(return_value=NodeView("r1-standby", True, True, 1))
    await runtime.failover.step("r1")
    runtime.failover.node_call.assert_not_awaited()
    runtime._gateway_probe.assert_not_awaited()


@pytest.mark.parametrize("gateway", [True, None])
async def test_second_path_must_explicitly_confirm_failure(setup, gateway):
    runtime, _, _, _ = setup
    runtime._gateway_probe.return_value = gateway
    runtime.failover.probe = AsyncMock(
        side_effect=[NodeView("r1-standby", True, True, 1), NodeView("r1-primary")]
    )
    await runtime.failover.step("r1")
    runtime.failover.node_call.assert_not_awaited()


async def test_recovered_primary_overrides_cached_down(setup):
    runtime, _, _, _ = setup
    runtime.failover.probe = AsyncMock(
        side_effect=[NodeView("r1-standby", True, True, 1), NodeView("r1-primary", True, False, 1)]
    )
    await runtime.failover.step("r1")
    runtime.failover.node_call.assert_not_awaited()


async def test_isolation_records_partition_without_promotion(setup):
    runtime, _, _, _ = setup
    runtime.health["r1-standby"].response.status = "down"
    runtime.failover.probe = AsyncMock(return_value=NodeView("r1-standby"))
    await runtime.failover.step("r1")
    runtime.failover.node_call.assert_not_awaited()
    assert runtime.failover.event.await_args.args[2] == "partition"


async def test_partition_retry_reuses_marker_but_a_later_outage_gets_a_new_one(setup):
    runtime, _, _, _ = setup
    runtime.health["r1-standby"].response.status = "down"
    runtime.failover.probe = AsyncMock(return_value=NodeView("r1-standby"))
    await runtime.failover.step("r1")
    first = runtime.failover.event.await_args.kwargs["marker"]
    await runtime.failover.step("r1")
    assert runtime.failover.event.await_args.kwargs["marker"] == first
    runtime.health["r1-primary"].response.status = "up"
    await runtime.failover.step("r1")
    runtime.health["r1-primary"].response.status = "down"
    await runtime.failover.step("r1")
    assert runtime.failover.event.await_args.kwargs["marker"] != first


@pytest.mark.parametrize("epoch", [0, 3])
async def test_stale_or_unexpected_writable_candidate_is_not_adopted(setup, epoch):
    runtime, _, _, _ = setup
    runtime.failover.probe = AsyncMock(return_value=NodeView("r1-standby", True, False, epoch))
    await runtime.failover.step("r1")
    runtime.failover.node_call.assert_not_awaited()


@pytest.mark.parametrize("epoch", [1, 2])
async def test_resume_role_or_epoch_step_without_new_failure_decision(setup, epoch):
    runtime, _, conn, _ = setup
    runtime.health["r1-primary"].response.status = "up"
    runtime.failover.probe = AsyncMock(return_value=NodeView("r1-standby", True, False, epoch))
    await runtime.failover.step("r1")
    assert runtime.failover.node_call.await_args.args[1] == "promotion_epoch"
    runtime._gateway_probe.assert_not_awaited()
    assert any("UPDATE region_state" in call.args[0] for call in conn.execute.await_args_list)
    assert runtime.failover.event.await_args.args[2] == "failover"


async def test_asynchronous_promotion_waits_for_writable_role(setup):
    runtime, _, conn, _ = setup
    runtime.failover.probe = AsyncMock(
        side_effect=[
            NodeView("r1-standby", True, True, 1),
            NodeView("r1-primary"),
            NodeView("r1-standby", True, True, 1),
        ]
    )
    await runtime.failover.step("r1")
    assert runtime.failover.node_call.await_count == 1
    assert runtime.failover.node_call.await_args.args[1] == "promote"
    assert not any("UPDATE region_state" in call.args[0] for call in conn.execute.await_args_list)


async def test_locked_or_missing_directory_is_skipped(setup):
    runtime, _, _, cursor = setup
    cursor.fetchone.return_value = None
    runtime.failover.probe = AsyncMock()
    await runtime.failover.step("r1")
    runtime.failover.probe.assert_not_awaited()


async def test_failed_epoch_update_cannot_publish_directory(setup):
    runtime, _, conn, _ = setup
    runtime.failover.probe = AsyncMock(return_value=NodeView("r1-standby", True, False, 1))
    runtime.failover.node_call.return_value = None
    await runtime.failover.step("r1")
    runtime.failover.event.assert_not_awaited()
    assert not any("UPDATE region_state" in call.args[0] for call in conn.execute.await_args_list)

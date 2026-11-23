"""Fallback guards and durable-intent decisions; real PostgreSQL is tested separately."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.localnode import NodeView
from restaurantos.coordinator import replication
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.service import Coordinator


@pytest.fixture
async def setup(monkeypatch):
    runtime = Coordinator(
        Cluster(),
        Settings(_env_file=None, client="coordinator", network="host"),
        CoordinatorSettings(_env_file=None),
    )
    runtime.health["r1-primary"].response.status = "up"
    data = SimpleNamespace(
        row={
            "region_id": "r1",
            "epoch": 1,
            "primary_node": "r1-primary",
            "standby_node": "r1-standby",
        },
        pending=None,
        state={
            "epoch": 1,
            "in_recovery": False,
            "standby_names": "FIRST 1 (*)",
            "commit_mode": "on",
            "setting_source": "configuration file",
            "sync_connected": False,
        },
    )
    cursor = AsyncMock()
    cursor.__aenter__.return_value = cursor
    query = {"text": ""}

    async def execute(sql, params):
        query["text"] = sql

    cursor.execute.side_effect = execute
    cursor.fetchone.side_effect = lambda: (
        data.row if "FROM region_state" in query["text"] else data.pending
    )
    conn = SimpleNamespace(execute=AsyncMock(), cursor=lambda **kwargs: cursor)

    async def run(pool, work, **kwargs):
        return await work(conn)

    monkeypatch.setattr(replication, "run_on", run)
    monkeypatch.setattr(
        replication, "probe_node", AsyncMock(return_value=NodeView("r1-standby", True, True, 1))
    )
    runtime.replication.status = AsyncMock(side_effect=lambda row: data.state)
    runtime.replication.disable = AsyncMock()
    runtime.failover.event = AsyncMock()
    yield runtime, data
    await runtime.close()


async def test_requires_threshold_and_only_commits_intent_first(setup):
    runtime, _ = setup
    for _ in range(runtime.config.heartbeat_misses - 1):
        await runtime.replication.step("r1")
    runtime.failover.event.assert_not_awaited()
    await runtime.replication.step("r1")
    assert runtime.failover.event.await_args.args[2] == "sync_downgrade_requested"
    runtime.replication.disable.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("standby_names", ""),
        ("commit_mode", "local"),
        ("commit_mode", "off"),
        ("sync_connected", True),
        ("in_recovery", True),
        ("epoch", 2),
        ("setting_source", "command line"),
    ],
)
async def test_no_downgrade_when_unnecessary_or_unsafe(setup, field, value):
    runtime, data = setup
    data.state[field] = value
    for _ in range(runtime.config.heartbeat_misses + 1):
        await runtime.replication.step("r1")
    runtime.failover.event.assert_not_awaited()
    runtime.replication.disable.assert_not_awaited()


async def test_healthy_sample_resets_disconnection_threshold(setup):
    runtime, data = setup
    await runtime.replication.step("r1")
    await runtime.replication.step("r1")
    data.state["sync_connected"] = True
    await runtime.replication.step("r1")
    data.state["sync_connected"] = False
    await runtime.replication.step("r1")
    runtime.failover.event.assert_not_awaited()


async def test_unhealthy_primary_is_never_reconfigured(setup):
    runtime, _ = setup
    runtime.health["r1-primary"].response.status = "down"
    await runtime.replication.step("r1")
    runtime.replication.status.assert_not_awaited()
    runtime.replication.disable.assert_not_awaited()


async def test_half_promoted_standby_takes_precedence(setup, monkeypatch):
    runtime, _ = setup
    monkeypatch.setattr(
        replication, "probe_node", AsyncMock(return_value=NodeView("r1-standby", True, False, 2))
    )
    await runtime.replication.step("r1")
    runtime.replication.status.assert_not_awaited()


async def test_restart_after_configuration_change_finishes_pending_audit(setup):
    runtime, data = setup
    data.pending = {"detail": {"coordinator_event_id": "intent-one", "epoch": 1}}
    data.state["standby_names"] = ""
    await runtime.replication.step("r1")
    runtime.replication.disable.assert_not_awaited()
    assert runtime.failover.event.await_args.args[2] == "sync_downgrade"
    assert runtime.failover.event.await_args.kwargs["marker"] == "sync-complete:intent-one"


async def test_reload_must_be_observed_before_completion(setup):
    runtime, data = setup
    data.pending = {"detail": {"coordinator_event_id": "intent-one", "epoch": 1}}
    await runtime.replication.step("r1")
    runtime.replication.disable.assert_awaited_once_with("r1-primary")
    runtime.failover.event.assert_not_awaited()


async def test_configuration_error_does_not_stop_failover_loop(setup):
    from psycopg.errors import InsufficientPrivilege

    runtime, data = setup
    data.pending = {"detail": {"coordinator_event_id": "intent-one", "epoch": 1}}
    runtime.replication.disable.side_effect = InsufficientPrivilege("test missing privilege")
    await runtime.replication.step("r1")
    runtime.failover.event.assert_not_awaited()


@pytest.mark.parametrize("gateway, expected", [(False, True), (True, False), (None, False)])
async def test_confirmed_loss_overrides_stale_streaming_view(setup, monkeypatch, gateway, expected):
    runtime, data = setup
    data.state["sync_connected"] = True
    runtime.health["r1-standby"].response.status = "down"
    runtime._gateway_probe = AsyncMock(return_value=gateway)
    monkeypatch.setattr(replication, "probe_node", AsyncMock(return_value=NodeView("r1-standby")))
    await runtime.replication.step("r1")
    assert runtime.failover.event.await_count == int(expected)

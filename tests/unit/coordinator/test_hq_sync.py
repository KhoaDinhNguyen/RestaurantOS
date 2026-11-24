"""HQ's sync fallback can journal locally and resume after restart."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.cluster import HQ_PRIMARY, Cluster
from restaurantos.common.config import Settings
from restaurantos.coordinator import replication
from restaurantos.coordinator.config import CoordinatorSettings


@pytest.fixture
def fixture(monkeypatch):
    state = {
        "in_recovery": False,
        "system_id": "123",
        "timeline": "00000001",
        "standby_names": "FIRST 1 (hq_standby)",
        "commit_mode": "on",
        "sync_connected": False,
    }
    pending = {"row": None}
    query = {"text": ""}
    cursor = AsyncMock()
    cursor.__aenter__.return_value = cursor

    async def execute(sql, params):
        query["text"] = sql

    cursor.execute.side_effect = execute
    cursor.fetchone.side_effect = lambda: (
        pending["row"] if "request.region_id IS NULL" in query["text"] else dict(state)
    )
    locked = AsyncMock()
    locked.fetchone.return_value = (True,)

    async def execute_control(sql, params=None):
        if "set_config('synchronous_commit'" in sql:
            state["commit_mode"] = "local"
        return locked

    conn = SimpleNamespace(
        cursor=lambda **k: cursor, execute=AsyncMock(side_effect=execute_control)
    )

    async def run(pool, work, **kwargs):
        before = state["commit_mode"]
        try:
            return await work(conn)
        finally:
            state["commit_mode"] = before

    monkeypatch.setattr(replication, "run_on", run)
    event = AsyncMock()

    async def record(conn, row, kind, node, epoch, detail, *, marker):
        if kind == "sync_downgrade_requested":
            pending["row"] = {"detail": {**detail, "coordinator_event_id": marker}}
        elif kind == "sync_downgrade":
            pending["row"] = None

    event.side_effect = record
    runtime = SimpleNamespace(
        cluster=Cluster(),
        config=CoordinatorSettings(_env_file=None, heartbeat_misses=2),
        settings=Settings(_env_file=None),
        hq_pool=SimpleNamespace(select=AsyncMock(return_value=HQ_PRIMARY)),
        pools={HQ_PRIMARY: object()},
        failover=SimpleNamespace(event=event),
    )
    fallback = replication.ReplicationFallback(runtime)

    async def disable(node):
        state["standby_names"] = ""

    fallback.disable = AsyncMock(side_effect=disable)
    return runtime, fallback, state, conn


async def test_hq_loss_intent_then_restart_completes_one_downgrade(fixture):
    runtime, fallback, state, conn = fixture
    await fallback.step_hq()
    runtime.failover.event.assert_not_awaited()
    await fallback.step_hq()
    assert runtime.failover.event.await_args.args[2] == "sync_downgrade_requested"
    fallback.disable.assert_not_awaited()
    resumed = replication.ReplicationFallback(runtime)
    resumed.disable = fallback.disable
    await resumed.step_hq()
    assert [c.args[2] for c in runtime.failover.event.await_args_list] == [
        "sync_downgrade_requested",
        "sync_downgrade",
    ]
    assert any("synchronous_commit" in c.args[0] for c in conn.execute.await_args_list)
    await resumed.step_hq()
    assert runtime.failover.event.await_count == 2


async def test_healthy_hq_replication_is_not_downgraded(fixture):
    runtime, fallback, state, _ = fixture
    state["sync_connected"] = True
    for _ in range(4):
        await fallback.step_hq()
    runtime.failover.event.assert_not_awaited()
    fallback.disable.assert_not_awaited()


async def test_database_error_keeps_pending_intent_and_later_cycle_recovers(fixture, caplog):
    from psycopg.errors import InsufficientPrivilege

    runtime, fallback, _, _ = fixture
    await fallback.step_hq()
    await fallback.step_hq()
    original = fallback.disable.side_effect
    fallback.disable.side_effect = InsufficientPrivilege("ALTER SYSTEM denied")
    await fallback.step_hq()
    assert "intent retained" in caplog.text
    assert runtime.failover.event.await_count == 1
    fallback.disable.side_effect = original
    await fallback.step_hq()
    assert [call.args[2] for call in runtime.failover.event.await_args_list] == [
        "sync_downgrade_requested",
        "sync_downgrade",
    ]

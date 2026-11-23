"""Manual authorization must not bypass standby, epoch, or commit safeguards."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.common.errors import RegionUnreachableError
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
    runtime.health["r1-primary"].response.status = "up"
    runtime.health["r1-standby"].response.status = "up"
    row = {
        "region_id": "r1",
        "epoch": 1,
        "primary_node": "r1-primary",
        "standby_node": "r1-standby",
    }
    state = SimpleNamespace(known=True, row=row, commits=0)
    cursor = AsyncMock()
    cursor.__aenter__.return_value = cursor
    query = {"text": ""}

    async def execute(sql, params):
        query["text"] = sql

    cursor.execute.side_effect = execute

    def fetch():
        if "FROM cluster_events" in query["text"]:
            return {"event_id": 18}
        if "FOR UPDATE" in query["text"]:
            return state.row
        return {"region_id": "r1"} if state.known else None

    cursor.fetchone.side_effect = fetch
    conn = SimpleNamespace(execute=AsyncMock(), cursor=lambda **kwargs: cursor)

    async def run(pool, work, **kwargs):
        result = await work(conn)
        state.commits += 1
        return result

    monkeypatch.setattr(failover, "run_on", run)
    runtime.failover.probe = AsyncMock(
        side_effect=[NodeView("r1-standby", True, True, 1), NodeView("r1-standby", True, False, 1)]
    )
    runtime.failover.node_call = AsyncMock(return_value=(2,))
    runtime.failover.event = AsyncMock()
    runtime._gateway_probe = AsyncMock()
    yield runtime, state, conn
    await runtime.close()


async def test_healthy_primary_can_be_promoted_without_failure_checks(setup):
    runtime, state, _ = setup
    result = await runtime.failover.manual("r1")
    assert result.model_dump() == {
        "region_id": "r1",
        "epoch": 2,
        "primary_node": "r1-standby",
        "standby_node": "r1-primary",
        "event_id": 18,
    }
    assert state.commits == 1
    assert [call.args[1] for call in runtime.failover.node_call.await_args_list] == [
        "promote",
        "promotion_epoch",
    ]
    runtime._gateway_probe.assert_not_awaited()


@pytest.mark.parametrize("region", ["r99", "hq", "hq-primary"])
async def test_unknown_region_and_hq_have_no_manual_promotion(setup, region):
    runtime, _, _ = setup
    with pytest.raises(failover.ManualFailoverError, match="not_found"):
        await runtime.failover.manual(region)
    runtime.failover.probe.assert_not_awaited()


async def test_missing_hq_region_is_404(setup):
    runtime, state, _ = setup
    state.known = False
    with pytest.raises(failover.ManualFailoverError, match="not_found"):
        await runtime.failover.manual("r1")
    runtime.failover.node_call.assert_not_awaited()


async def test_locked_region_refuses_without_waiting_or_promoting(setup):
    runtime, state, _ = setup
    state.row = None
    with pytest.raises(failover.ManualFailoverError, match="failover_in_progress"):
        await runtime.failover.manual("r1")
    runtime.failover.probe.assert_not_awaited()


@pytest.mark.parametrize("status", ["suspect", "down"])
async def test_standby_must_be_up_even_if_one_fresh_probe_answers(setup, status):
    runtime, _, _ = setup
    runtime.health["r1-standby"].response.status = status
    with pytest.raises(failover.ManualFailoverError, match="standby_unavailable"):
        await runtime.failover.manual("r1")
    runtime.failover.node_call.assert_not_awaited()


@pytest.mark.parametrize(
    "view,error",
    [
        (NodeView("r1-standby"), "standby_unavailable"),
        (NodeView("r1-standby", True, True, 0), "standby_unavailable"),
        (NodeView("r1-standby", True, False, 0), "standby_unavailable"),
        (NodeView("r1-standby", True, False, 1), "failover_in_progress"),
        (NodeView("r1-standby", True, False, 2), "failover_in_progress"),
    ],
)
async def test_fresh_role_and_epoch_are_required(setup, view, error):
    runtime, _, _ = setup
    runtime.failover.probe.side_effect = None
    runtime.failover.probe.return_value = view
    with pytest.raises(failover.ManualFailoverError, match=error):
        await runtime.failover.manual("r1")
    runtime.failover.node_call.assert_not_awaited()


async def test_success_is_not_returned_when_hq_commit_is_unconfirmed(setup, monkeypatch):
    runtime, _, _ = setup

    original = failover.run_on

    async def lost_ack(pool, work, **kwargs):
        await original(pool, work, **kwargs)
        raise RegionUnreachableError(region_id="hq")

    monkeypatch.setattr(failover, "run_on", lost_ack)
    with pytest.raises(RegionUnreachableError):
        await runtime.failover.manual("r1")


@pytest.mark.parametrize("standby", [None, "r1-primary"])
async def test_absent_or_self_target_is_never_promoted(setup, standby):
    runtime, state, _ = setup
    state.row["standby_node"] = standby
    with pytest.raises(failover.ManualFailoverError, match="standby_unavailable"):
        await runtime.failover.manual("r1")
    runtime.failover.probe.assert_not_awaited()


async def test_manual_waits_for_asynchronous_postgres_promotion(setup):
    runtime, _, _ = setup
    runtime.failover.probe.side_effect = [
        NodeView("r1-standby", True, True, 1),
        NodeView("r1-standby", True, True, 1),
        NodeView("r1-standby", True, False, 1),
    ]
    assert (await runtime.failover.manual("r1")).epoch == 2
    assert runtime.failover.probe.await_count == 3


async def test_unfinished_postgres_promotion_never_answers_success(setup, monkeypatch):
    runtime, _, _ = setup
    runtime.failover.probe.side_effect = None
    runtime.failover.probe.return_value = NodeView("r1-standby", True, True, 1)
    times = iter([0, runtime.config.failover_timeout_s + 1])
    monkeypatch.setattr(failover, "time", SimpleNamespace(monotonic=lambda: next(times)))
    with pytest.raises(RegionUnreachableError):
        await runtime.failover.manual("r1")
    assert [call.args[1] for call in runtime.failover.node_call.await_args_list] == ["promote"]
    runtime.failover.event.assert_not_awaited()

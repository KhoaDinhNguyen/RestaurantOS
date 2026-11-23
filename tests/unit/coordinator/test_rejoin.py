"""Operator recovery must never stop or overwrite HQ's current primary."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from psycopg.conninfo import conninfo_to_dict

from restaurantos.common.cluster import Cluster, proxy_for
from restaurantos.common.config import Settings
from restaurantos.common.localnode import NodeView
from restaurantos.coordinator import rejoin
from restaurantos.coordinator.config import CoordinatorSettings


@pytest.fixture
def plan():
    return rejoin.RejoinPlan("r1", 2, "r1-standby", "r1-primary")


@pytest.fixture
def settings():
    return Settings(_env_file=None, client="coordinator", network="compose")


@pytest.fixture
def compose_config(plan):
    return {
        "services": {
            node: {"volumes": [{"source": f"{node}-data", "target": "/var/lib/postgresql"}]}
            for node in (plan.source, plan.target)
        }
    }


@pytest.mark.parametrize(
    "source,target,epoch",
    [
        ("r1-primary", "r1-primary", 2),
        ("r2-primary", "r1-primary", 2),
        ("hq-primary", "hq-standby", 2),
        ("r1-primary", "r1-standby", 0),
    ],
)
def test_invalid_hq_pair_is_rejected(source, target, epoch):
    with pytest.raises(rejoin.RecoveryError):
        rejoin.RejoinPlan("r1", epoch, source, target).validate(Cluster())


def test_no_standbys_cannot_rejoin(plan):
    with pytest.raises(rejoin.RecoveryError):
        plan.validate(Cluster(standbys=False))


def test_shared_volume_is_refused(compose_config, plan):
    compose_config["services"][plan.target] = compose_config["services"][plan.source]
    with pytest.raises(rejoin.RecoveryError, match="share a data volume"):
        rejoin.validate_storage(compose_config, plan)


@pytest.fixture
def db(monkeypatch, plan):
    conn = SimpleNamespace(execute=AsyncMock())
    cursor = AsyncMock()
    cursor.fetchone.return_value = (plan.region_id, plan.epoch, plan.source, plan.target)
    conn.execute.return_value = cursor
    pool = SimpleNamespace(open=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(rejoin, "HQPool", lambda *args: pool)

    async def run(pool, work, **kwargs):
        return await work(conn)

    monkeypatch.setattr(rejoin, "run_on", run)
    return conn, cursor


@pytest.mark.parametrize(
    "view",
    [
        NodeView("r1-standby"),
        NodeView("r1-standby", True, True, 2),
        NodeView("r1-standby", True, False, 1),
    ],
)
async def test_unfinished_or_unreachable_source_is_refused(db, settings, monkeypatch, view):
    monkeypatch.setattr(rejoin, "probe_node", AsyncMock(return_value=view))
    with pytest.raises(rejoin.RecoveryError, match="finish the promotion"):
        await rejoin.read_plan(Cluster(), settings, "r1")


@pytest.mark.parametrize("row", [None, ("r1", 3, "r1-primary", "r1-standby")])
async def test_stop_is_serialized_and_cannot_stop_a_new_primary(
    db, settings, plan, monkeypatch, row
):
    _, cursor = db
    cursor.fetchone.return_value = row
    command = AsyncMock()
    monkeypatch.setattr(rejoin, "command", command)
    with pytest.raises(rejoin.RecoveryError, match="no node stopped"):
        await rejoin.stop_target(Cluster(), settings, plan, ["docker", "compose"])
    command.assert_not_awaited()


@pytest.fixture
def controller(monkeypatch, plan, compose_config):
    monkeypatch.setattr(rejoin, "read_plan", AsyncMock(return_value=plan))
    monkeypatch.setattr(rejoin, "stop_target", AsyncMock())
    monkeypatch.setattr(
        rejoin, "probe_node", AsyncMock(return_value=NodeView(plan.target, True, True, 2))
    )
    monkeypatch.setattr(rejoin, "streaming", AsyncMock(return_value=True))
    command = AsyncMock(side_effect=[json.dumps(compose_config), "rejoin: rewind completed\n", ""])
    monkeypatch.setattr(rejoin, "command", command)
    return command


async def test_success_uses_reverse_proxy_and_only_starts_the_old_node(
    controller,
    plan,
    settings,
):
    result = await rejoin.recover(
        Cluster(), settings, CoordinatorSettings(_env_file=None), "r1", Path("infra/compose.yaml")
    )
    assert "r1-primary rejoined as standby of r1-standby" in result
    payload = controller.await_args_list[1]
    env = payload.kwargs["env"]
    connection = conninfo_to_dict(env["ROS_REJOIN_SOURCE_DSN"])
    assert connection["host"] == "toxiproxy"
    assert int(connection["port"]) == proxy_for(Cluster(), plan.target, plan.source).port
    assert env["ROS_SLOT_NAME"] == "r1_primary"
    assert controller.await_args_list[-1].args[0][-2:] == ["start", plan.target]


async def test_failed_payload_leaves_target_stopped(controller, settings, compose_config):
    controller.side_effect = [
        json.dumps(compose_config),
        rejoin.RecoveryError("source lost"),
    ]
    with pytest.raises(rejoin.RecoveryError, match="source lost"):
        await rejoin.recover(
            Cluster(),
            settings,
            CoordinatorSettings(_env_file=None),
            "r1",
            Path("infra/compose.yaml"),
        )
    assert not any("start" in call.args[0] for call in controller.await_args_list)


async def test_hq_change_after_stop_prevents_touching_data(controller, settings, plan, monkeypatch):
    monkeypatch.setattr(
        rejoin,
        "read_plan",
        AsyncMock(side_effect=[plan, rejoin.RejoinPlan("r1", 3, plan.target, plan.source)]),
    )
    with pytest.raises(rejoin.RecoveryError, match="HQ changed"):
        await rejoin.recover(
            Cluster(),
            settings,
            CoordinatorSettings(_env_file=None),
            "r1",
            Path("infra/compose.yaml"),
        )
    assert controller.await_count == 1  # read-only Compose config, no payload

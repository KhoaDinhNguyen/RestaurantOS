"""Only the physically fenced old HQ can be replaced by recovery."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster, proxy_for
from restaurantos.common.config import Settings
from restaurantos.coordinator import hq_rejoin as rejoin
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.rejoin import RecoveryError


@pytest.fixture
def fixture(monkeypatch):
    config = {"name": "scratch", "services": {}, "volumes": {}}
    containers = {}
    for node in (HQ_PRIMARY, HQ_STANDBY):
        config["services"][node] = {
            "volumes": [{"source": node, "type": "volume", "target": "/var/lib/postgresql"}]
        }
        config["volumes"][node] = {"name": node}
        containers[node] = {
            "Id": node,
            "Image": "same-image",
            "State": {"Running": node == HQ_STANDBY},
            "HostConfig": {"RestartPolicy": {"Name": "no"}},
            "Config": {
                "Env": ["PGDATA=/var/lib/postgresql/18/docker"],
                "Labels": {
                    "com.docker.compose.service": node,
                    "com.docker.compose.project": "scratch",
                },
            },
            "Mounts": [{"Name": node, "Type": "volume", "Destination": "/var/lib/postgresql"}],
        }

    async def command(args, **kwargs):
        if "config" in args:
            return json.dumps(config)
        if "ps" in args:
            return args[-1]
        return "rejoin: rebuild completed\n"

    docker = AsyncMock(side_effect=command)
    monkeypatch.setattr(rejoin, "command", docker)
    monkeypatch.setattr(rejoin, "inspect_container", AsyncMock(side_effect=lambda n: containers[n]))
    pool = AsyncMock()
    pool.__aenter__.return_value = pool
    monkeypatch.setattr(rejoin, "make_pool", lambda *a: pool)
    cursor = AsyncMock()
    cursor.fetchone.return_value = (True,)
    policy = AsyncMock()
    policy.fetchone.return_value = ("",)
    cursor.policy = policy

    async def execute(sql, params=None):
        return policy if "synchronous_standby_names" in sql else cursor

    conn = SimpleNamespace(execute=AsyncMock(side_effect=execute))

    async def run(pool, work, **kwargs):
        return await work(conn)

    monkeypatch.setattr(rejoin, "run_on", run)
    monkeypatch.setattr(
        rejoin,
        "role",
        AsyncMock(
            side_effect=[
                (False, "123", True, "00000002"),
                (False, "123", True, "00000002"),
                (True, "123", True, None),
            ]
        ),
    )
    return containers, docker, cursor


async def recover():
    return await rejoin.recover(
        Cluster(),
        Settings(_env_file=None, client="coordinator", network="compose"),
        CoordinatorSettings(_env_file=None),
        Path("scratch.yaml"),
    )


async def test_rejoin_uses_reverse_proxy_and_never_replaces_the_source(fixture):
    _, docker, _ = fixture
    assert "rejoined as standby" in await recover()
    payload = next(call for call in docker.await_args_list if "run" in call.args[0])
    assert payload.args[0][-2:] == [HQ_PRIMARY, "-s"]
    env = payload.kwargs["env"]
    assert env["ROS_REJOIN_KIND"] == "hq"
    assert env["ROS_REJOIN_HQ_ID"] == "123"
    assert env["ROS_REJOIN_HQ_TIMELINE"] == "00000002"
    assert env["ROS_PRIMARY_PORT"] == str(proxy_for(Cluster(), HQ_PRIMARY, HQ_STANDBY).port)
    assert env["ROS_SLOT_NAME"] == "hq_primary"
    assert docker.await_args_list[-1].args[0][-2:] == ["start", HQ_PRIMARY]


async def test_live_old_primary_is_never_replaced(fixture):
    containers, docker, _ = fixture
    containers[HQ_PRIMARY]["State"]["Running"] = True
    with pytest.raises(RecoveryError, match="remain fenced"):
        await recover()
    assert not any("run" in c.args[0] for c in docker.await_args_list)


async def test_missing_promotion_lock_refuses_without_rebuilding(fixture):
    _, docker, cursor = fixture
    cursor.fetchone.return_value = (False,)
    with pytest.raises(RecoveryError, match="already|another"):
        await recover()
    assert not any("run" in c.args[0] for c in docker.await_args_list)


async def test_interrupted_helper_never_starts_target(fixture):
    _, docker, _ = fixture
    original = docker.side_effect

    async def fail(args, **kwargs):
        if "run" in args:
            raise RecoveryError("source cut during clone")
        return await original(args, **kwargs)

    docker.side_effect = fail
    with pytest.raises(RecoveryError, match="source cut"):
        await recover()
    assert not any("start" in c.args[0] for c in docker.await_args_list)


async def test_recovered_writable_target_is_stopped_immediately(fixture, monkeypatch):
    _, docker, _ = fixture
    monkeypatch.setattr(rejoin, "role", AsyncMock(return_value=(False, "123", True, "00000002")))
    with pytest.raises(RecoveryError, match="recovered HQ is writable"):
        await recover()
    assert docker.await_args_list[-1].args[0] == ["docker", "stop", "--time", "5", HQ_PRIMARY]


def test_rosctl_rejoin_delegates_to_recovery(fixture, monkeypatch, capsys):
    from restaurantos.sim.cli import app

    recovered = AsyncMock(return_value="hq-primary rejoined as standby\n")
    monkeypatch.setattr(rejoin, "recover", recovered)
    assert app(["hq", "rejoin", "--compose-file", "scratch.yaml", "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["rejoined"] is True
    recovered.assert_awaited_once()


async def test_retained_sync_policy_refuses_before_touching_target(fixture):
    _, docker, cursor = fixture
    cursor.policy.fetchone.return_value = ("FIRST 1 (hq_standby)",)
    with pytest.raises(RecoveryError, match="synchronous policy"):
        await recover()
    assert not any(
        "run" in call.args[0] or "start" in call.args[0] for call in docker.await_args_list
    )


async def test_policy_change_before_success_does_not_report_rejoin_success(fixture):
    _, _, cursor = fixture
    cursor.policy.fetchone.side_effect = [("",), ("",), ("FIRST 1 (hq_primary)",)]
    with pytest.raises(RecoveryError, match="synchronous policy"):
        await recover()

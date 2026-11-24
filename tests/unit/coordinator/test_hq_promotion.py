"""HQ has no epoch fence: prove the physical fence before pg_promote."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster
from restaurantos.common.config import Settings
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.coordinator import hq_promotion as hq
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.sim.cli import app, build_parser


@pytest.fixture
def layout():
    definition = {"name": "scratch", "services": {}, "volumes": {}}
    containers = {}
    for node in (HQ_PRIMARY, HQ_STANDBY):
        volume = f"{node}-data"
        definition["services"][node] = {
            "volumes": [{"type": "volume", "source": volume, "target": "/var/lib/postgresql"}]
        }
        definition["volumes"][volume] = {"name": f"scratch_{volume}"}
        containers[node] = {
            "Id": node + "-id",
            "Image": "sha256:postgres-image",
            "Config": {
                "Env": ["PGDATA=/var/lib/postgresql/18/docker"],
                "Labels": {
                    "com.docker.compose.service": node,
                    "com.docker.compose.project": "scratch",
                },
            },
            "Mounts": [
                {
                    "Destination": "/var/lib/postgresql",
                    "Type": "volume",
                    "Name": f"scratch_{volume}",
                }
            ],
            "State": {"Running": True},
            "HostConfig": {"RestartPolicy": {"Name": "no"}},
        }
    return definition, containers


@pytest.mark.parametrize(
    "bad", ["project", "service", "shared", "image", "stopped", "pgdata", "mount"]
)
def test_wrong_or_unsafe_containers_refused(layout, bad):
    definition, containers = layout
    candidate = containers[HQ_STANDBY]
    if bad == "project":
        candidate["Config"]["Labels"]["com.docker.compose.project"] = "normal-cluster"
    elif bad == "service":
        candidate["Config"]["Labels"]["com.docker.compose.service"] = "r1-primary"
    elif bad == "shared":
        candidate["Mounts"][0]["Name"] = containers[HQ_PRIMARY]["Mounts"][0]["Name"]
        definition["volumes"]["hq-standby-data"] = definition["volumes"]["hq-primary-data"]
    elif bad == "image":
        candidate["Image"] = "another-version"
    elif bad == "stopped":
        candidate["State"]["Running"] = False
    elif bad == "pgdata":
        candidate["Config"]["Env"] = ["PGDATA=/tmp/elsewhere"]
    else:
        candidate["Mounts"][0]["Type"] = "bind"
    with pytest.raises(hq.HQPromotionError):
        hq.validate_containers(definition, containers)


@pytest.fixture
def operation(monkeypatch, layout):
    definition, containers = layout
    state = SimpleNamespace(
        recovery=True,
        identifier="123",
        is_hq=True,
        lock=True,
        fence=True,
        event=9,
        committed=0,
        promoted=0,
    )
    cursor = AsyncMock()
    conn = SimpleNamespace(execute=AsyncMock(), set_autocommit=AsyncMock())
    calls = []

    async def execute(sql, params=None):
        calls.append(sql)
        if "pg_try_advisory_xact_lock" in sql:
            row = (state.lock,)
        elif "pg_control_system" in sql:
            row = (state.recovery, state.identifier, state.is_hq, "00000002")
        elif "pg_promote" in sql:
            assert not containers[HQ_PRIMARY]["State"]["Running"]
            state.promoted += 1
            state.recovery = False
            row = (True,)
        elif "current_setting('synchronous_standby_names')" in sql:
            row = ("",)
        elif "AS event_id" in sql:
            row = (state.event,)
        else:
            row = None
        cursor.fetchone.return_value = row
        return cursor

    conn.execute.side_effect = execute

    async def run(pool, work, **kwargs):
        result = await work(conn)
        state.committed += 1
        return result

    async def docker(args):
        if "config" in args:
            return json.dumps(definition)
        if "ps" in args:
            return args[-1] + "-id"
        if args[1] == "inspect":
            return json.dumps([containers[args[-1].removesuffix("-id")]])
        if args[1] == "run":
            assert "--pull=never" in args and "none" in args
            assert "hq-primary-id:ro" in args
            return "Database system identifier: 123\n"
        if args[1] == "stop" and state.fence:
            containers[HQ_PRIMARY]["State"]["Running"] = False
        return ""

    pool = AsyncMock()
    pool.__aenter__.return_value = pool
    monkeypatch.setattr(hq, "make_pool", lambda *args: pool)
    monkeypatch.setattr(hq, "run_on", run)
    monkeypatch.setattr(hq, "command", AsyncMock(side_effect=docker))
    return state, calls, containers


async def attempt():
    return await hq.promote(
        Cluster(),
        Settings(_env_file=None, client="coordinator", network="compose"),
        CoordinatorSettings(_env_file=None),
        Path("scratch.yaml"),
    )


async def test_fence_then_promote_and_committed_event(operation):
    state, calls, _ = operation
    assert await attempt() == {
        "primary_node": HQ_STANDBY,
        "old_primary_stopped": True,
        "event_id": 9,
        "client_switch_required": False,
    }
    assert state.promoted == 1 and state.committed == 4
    payload = next(c for c in hq.command.await_args_list if c.args[0][1] == "update")
    assert payload.args[0] == ["docker", "update", "--restart=no", "hq-primary-id"]
    assert any("INSERT INTO cluster_events" in sql for sql in calls)


@pytest.mark.parametrize("failure", ["lock", "identifier", "is_hq", "fence", "restart"])
async def test_failed_authorization_never_promotes(operation, failure):
    state, calls, containers = operation
    if failure == "identifier":
        state.identifier = "other-cluster"
    elif failure == "restart":
        containers[HQ_PRIMARY]["HostConfig"]["RestartPolicy"]["Name"] = "always"
    else:
        setattr(state, failure, False)
    with pytest.raises(hq.HQPromotionError):
        await attempt()
    assert not any("pg_promote" in sql for sql in calls)
    if failure in ("lock", "identifier", "is_hq"):
        assert not any(c.args[0][1] in ("stop", "update") for c in hq.command.await_args_list)


async def test_restart_after_promotion_records_without_promoting_again(operation):
    state, _, containers = operation
    state.recovery = False
    containers[HQ_PRIMARY]["State"]["Running"] = False
    assert (await attempt())["event_id"] == 9
    assert state.promoted == 0


async def test_repeat_with_unconfirmed_running_old_node_never_stops_it(operation, monkeypatch):
    state, _, containers = operation
    state.recovery = False
    original = hq.run_on

    async def failed_probe(pool, work, **kwargs):
        if work is hq.role:
            raise RegionUnreachableError("old role probe cut", region_id="hq")
        return await original(pool, work, **kwargs)

    monkeypatch.setattr(hq, "run_on", failed_probe)
    with pytest.raises(hq.HQPromotionError, match="role is unconfirmed"):
        await attempt()
    assert containers[HQ_PRIMARY]["State"]["Running"]
    assert not any(call.args[0][1] in ("stop", "update") for call in hq.command.await_args_list)
    assert state.promoted == 0


async def test_commit_failure_does_not_report_success(operation, monkeypatch):
    original = hq.run_on

    async def lost_commit(pool, work, **kwargs):
        value = await original(pool, work, **kwargs)
        if isinstance(value, dict):
            raise RegionUnreachableError(region_id="hq")
        return value

    monkeypatch.setattr(hq, "run_on", lost_commit)
    with pytest.raises(RegionUnreachableError):
        await attempt()


async def test_no_standby_does_not_touch_docker(monkeypatch):
    docker = AsyncMock()
    monkeypatch.setattr(hq, "command", docker)
    with pytest.raises(hq.HQPromotionError):
        await hq.promote(
            Cluster(standbys=False),
            Settings(_env_file=None),
            CoordinatorSettings(_env_file=None),
            Path("scratch.yaml"),
        )
    docker.assert_not_awaited()


def test_rosctl_requires_explicit_compose_file():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["hq", "promote", "--yes"])


def test_rosctl_requires_authorization_before_docker(monkeypatch, capsys):
    execute = AsyncMock()
    monkeypatch.setattr(hq, "promote", execute)
    assert app(["hq", "promote", "--compose-file", "scratch.yaml"]) == 1
    execute.assert_not_awaited()
    assert "requires --yes" in capsys.readouterr().err


def test_rosctl_outputs_database_scope_and_handoff_warning(monkeypatch, capsys):
    monkeypatch.setattr(hq, "promote", AsyncMock(return_value={"client_switch_required": False}))
    assert app(["hq", "promote", "--compose-file", "scratch.yaml", "--yes"]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["client_switch_required"] is False
    assert not output.err


def test_rosctl_failure_is_actionable_without_traceback(monkeypatch, capsys):
    monkeypatch.setattr(hq, "promote", AsyncMock(side_effect=hq.HQPromotionError("fence failed")))
    assert app(["hq", "promote", "--compose-file", "scratch.yaml", "--yes"]) == 1
    assert "Inspect HQ before retrying" in capsys.readouterr().err

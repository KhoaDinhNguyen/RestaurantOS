"""`rosctl control` and its clients: argument wiring, the start body sent
through --via-control, and the client messages. The routes themselves are
covered in test_control_api.py."""

from __future__ import annotations

import argparse
import json

import httpx
import pytest

from restaurantos.sim import control_cli, playback_cli
from restaurantos.sim.cli import NOT_BUILT, app, build_parser


@pytest.fixture
def fresh_settings():
    from restaurantos.common.config import get_settings
    from restaurantos.sim.config import get_sim_settings

    get_settings.cache_clear()
    get_sim_settings.cache_clear()
    yield
    get_settings.cache_clear()
    get_sim_settings.cache_clear()


def test_control_is_built_and_takes_only_engine_flags() -> None:
    assert "control" not in NOT_BUILT
    parser = build_parser()
    args = parser.parse_args(["control", "--region", "r1", "--sequential", "--port", "9"])
    assert args.region == "r1" and args.sequential and args.port == 9
    # the run flags are not control's: they arrive per run
    assert args.from_day is None and args.seed == 0
    with pytest.raises(SystemExit):
        parser.parse_args(["control", "--from-day", "14"])


def test_client_parsers() -> None:
    parser = build_parser()
    fault = parser.parse_args(
        ["fault", "add", "link:r1<->hq", "latency", "--latency-ms", "4000", "--port", "9"]
    )
    assert (fault.target, fault.kind, fault.latency_ms, fault.port) == (
        "link:r1<->hq",
        "latency",
        4000,
        9,
    )
    assert parser.parse_args(["fault", "heal", "f3"]).fault_id == "f3"
    assert parser.parse_args(["fault", "heal-all"]).fault_command == "heal-all"
    assert parser.parse_args(["node", "kill", "r1-primary"]).node_id == "r1-primary"
    assert parser.parse_args(["job", "j2"]).job_id == "j2"
    with pytest.raises(SystemExit):
        parser.parse_args(["fault", "add", "repl:r1", "flood"])


def test_via_control_refuses_engine_flags(capsys) -> None:
    code = app(["playback", "start", "--via-control", "--from-day", "14", "--sequential"])
    assert code == 1
    assert "--sequential belong to `rosctl control`" in capsys.readouterr().err


def test_via_control_needs_from_day(capsys) -> None:
    assert app(["playback", "start", "--via-control"]) == 1
    assert "--via-control needs --from-day" in capsys.readouterr().err


def test_via_control_posts_the_run_body(monkeypatch) -> None:
    sent: dict = {}

    async def fake_control_command(args, *, method, body):
        sent.update(method=method, body=body)
        return 0

    monkeypatch.setattr(playback_cli, "control_command", fake_control_command)
    code = app(
        [
            "playback",
            "start",
            "--via-control",
            "--from-day",
            "14",
            "--to-day",
            "20",
            "--seed",
            "3",
            "--duplicate-rate",
            "0.05",
            "--no-shuffle",
        ]
    )
    assert code == 0
    assert sent == {
        "method": "POST",
        "body": {
            "action": "start",
            "from_day": 14,
            "seed": 3,
            "to_day": 20,
            "duplicate_rate": 0.05,
            "shuffle": False,
        },
    }


def test_via_control_forwards_skip_first_restock(monkeypatch) -> None:
    """A snapshot restore's replay sets the flag; it is the run's own,
    so it must travel with the start body (not stay on the CLI)."""
    sent: dict = {}

    async def fake_control_command(args, *, method, body):
        sent.update(method=method, body=body)
        return 0

    monkeypatch.setattr(playback_cli, "control_command", fake_control_command)
    code = app(["playback", "start", "--via-control", "--from-day", "21", "--skip-first-restock"])
    assert code == 0
    assert sent["body"]["skip_first_restock"] is True

    sent.clear()
    code = app(["playback", "start", "--via-control", "--from-day", "21"])
    assert code == 0
    assert "skip_first_restock" not in sent["body"]


def _args(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def test_fault_add_posts_the_section_6_3_body(capsys) -> None:
    seen: list = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, json.loads(request.content)))
        return httpx.Response(201, json={"fault_id": "f1"})

    code = control_cli.run(
        _args(["fault", "add", "repl:r1", "latency", "--latency-ms", "50"]),
        transport=httpx.MockTransport(handler),
    )
    assert code == 0
    assert seen == [
        (
            "POST",
            "/sim/faults",
            {"target": "repl:r1", "fault": "latency", "params": {"latency_ms": 50}},
        )
    ]
    assert json.loads(capsys.readouterr().out) == {"fault_id": "f1"}


def test_an_error_answer_prints_its_message(capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409, json={"error": "fault_exists", "message": "repl:r1 already has a cut fault"}
        )

    code = control_cli.run(
        _args(["fault", "add", "repl:r1", "cut"]), transport=httpx.MockTransport(handler)
    )
    assert code == 1
    assert "rosctl fault add: repl:r1 already has a cut fault" in capsys.readouterr().err


def test_no_control_process_says_how_to_start_one(capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    code = control_cli.run(_args(["links"]), transport=httpx.MockTransport(handler))
    assert code == 1
    assert "start it with `rosctl control`" in capsys.readouterr().err


def test_links_note_unexplained_state(capsys) -> None:
    links = [
        {
            "group": "repl:r1",
            "proxies": [],
            "faults": [],
            "unexplained": {"disabled": [], "toxics": [{"proxy": "p", "name": "f1-silence-up"}]},
        }
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=links)

    assert control_cli.run(_args(["links"]), transport=httpx.MockTransport(handler)) == 0
    err = capsys.readouterr().err
    assert "repl:r1 hold disabled proxies or toxics" in err and "heal-all" in err


def test_fault_list_flattens_the_faults(capsys) -> None:
    links = [
        {"group": "a", "faults": [{"fault_id": "f1"}], "unexplained": {}},
        {"group": "b", "faults": [{"fault_id": "f2"}], "unexplained": {}},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=links)

    assert control_cli.run(_args(["fault", "list"]), transport=httpx.MockTransport(handler)) == 0
    assert json.loads(capsys.readouterr().out) == [{"fault_id": "f1"}, {"fault_id": "f2"}]


def test_node_kill_posts_to_the_node_route() -> None:
    paths: list = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append((request.method, request.url.path))
        return httpx.Response(200, json={"node_id": "r1-primary", "running": False})

    code = control_cli.run(
        _args(["node", "kill", "r1-primary"]), transport=httpx.MockTransport(handler)
    )
    assert code == 0 and paths == [("POST", "/sim/nodes/r1-primary/kill")]


def test_control_serves_playback_and_control_routes(tmp_path, monkeypatch, fresh_settings):
    """The takeover: one process, one port, both route sets. The engine
    and the server are faked the way test_cli_playback fakes them."""
    from test_cli_playback import _install_fake_start, _make_data_with_vendors

    monkeypatch.setenv("ROS_SIM_STOCK_WARM_UP_DAYS", "14")
    data = _make_data_with_vendors(
        tmp_path,
        [
            ["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"],
            ["2", "c1", "v1", "w21z7", "3", 15, "12:00:00"],
        ],
        [["1", "p1", 1], ["2", "p1", 1]],
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n",
    )
    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 1\nstandbys = true\n", encoding="utf-8")
    captured: dict = {}
    _install_fake_start(monkeypatch, captured)

    real_extras = playback_cli._add_control_extras

    def capture_extras(app_, path, **kwargs):
        captured["app"] = app_
        captured["extras_port"] = kwargs.get("port")
        captured["scenario_engine"] = kwargs.get("scenario_engine")
        return real_extras(app_, path, **kwargs)

    paths: set[str] = set()

    async def stop_at_once() -> None:
        paths.update(route.path for route in captured["app"].routes)

    monkeypatch.setattr(playback_cli, "_add_control_extras", capture_extras)
    monkeypatch.setattr(playback_cli, "_wait_for_stop", stop_at_once)
    code = app(
        [
            "control",
            "--config",
            str(cluster_file),
            "--kitchen-target",
            "http://127.0.0.1:8900/api/v1",
            "--port",
            "1",
        ]
    )
    assert code == 0
    assert {
        "/sim/playback",
        "/sim/clock",
        "/sim/links",
        "/sim/faults",
        "/sim/faults/heal-all",
        "/sim/faults/{fault_id}",
        "/sim/nodes/{node_id}/kill",
        "/sim/jobs/{job_id}",
        "/sim/scenarios",
        "/sim/scenarios/{name}",
        "/sim/snapshots",
        "/sim/snapshots/{name}/restore",
        "/sim/check",
    } <= paths
    # the scenarios get the engine's own defaults
    engine = captured["scenario_engine"]
    assert engine["default_from_day"] == 14 and engine["last_day"] == 15
    assert engine["api_base"] == "http://127.0.0.1:8000/api/v1"
    # control starts no run of its own
    assert "start_kwargs" not in captured
    # its jobs reach its playback on the port it listens on
    assert captured["extras_port"] == 1


def test_cut_warnings_are_printed(capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(
                200, json={"left_disabled": ["r1-standby__r1-primary"], "fault": "cut"}
            )
        return httpx.Response(
            201, json={"already_disabled": ["r1-standby__r1-primary"], "fault": "cut"}
        )

    transport = httpx.MockTransport(handler)
    assert control_cli.run(_args(["fault", "add", "repl:r1", "cut"]), transport=transport) == 0
    assert "already disabled before this cut" in capsys.readouterr().err
    assert control_cli.run(_args(["fault", "heal", "f1"]), transport=transport) == 0
    err = capsys.readouterr().err
    assert "left disabled (already disabled before their cut)" in err
    assert "r1-standby__r1-primary" in err


def _scenario_handler(jobs: list[dict], seen: list):
    """POST answers 202 with the first job; each GET the next one."""

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.content))
        if request.method == "POST":
            return httpx.Response(202, json=jobs[0])
        return httpx.Response(200, json=jobs.pop(1) if len(jobs) > 1 else jobs[0])

    return handler


def _job(state: str, *, progress=None, result=None, error=None) -> dict:
    return {
        "job_id": "j1",
        "kind": "scenario",
        "name": "T2",
        "state": state,
        "progress": progress,
        "result": result,
        "error": error,
    }


def test_scenario_posts_its_flags_and_follows_the_job(monkeypatch, capsys) -> None:
    monkeypatch.setattr(control_cli, "POLL_S", 0)
    result = {
        "scenario": "T2",
        "passed": True,
        "run_id": "t2-r1-x",
        "region_id": "r2",
        "window": {"from_day": 14, "to_day": 15},
        "conditions": [{"name": "checker passed", "passed": True, "detail": ""}],
    }
    jobs = [
        _job("running"),
        _job("running", progress="r2 cut off from HQ: 50 s left"),
        _job("done", result=result),
    ]
    seen: list = []
    code = control_cli.run(
        _args(["scenario", "T2", "--region", "r2", "--fault-s", "300"]),
        transport=httpx.MockTransport(_scenario_handler(jobs, seen)),
    )
    assert code == 0
    method, path, content = seen[0]
    assert (method, path) == ("POST", "/sim/scenarios/T2")
    assert json.loads(content) == {"region_id": "r2", "fault_s": 300.0}
    assert seen[1][:2] == ("GET", "/sim/jobs/j1")
    captured = capsys.readouterr()
    assert "r2 cut off from HQ: 50 s left" in captured.err
    assert captured.out.startswith("T2 PASSED (run t2-r1-x")


def test_scenario_exit_codes(monkeypatch, capsys) -> None:
    monkeypatch.setattr(control_cli, "POLL_S", 0)
    failed_verdict = [_job("done", result={"passed": False, "conditions": []})]
    transport = httpx.MockTransport(_scenario_handler(failed_verdict, []))
    assert control_cli.run(_args(["scenario", "T5", "--json"]), transport=transport) == 1
    assert json.loads(capsys.readouterr().out)["passed"] is False
    crashed = [_job("failed", error="ScenarioError: the run ended failed")]
    transport = httpx.MockTransport(_scenario_handler(crashed, []))
    assert control_cli.run(_args(["scenario", "T5"]), transport=transport) == 1
    assert "the run ended failed" in capsys.readouterr().err


def test_scenario_refusal_and_list(capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[{"name": "T1"}])
        return httpx.Response(409, json={"error": "scenario_refused", "message": "a run is live"})

    transport = httpx.MockTransport(handler)
    assert control_cli.run(_args(["scenario", "T2"]), transport=transport) == 1
    assert "a run is live" in capsys.readouterr().err
    assert control_cli.run(_args(["scenario", "list"]), transport=transport) == 0
    assert json.loads(capsys.readouterr().out) == [{"name": "T1"}]


def test_scenario_flags_reject_non_finite_numbers() -> None:
    for bad in ("nan", "inf", "-inf"):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["scenario", "T5", "--fault-s", bad])
        with pytest.raises(SystemExit):
            build_parser().parse_args(["scenario", "T5", "--speed", bad])
    args = _args(["scenario", "T5", "--fault-s", "2.5", "--duplicate-rate", "0.1"])
    assert args.fault_s == 2.5 and args.duplicate_rate == 0.1

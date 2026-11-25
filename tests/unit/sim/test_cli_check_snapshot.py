"""`rosctl check` and `rosctl snapshot`: argument wiring and messages."""

from __future__ import annotations

import json

import pytest

from restaurantos.sim import jobs_cli
from restaurantos.sim.checker import CheckError
from restaurantos.sim.cli import app, build_parser


def test_parsers() -> None:
    parser = build_parser()
    check = parser.parse_args(["check", "--run", "a", "--run", "a-r1", "--allow-unacked"])
    assert check.runs == ["a", "a-r1"] and check.allow_unacked and not check.json
    assert check.port is None
    with pytest.raises(SystemExit):
        parser.parse_args(["check"])
    restore = parser.parse_args(["snapshot", "restore", "s1", "--replace-cluster-file"])
    assert restore.name == "s1" and restore.replace_cluster_file
    assert restore.services == "auto"
    restore = parser.parse_args(["snapshot", "restore", "s1", "--services", "containers"])
    assert restore.services == "containers"
    with pytest.raises(SystemExit):
        parser.parse_args(["snapshot", "restore", "s1", "--services", "both"])
    assert parser.parse_args(["snapshot", "list"]).snapshot_command == "list"
    assert parser.parse_args(["playback", "start", "--skip-first-restock"]).skip_first_restock


def test_check_exit_codes_and_json(monkeypatch, capsys, tmp_path) -> None:
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 1\n", encoding="utf-8")
    verdict = {
        "passed": False,
        "acked": 1,
        "stored": 0,
        "hq": 1,
        "lost_at_failover": {"count": 0, "order_ids": []},
        "duplicates": [],
        "unaccounted": [
            {"external_order_id": "7", "reason": "acked_not_stored", "region_id": "r1"}
        ],
        "stored_unacked": {"count": 0, "order_ids": [], "allowed": False},
        "runs": ["a"],
        "days": [21, 21],
    }
    monkeypatch.setattr(jobs_cli, "run_check", lambda *a, **k: verdict)
    assert app(["check", "--run", "a", "--config", str(cluster_file), "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["unaccounted"][0]["external_order_id"] == "7"
    assert app(["check", "--run", "a", "--config", str(cluster_file)]) == 1
    assert "7 acked_not_stored (r1)" in capsys.readouterr().out
    monkeypatch.setattr(jobs_cli, "run_check", lambda *a, **k: {**verdict, "passed": True})
    assert app(["check", "--run", "a", "--config", str(cluster_file), "--json"]) == 0


def test_check_errors_are_messages(monkeypatch, capsys, tmp_path) -> None:
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 1\n", encoding="utf-8")

    def broken(*args, **kwargs):
        raise CheckError("the queues did not drain in 300 s - outbox on r1-primary (r1): 4 left")

    monkeypatch.setattr(jobs_cli, "run_check", broken)
    assert app(["check", "--run", "a", "--config", str(cluster_file)]) == 1
    assert "rosctl check: the queues did not drain" in capsys.readouterr().err


def test_check_asks_the_playback_host_for_the_live_run(monkeypatch, capsys, tmp_path) -> None:
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 1\n", encoding="utf-8")
    seen: dict = {}

    def run_check(*args, **kwargs):
        seen.update(kwargs)
        raise CheckError("run a is still live on the playback host")

    ports: list = []

    def live_run(sim, port=None):
        ports.append(port)
        return "a"

    monkeypatch.setattr(jobs_cli, "_live_run", live_run)
    monkeypatch.setattr(jobs_cli, "run_check", run_check)
    assert app(["check", "--run", "a", "--config", str(cluster_file)]) == 1
    assert seen["live_run"] == "a"
    # --port asks the playback host on that port, like snapshot save/restore
    assert app(["check", "--run", "a", "--config", str(cluster_file), "--port", "8099"]) == 1
    assert ports == [None, 8099]
    assert "still live" in capsys.readouterr().err


def test_snapshot_list_with_none(monkeypatch, capsys, tmp_path) -> None:
    from restaurantos.sim.config import get_sim_settings

    monkeypatch.setenv("ROS_SIM_SNAPSHOT_DIR", str(tmp_path / "nothing"))
    get_sim_settings.cache_clear()
    try:
        assert app(["snapshot", "list"]) == 0
        assert json.loads(capsys.readouterr().out) == []
    finally:
        get_sim_settings.cache_clear()


def test_snapshot_restore_passes_the_services_mode(monkeypatch, capsys, tmp_path) -> None:
    from restaurantos.sim.snapshot import SERVICE_MODES

    # the parser takes every mode the restore has
    for mode in SERVICE_MODES:
        args = build_parser().parse_args(["snapshot", "restore", "s1", "--services", mode])
        assert args.services == mode
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 1\n", encoding="utf-8")
    seen: dict = {}

    async def restore(name, ctx, *, replace_cluster_file=False, services="auto"):
        seen.update(name=name, services=services)
        return {"name": name, "warnings": []}

    monkeypatch.setattr(jobs_cli, "restore", restore)
    monkeypatch.setattr(jobs_cli, "default_context", lambda *a, **kw: object())
    argv = ["snapshot", "restore", "s1", "--services", "none", "--config", str(cluster_file)]
    assert app(argv) == 0
    assert seen == {"name": "s1", "services": "none"}


def test_live_run_asks_the_given_port(monkeypatch) -> None:
    import restaurantos.sim.snapshot as snapshot
    from restaurantos.sim.config import SimSettings

    bases: list[str] = []

    class Client:
        def __init__(self, base, timeout_s):
            bases.append(base)

        async def status(self):
            return {"state": "running", "run_id": "a"}

    monkeypatch.setattr(snapshot, "PlaybackClient", Client)
    sim = SimSettings(_env_file=None)
    assert jobs_cli._live_run(sim, 8099) == "a"
    assert jobs_cli._live_run(sim) == "a"
    assert bases == [
        f"http://{sim.control_host}:8099",
        f"http://{sim.control_host}:{sim.control_port}",
    ]

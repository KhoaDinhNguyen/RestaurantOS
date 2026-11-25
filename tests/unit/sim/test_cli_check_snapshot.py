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
    with pytest.raises(SystemExit):
        parser.parse_args(["check"])
    restore = parser.parse_args(["snapshot", "restore", "s1", "--replace-cluster-file"])
    assert restore.name == "s1" and restore.replace_cluster_file
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

    monkeypatch.setattr(jobs_cli, "_live_run", lambda sim: "a")
    monkeypatch.setattr(jobs_cli, "run_check", run_check)
    assert app(["check", "--run", "a", "--config", str(cluster_file)]) == 1
    assert seen["live_run"] == "a"
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

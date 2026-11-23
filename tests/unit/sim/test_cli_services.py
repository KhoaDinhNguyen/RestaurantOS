"""The CLI wiring for up/down: the parser shape (positional names, as
the briefs spell them) and the error paths that must not spawn
anything. The spawning behaviour itself is covered by
test_service_lifecycle.py against fake targets.

Every behaviour test runs in its own tmp_path (so a developer's .env
or infra/cluster.toml cannot change the result) with the platform
check patched to pass, so the suite also runs where there is no
/proc."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from restaurantos.sim import services
from restaurantos.sim.cli import NOT_BUILT, app, build_parser

CLUSTER_TOML = "[cluster]\nregions = 1\n"
"""A cluster file for tests that need the cluster loaded; load_cluster
defaults everything else."""


def _host_like(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(services, "_has_proc", lambda: True)


def test_up_and_down_left_the_not_built_list() -> None:
    assert "up" not in NOT_BUILT and "down" not in NOT_BUILT
    assert "control" in NOT_BUILT  # WP6b keeps its placeholder


def test_parser_takes_positional_service_names() -> None:
    parser = build_parser()
    up = parser.parse_args(["up", "gateway", "relay-r1"])
    assert up.command == "up" and up.names == ["gateway", "relay-r1"]
    down = parser.parse_args(["down", "edge-r1"])
    assert down.command == "down" and down.names == ["edge-r1"]
    assert parser.parse_args(["up"]).names == []
    assert parser.parse_args(["down"]).names == []


def test_unknown_service_name_is_an_error_that_spawns_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _host_like(tmp_path, monkeypatch)
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text(CLUSTER_TOML, encoding="utf-8")
    # an explicit --config, so the error comes from the name check -
    # never from settings, and nothing is spawned
    code = app(["up", "nope", "--config", str(cluster_file), "--no-precheck"])
    err = capsys.readouterr().err
    assert code == 1
    assert "no service nope" in err
    assert "valid names" in err
    assert not (tmp_path / "runs").exists()


def test_a_plain_down_needs_no_cluster_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # no cluster.toml, no .env, nothing: the pid files alone name the
    # work, so a broken cluster file must not stop down from cleaning up
    _host_like(tmp_path, monkeypatch)
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    ticks = services._start_ticks(dead.pid)
    dead.wait()
    services_dir = tmp_path / "runs" / "services"
    services_dir.mkdir(parents=True)
    # a patterned name, so the sweep can rebuild its spec from the
    # name alone - that is the whole point of the pid-file-driven down
    services._write_pid_file(services_dir / "relay-r1.pid", dead.pid, ticks)
    assert app(["down"]) == 0
    out = capsys.readouterr().out
    assert "relay-r1: not-running" in out
    assert not (services_dir / "relay-r1.pid").exists()


def test_control_is_still_not_built(capsys: pytest.CaptureFixture[str]) -> None:
    assert app(["control"]) == 2

"""The CLI wiring for up/down: the parser shape (positional names, as
the briefs spell them) and the error paths that must not spawn
anything. The spawning behaviour itself is covered by
test_service_lifecycle.py against fake targets.

Every behaviour test runs in its own tmp_path (so a developer's .env
or infra/cluster.toml cannot change the result) with the platform
check patched to pass, so the suite also runs where there is no
/proc."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
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
    # no cluster.toml, no .env, no real process either: a fixed record
    # that is simply not running. The pid files alone name the work, so
    # a broken cluster file must not stop down from cleaning up. The
    # lock and liveness are patched, so the test also runs where there
    # is no /proc and no flock; the real CLI dispatch, the pid-file
    # discovery and the record removal are what run
    _host_like(tmp_path, monkeypatch)

    @contextlib.contextmanager
    def _null_lock(runs_dir: Path, name: str) -> Iterator[None]:
        yield

    monkeypatch.setattr(services, "_service_lock", _null_lock)
    monkeypatch.setattr(services, "_liveness", lambda pid: "dead")
    services_dir = tmp_path / "runs" / "services"
    services_dir.mkdir(parents=True)
    # a patterned name, so the sweep can rebuild its spec from the
    # name alone - that is the whole point of the pid-file-driven down
    services._write_pid_file(services_dir / "relay-r1.pid", 4194000, 12345)
    assert app(["down"]) == 0
    out = capsys.readouterr().out
    assert "relay-r1: not-running" in out
    assert not (services_dir / "relay-r1.pid").exists()


def test_a_services_folder_that_cannot_be_listed_is_an_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Path.glob silently returns nothing when a folder cannot be read;
    # down must not say "nothing running" while services may run. A
    # services path that is not a folder at all is the same class of
    # problem: an error message and exit 1, not a clean no-op
    _host_like(tmp_path, monkeypatch)
    (tmp_path / "runs").mkdir()
    (tmp_path / "runs" / "services").write_text("a file, not a folder", encoding="utf-8")
    assert app(["down"]) == 1
    assert "rosctl down:" in capsys.readouterr().err


def test_a_badly_shaped_cluster_file_is_a_message_not_a_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # load_cluster names the offending key in a ValueError, and the CLI
    # handler turns it into a one-line message with exit 1
    _host_like(tmp_path, monkeypatch)
    bad = tmp_path / "bad.toml"
    bad.write_text('[cluster]\nregions = "3"\n', encoding="utf-8")
    code = app(["up", "--config", str(bad), "--no-precheck"])
    assert code == 1
    assert "rosctl up: cluster file: regions must be an int" in capsys.readouterr().err
    assert not (tmp_path / "runs").exists()


def test_control_is_still_not_built(capsys: pytest.CaptureFixture[str]) -> None:
    assert app(["control"]) == 2

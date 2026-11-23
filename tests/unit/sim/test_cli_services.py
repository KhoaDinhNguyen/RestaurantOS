"""The CLI wiring for up/down: the parser shape (positional names, as
the briefs spell them) and the error paths that must not spawn
anything. The spawning behaviour itself is covered by
test_service_lifecycle.py against fake targets."""

from __future__ import annotations

import pytest

from restaurantos.sim.cli import NOT_BUILT, app, build_parser


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
    capsys: pytest.CaptureFixture[str],
) -> None:
    # no chdir: the error must come from the name check, after
    # load_cluster succeeded on the repo's own infra/cluster.toml
    code = app(["up", "nope", "--no-precheck"])
    err = capsys.readouterr().err
    assert code == 1
    assert "no service nope" in err
    assert "valid names" in err


def test_control_is_still_not_built(capsys: pytest.CaptureFixture[str]) -> None:
    assert app(["control"]) == 2

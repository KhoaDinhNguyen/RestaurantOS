"""The `rosctl playback` subcommands: argument wiring and the failure
paths a user actually hits. The happy paths are covered by the engine
and control tests; here we check that the CLI shapes and messages are
right without needing a live run or a cluster."""

from __future__ import annotations

import pytest

from restaurantos.sim.cli import app


@pytest.fixture
def fresh_settings():
    """get_settings() is cached per process; tests that point
    ROS_DATA_DIR elsewhere must start from a clean cache and leave one
    behind (the established pattern in test_toxiproxy.py)."""
    from restaurantos.common.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_help_works(capsys):
    # argparse prints help and raises SystemExit(0); assert the exit code
    for args in (
        ["playback", "--help"],
        *[
            ["playback", command, "--help"]
            for command in ("start", "pause", "resume", "stop", "speed", "status", "build-cache")
        ],
    ):
        with pytest.raises(SystemExit) as caught:
            app(args)
        assert caught.value.code == 0, args


def test_subcommands_are_registered(capsys):
    with pytest.raises(SystemExit):
        app(["playback"])
    out = capsys.readouterr()
    for command in ("start", "pause", "resume", "stop", "speed", "status", "build-cache"):
        assert command in out.out + out.err


def test_status_without_a_live_run_says_so(capsys):
    # port 1: nothing listens there; the CLI must say what to do, not
    # show a traceback
    rc = app(["playback", "status", "--port", "1"])
    assert rc == 1
    assert "no playback control server" in capsys.readouterr().err


def test_pause_without_a_live_run_says_so(capsys):
    rc = app(["playback", "pause", "--port", "1"])
    assert rc == 1
    assert "is a run live" in capsys.readouterr().err


def test_stop_without_a_live_run_says_so(capsys):
    rc = app(["playback", "stop", "--port", "1"])
    assert rc == 1
    assert "is a run live" in capsys.readouterr().err


def test_speed_must_be_positive(capsys):
    assert app(["playback", "speed", "-5", "--port", "1"]) == 1
    assert "positive" in capsys.readouterr().err


def test_stop_gets_a_timeout_that_outlives_the_drain(fresh_settings):
    """`stop` waits for the server's drain (up to the drain timeout)
    before answering; the request must not time out first (review B2)."""
    from restaurantos.sim.config import get_sim_settings
    from restaurantos.sim.playback_cli import _control_timeout_s

    sim = get_sim_settings()
    assert sim.playback_drain_timeout_s == 30.0
    assert _control_timeout_s(sim, stop=True) == 30.0 + 5.0 + 5.0
    assert _control_timeout_s(sim) == sim.http_timeout_s


async def test_status_reports_http_errors_as_messages(capsys, monkeypatch):
    """A slow or broken control server is a message, not a traceback
    (review B2: status only caught ConnectError)."""
    import httpx

    from restaurantos.sim import playback_cli

    class NoServer:
        def __init__(self, timeout=None):
            raise httpx.ReadTimeout("server too slow", request=None)

    monkeypatch.setattr(playback_cli.httpx, "AsyncClient", NoServer)
    import argparse

    from restaurantos.sim.playback_cli import status_command

    args = argparse.Namespace(port=1)
    rc = await status_command(args)
    assert rc == 1
    assert "rosctl playback status:" in capsys.readouterr().err


def test_build_cache_reports_a_missing_manifest(tmp_path, capsys, monkeypatch, fresh_settings):
    monkeypatch.setenv("ROS_DATA_DIR", str(tmp_path / "empty"))
    rc = app(["playback", "build-cache"])
    assert rc == 1
    assert "MANIFEST.csv" in capsys.readouterr().err


def test_start_refuses_a_window_inside_the_warmup(tmp_path, monkeypatch, capsys, fresh_settings):
    # a tiny valid data directory whose cache starts at day 14
    import hashlib

    from restaurantos.sim.loading import REFERENCE_COLUMNS
    from restaurantos.sim.replaydata import build_cache

    data = tmp_path / "data"
    data.mkdir()
    manifest = ["file,rows,sha256"]
    for name, columns in REFERENCE_COLUMNS.items():
        (data / name).write_text(",".join(columns) + "\n", encoding="utf-8")
        raw = (data / name).read_bytes()
        manifest.append(f"{name},0,{hashlib.sha256(raw).hexdigest()}")
    orders = "order_id,customer_id,vendor_id,geohash,day_of_week,order_day,order_time\n"
    orders += "1,c1,v1,w21z7,3,14,12:00:00\n"
    (data / "orders.csv").write_text(orders, encoding="utf-8")
    items = "order_id,product_id,quantity\n1,p1,1\n"
    (data / "order_items.csv").write_text(items, encoding="utf-8")
    for name in ("orders.csv", "order_items.csv"):
        raw = (data / name).read_bytes()
        rows = len(raw.decode().strip().splitlines()) - 1
        manifest.append(f"{name},{rows},{hashlib.sha256(raw).hexdigest()}")
    (data / "MANIFEST.csv").write_text("\n".join(manifest) + "\n", encoding="utf-8")

    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    build_cache(data, warm_up_days=14, quiet=True)

    rc = app(["playback", "start", "--from-day", "13", "--port", "1"])
    assert rc == 1
    assert "warm-up" in capsys.readouterr().err

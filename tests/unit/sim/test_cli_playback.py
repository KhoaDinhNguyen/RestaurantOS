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


async def test_a_wrong_state_prints_the_message_field(capsys, monkeypatch):
    """API review 4.5: the control plane's 409 body carries `message`;
    the CLI prints it, falling back to `detail` for any other 409
    shape."""
    import argparse

    import httpx

    from restaurantos.sim import playback_cli

    class Server:
        def __init__(self, timeout=None, body=None):
            self.body = body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None):
            return httpx.Response(409, json=self.body)

    from restaurantos.sim.playback_cli import control_command

    async def run(body: dict) -> str:
        monkeypatch.setattr(
            playback_cli.httpx,
            "AsyncClient",
            lambda timeout=None: Server(timeout=timeout, body=body),
        )
        args = argparse.Namespace(port=1, play_command="pause")
        rc = await control_command(args, method="POST", body={"action": "pause"})
        assert rc == 1
        return capsys.readouterr().err

    err = await run(
        {
            "error": "invalid_state",
            "state": "running",
            "message": "cannot pause while the run is running",
        }
    )
    assert "cannot pause while the run is running" in err

    err = await run({"detail": "an old-shaped conflict"})
    assert "an old-shaped conflict" in err


def test_build_cache_reports_a_missing_manifest(tmp_path, capsys, monkeypatch, fresh_settings):
    monkeypatch.setenv("ROS_DATA_DIR", str(tmp_path / "empty"))
    rc = app(["playback", "build-cache"])
    assert rc == 1
    assert "MANIFEST.csv" in capsys.readouterr().err


def test_start_help_lists_the_extras_flags(capsys):
    with pytest.raises(SystemExit):
        app(["playback", "start", "--help"])
    out = capsys.readouterr().out
    for flag in ("--events", "--no-events", "--event-sample-rate", "--restock-days"):
        assert flag in out


# --- restock helpers ---------------------------------------------------------


def test_restock_days_are_the_interval_days_inside_the_window():
    from restaurantos.sim.playback import restock_days

    # W=14, R=7: the first restock is a full interval after day W
    assert restock_days(14, 7, 14, 90) == [21, 28, 35, 42, 49, 56, 63, 70, 77, 84]
    # a window that starts later keeps only the days it contains
    assert restock_days(14, 7, 25, 90) == [28, 35, 42, 49, 56, 63, 70, 77, 84]
    # a window that ends before the first restock day restocks nothing
    assert restock_days(14, 7, 14, 20) == []
    assert restock_days(14, 7, 21, 27) == [21]

    with pytest.raises(ValueError, match="positive"):
        restock_days(14, 0, 14, 90)


def test_restock_vendors_are_sorted_and_header_checked(tmp_path):
    from restaurantos.sim.loading import REFERENCE_COLUMNS
    from restaurantos.sim.playback_cli import restock_vendors

    columns = ",".join(REFERENCE_COLUMNS["vendors.csv"])
    (tmp_path / "vendors.csv").write_text(
        columns + "\nv9,,w21z7,x\nv1,,w21z7,x\nv5,,w21z6,y\n", encoding="utf-8"
    )
    assert restock_vendors(tmp_path) == ["v1", "v5", "v9"]

    (tmp_path / "vendors.csv").write_text("wrong,header\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="header"):
        restock_vendors(tmp_path)


def test_start_refuses_a_sample_rate_outside_zero_to_one(capsys):
    """Review item 6: the CLI override skips SimSettings validation;
    a negative rate would silently drop every event, a rate above 1
    silently keep them all. Both refuse before anything is built."""
    for bad in ("1.5", "-0.1", "nan", "inf"):
        rc = app(["playback", "start", "--event-sample-rate", bad, "--port", "1"])
        assert rc == 1, bad
        assert "must be a number in [0, 1]" in capsys.readouterr().err, bad


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


def test_start_shortens_a_to_day_past_the_data(tmp_path, monkeypatch, capsys, fresh_settings):
    """Review round 2, item 4: a to_day past the last cached day is
    shortened, with a warning before the banner, and the banner's
    restock-day count comes from the shortened window. The engine
    boundary is faked: the CLI's own reporting is what is under test."""
    import asyncio

    import uvicorn
    from test_replaydata import make_data_dir

    import restaurantos.sim.playback_cli as playback_cli
    from restaurantos.sim.replaydata import build_cache, load_meta

    orders = [
        ["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"],
        ["2", "c1", "v1", "w21z7", "3", 15, "12:00:00"],
    ]
    data = make_data_dir(tmp_path, orders, [["1", "p1", 1], ["2", "p1", 1]])
    build_cache(data, warm_up_days=14, quiet=True)
    meta = load_meta(data)
    assert meta is not None

    class FakeServer:
        started = True

        def __init__(self, config):
            self.should_exit = False

        async def serve(self):
            while not self.should_exit:
                await asyncio.sleep(0.01)

    class FakePlayback:
        default_speed = 1000.0

        def __init__(self, *args, **kwargs):
            self._state = "idle"

        async def start(self, **kwargs):
            self._state = "done"

        async def pause(self):
            pass

        async def resume(self):
            pass

        async def stop(self):
            pass

        async def set_speed(self, speed):
            pass

        async def wait_done(self):
            pass

        @property
        def state(self):
            return self._state

        def status(self):
            return {
                "run_id": "fake",
                "state": self._state,
                "orders_sent": 0,
                "orders_acked": 0,
                "orders_parked": 0,
                "orders_refused": 0,
                "orders_failed": 0,
            }

        def clock(self):
            return {}

    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    monkeypatch.setattr(playback_cli, "Playback", FakePlayback)

    rc = app(
        [
            "playback",
            "start",
            "--from-day",
            "14",
            "--to-day",
            "40",
            "--restock-days",
            "1",
            "--port",
            "1",
        ]
    )
    assert rc == 0
    err = capsys.readouterr().err
    assert (
        f"data only goes to day {meta.last_day}; "
        f"running to day {meta.last_day} instead of 40" in err
    )
    # the banner's restock count comes from the shortened window: day
    # 15 alone (the first restock is W+R), not 15 through 40
    assert "restock on 1 day(s)" in err
    # and the banner names the effective end, not the requested one
    assert f"started: 14 -> {meta.last_day}," in err

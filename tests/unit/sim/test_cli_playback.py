"""The `rosctl playback` subcommands: argument wiring and the failure
paths a user actually hits. The happy paths are covered by the engine
and control tests; here we check that the CLI shapes and messages are
right without needing a live run or a cluster."""

from __future__ import annotations

from pathlib import Path

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


def _vendors_csv_dir(tmp_path, text: str) -> Path:
    """A data dir holding exactly this vendors.csv plus a MANIFEST.csv
    entry that matches it (or not, when the caller edits one after)."""
    import hashlib
    from pathlib import Path as _Path

    from restaurantos.sim.loading import REFERENCE_COLUMNS

    tmp_path = _Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "vendors.csv").write_text(text, encoding="utf-8")
    raw = (tmp_path / "vendors.csv").read_bytes()
    (tmp_path / "MANIFEST.csv").write_text(
        "file,rows,sha256\n"
        f"vendors.csv,1,{hashlib.sha256(raw).hexdigest()}\n"
        + "".join(
            f"{name},0,{hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()}\n"
            if (tmp_path / name).exists()
            else ""
            for name in REFERENCE_COLUMNS
        ),
        encoding="utf-8",
    )
    return tmp_path


def test_restock_vendors_are_sorted_and_header_checked(tmp_path):
    from restaurantos.sim.playback_cli import restock_vendors

    header = "vendor_id,chain_id,geohash,primary_cuisine"
    data = _vendors_csv_dir(tmp_path, header + "\nv9,,w21z7,x\nv1,,w21z7,x\nv5,,w21z6,y\n")
    assert restock_vendors(data) == [
        ("v1", "w21z7"),
        ("v5", "w21z6"),
        ("v9", "w21z7"),
    ]

    # a fresh dir whose manifest matches: the header check is what
    # fires, and since round 4 it is a ValueError like every other data
    # error, not a RuntimeError the CLI does not turn into a message
    bad = _vendors_csv_dir(tmp_path / "bad-header", "wrong,header\n")
    with pytest.raises(ValueError, match="header"):
        restock_vendors(bad)


def test_restock_vendors_reject_a_blank_line_with_its_number(tmp_path):
    """Review round 2, item 7: a blank line used to raise an IndexError
    out of the CLI; now it is a ValueError naming the line (the header
    is line 1), and the CLI already turns ValueError into exit 1."""
    from restaurantos.sim.playback_cli import restock_vendors

    data = _vendors_csv_dir(
        tmp_path,
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n\nv2,,w21z7,x\n",
    )
    with pytest.raises(ValueError, match=r"vendors.csv line 3: expected 4 values, got 0"):
        restock_vendors(data)


def test_restock_vendors_reject_an_empty_vendor_id(tmp_path):
    """Review round 3, item 5: a four-column row with an empty
    vendor_id reaches the id check (the value count is checked first,
    with its own message) and is refused there."""
    from restaurantos.sim.playback_cli import restock_vendors

    data = _vendors_csv_dir(tmp_path, "vendor_id,chain_id,geohash,primary_cuisine\n,,w21z7,x\n")
    with pytest.raises(ValueError, match=r"vendors.csv line 2: empty vendor_id"):
        restock_vendors(data)


def test_restock_vendors_refuse_a_file_that_left_the_manifest(tmp_path):
    """Review round 2, item 7: on a cache hit nothing bound vendors.csv
    to MANIFEST.csv, so a changed file would restock a different vendor
    set than the one bootstrap loaded. Now the sha256 is checked."""
    from restaurantos.sim.playback_cli import restock_vendors

    data = _vendors_csv_dir(tmp_path, "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n")
    # the vendor set changes after the manifest was written
    (data / "vendors.csv").write_text(
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\nv2,,w21z7,x\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="does not match its MANIFEST.csv entry"):
        restock_vendors(data)


def test_vendor_regions_match_bootstraps_shards():
    """Review round 2, item 6: the CLI's vendor -> region map is
    bootstrap's own sharding, not a second derivation - the same
    vendor_counts + assign_shards walk, cross-checked here against
    bootstrap's `_vendors_by_cell` over the mini fixture."""
    from restaurantos.common.cluster import Cluster
    from restaurantos.sim.bootstrap import _vendors_by_cell
    from restaurantos.sim.loading import REFERENCE_COLUMNS, read_rows
    from restaurantos.sim.mini import mini_dir
    from restaurantos.sim.regions import vendor_region_map
    from restaurantos.sim.shard import assign_shards, counts_by_cell, vendor_counts

    data = mini_dir()
    cluster = Cluster(regions=3)
    cell_region = dict(assign_shards(vendor_counts(data), cluster.region_ids))
    by_cell = _vendors_by_cell(data)
    expected = {
        vendor: cell_region[cell]
        for cell, rid in cell_region.items()
        for vendor in by_cell.get(cell, ())
    }
    columns = REFERENCE_COLUMNS["vendors.csv"]
    geohash_column = columns.index("geohash")
    pairs = sorted(
        (row[0], row[geohash_column]) for row in read_rows(data / "vendors.csv", columns)
    )
    regions = vendor_region_map(pairs, counts_by_cell(pairs), cluster)
    assert regions == expected
    assert set(regions) == set(expected)  # every vendor is covered


def test_start_refuses_a_sample_rate_outside_zero_to_one(capsys):
    """Review item 6: the CLI override skips SimSettings validation;
    a negative rate would silently drop every event, a rate above 1
    silently keep them all. Both refuse before anything is built."""
    for bad in ("1.5", "-0.1", "nan", "inf"):
        rc = app(["playback", "start", "--event-sample-rate", bad, "--port", "1"])
        assert rc == 1, bad
        assert "must be a number in [0, 1]" in capsys.readouterr().err, bad


def test_start_refuses_a_duplicate_rate_outside_zero_to_one(capsys):
    """The duplicates PR: the same discipline as the sample rate - the
    CLI override skips pydantic, so a bad rate refuses here, before the
    cache is built or anything starts."""
    for bad in ("1.5", "-0.1", "nan", "inf"):
        rc = app(["playback", "start", "--duplicate-rate", bad, "--port", "1"])
        assert rc == 1, bad
        assert "must be a number in [0, 1]" in capsys.readouterr().err, bad


def test_start_refuses_a_negative_duplicate_offset(capsys):
    # "-inf" would read as an option flag, so the = form; -1 and nan are
    # plain values
    for bad in ("-1", "nan"):
        rc = app(["playback", "start", "--duplicate-offset-s", bad, "--port", "1"])
        assert rc == 1, bad
        assert "must be a number >= 0" in capsys.readouterr().err, bad
    rc = app(["playback", "start", "--duplicate-offset-s=-inf", "--port", "1"])
    assert rc == 1
    assert "must be a number >= 0" in capsys.readouterr().err


def test_start_refuses_a_shuffle_window_below_one(capsys):
    rc = app(["playback", "start", "--shuffle-window", "0", "--port", "1"])
    assert rc == 1
    assert "must be at least 1" in capsys.readouterr().err


def test_start_refuses_shuffle_with_sequential(capsys):
    """Sequential mode's whole point is in-order, one-at-a-time
    delivery the oracle can match; shuffle would quietly break it.
    The engine refuses the combination too - this is the CLI saying it
    before anything is built."""
    rc = app(["playback", "start", "--from-day", "14", "--shuffle", "--sequential", "--port", "1"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "--shuffle cannot run with --sequential" in err
    assert "duplicates are allowed in any mode" in err


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
    boundary is faked: the CLI's own reporting is what is under test.

    Review round 3, item 8: the settings this test's numbers ride on
    are pinned - ROS_SIM_STOCK_WARM_UP_DAYS=14 with the
    get_sim_settings cache cleared around the test, so no stray
    variable or .env on a dev machine moves the numbers - and the run
    passes --kitchen-target, so it needs no cluster file."""
    from restaurantos.sim.config import get_sim_settings
    from restaurantos.sim.replaydata import load_meta

    get_sim_settings.cache_clear()
    monkeypatch.setenv("ROS_SIM_STOCK_WARM_UP_DAYS", "14")
    try:
        orders = [
            ["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"],
            ["2", "c1", "v1", "w21z7", "3", 15, "12:00:00"],
        ]
        data = _make_data_with_vendors(
            tmp_path,
            orders,
            [["1", "p1", 1], ["2", "p1", 1]],
            "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n",
        )
        meta = load_meta(data)
        assert meta is not None

        captured: dict = {}
        _install_fake_start(monkeypatch, captured)
        monkeypatch.setenv("ROS_DATA_DIR", str(data))
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
                "--kitchen-target",
                "http://127.0.0.1:8900/api/v1",
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
    finally:
        get_sim_settings.cache_clear()


def _install_fake_start(monkeypatch, captured: dict) -> None:
    """The engine boundary faked: the CLI's own wiring (routing
    arguments, meta, exit codes) is what is under test. FakePlayback
    records the kwargs it was built with; a start that would dispatch
    to the real control loop is not needed here."""
    import asyncio

    import uvicorn

    import restaurantos.sim.playback_cli as playback_cli

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
            captured.update(kwargs)

        async def start(self, **kwargs):
            self._state = "done"
            captured["start_kwargs"] = kwargs

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
                "orders_resent": 0,
                "events_sent": 0,
                "events_acked": 0,
                "events_duplicate": 0,
                "events_conflicted": 0,
                "events_failed": 0,
                "events_skipped": 0,
                "restocks_sent": 0,
                "restocks_acked": 0,
                "restocks_failed": 0,
            }

        def clock(self):
            return {}

    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    monkeypatch.setattr(playback_cli, "Playback", FakePlayback)


def _make_data_with_vendors(tmp_path, orders, items, vendors_csv: str) -> Path:
    """A built cache over a data dir whose vendors.csv is in the
    manifest, so restock_vendors accepts it."""
    import hashlib

    from test_replaydata import make_data_dir

    from restaurantos.sim.replaydata import build_cache

    data = make_data_dir(tmp_path, orders, items)
    (data / "vendors.csv").write_text(vendors_csv, encoding="utf-8")
    raw = (data / "vendors.csv").read_bytes()
    manifest = [
        line
        for line in (data / "MANIFEST.csv").read_text().splitlines()
        if not line.startswith("vendors.csv,")
    ]
    manifest.append(
        f"vendors.csv,{vendors_csv.count(chr(10)) - 1},{hashlib.sha256(raw).hexdigest()}"
    )
    (data / "MANIFEST.csv").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    build_cache(data, warm_up_days=14, quiet=True)
    return data


def test_a_kitchen_target_run_needs_no_cluster_file(tmp_path, monkeypatch, capsys, fresh_settings):
    """Review round 3, item 3: a stub run resolves --kitchen-target
    first and never reads a cluster file, so it starts fine in a
    directory without one."""
    orders = [["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"]]
    data = _make_data_with_vendors(
        tmp_path,
        orders,
        [["1", "p1", 1]],
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n",
    )
    captured: dict = {}
    _install_fake_start(monkeypatch, captured)
    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    monkeypatch.chdir(tmp_path)  # no infra/cluster.toml in here
    rc = app(
        [
            "playback",
            "start",
            "--from-day",
            "14",
            "--to-day",
            "14",
            "--kitchen-target",
            "http://127.0.0.1:8900/api/v1",
            "--port",
            "1",
        ]
    )
    assert rc == 0
    assert captured["kitchen_url"] == "http://127.0.0.1:8900/api/v1"
    assert captured["kitchen_urls"] is None
    assert captured["vendor_regions"] == {}
    # no cluster file was used, so none is recorded
    assert "cluster" not in captured["start_kwargs"]["meta_extra"]
    # the end-of-run summary carries the kitchen results too (review
    # round 3, item 6)
    out = capsys.readouterr().out
    assert (
        "events: 0 sent, 0 acked, 0 duplicate, 0 conflicted, 0 failed, 0 skipped; "
        "restocks: 0 sent, 0 acked, 0 failed" in out
    )


def test_config_decides_the_region_routing(tmp_path, monkeypatch, capsys, fresh_settings):
    """Review round 3, item 3: routing per region loads the cluster
    file bootstrap ran with, so vendors map to both regions of a
    two-region file exactly the way bootstrap shards them."""
    orders = [["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"]]
    vendors_csv = (
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\nv2,,w21z7,x\nv3,,w21z6,y\n"
    )
    data = _make_data_with_vendors(tmp_path, orders, [["1", "p1", 1]], vendors_csv)
    cluster_file = tmp_path / "two-regions.toml"
    cluster_file.write_text("[cluster]\nregions = 2\n", encoding="utf-8")
    captured: dict = {}
    _install_fake_start(monkeypatch, captured)
    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    monkeypatch.chdir(tmp_path)
    rc = app(
        [
            "playback",
            "start",
            "--from-day",
            "14",
            "--to-day",
            "14",
            "--config",
            str(cluster_file),
            "--port",
            "1",
        ]
    )
    assert rc == 0
    from restaurantos.common.cluster import load_cluster
    from restaurantos.sim.playback_cli import restock_vendors
    from restaurantos.sim.shard import assign_shards, vendor_counts

    cluster = load_cluster(cluster_file)
    cell_region = dict(assign_shards(vendor_counts(data), cluster.region_ids))
    expected = {vendor: cell_region[geohash] for vendor, geohash in restock_vendors(data)}
    # both regions of the two-region file carry vendors
    assert set(expected.values()) == {"r1", "r2"}
    assert captured["vendor_regions"] == expected
    assert set(captured["kitchen_urls"]) == {"r1", "r2"}
    # review round 4 (PR #7): the cluster file now travels with the
    # engine (`run_meta`), so every run's meta - including one started
    # through the control API - records it, and `meta_extra` does not
    assert captured["run_meta"]["cluster"]["config"] == str(cluster_file)
    assert "cluster" not in captured["start_kwargs"]["meta_extra"]


def test_vendors_csv_is_read_once_by_a_playback_start(
    tmp_path, monkeypatch, capsys, fresh_settings
):
    """Review round 3, item 11, measured through the CLI itself (review
    round 5, item 6), now on the per-region path (review round 6,
    item 4): the cache is built first, then the open counter, then a
    playback start that routes per region - and vendors.csv is opened
    exactly once during the start. The map FakePlayback received is
    bootstrap's own map."""
    orders = [["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"]]
    vendors_csv = (
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\nv2,,w21z7,x\nv3,,w21z6,y\n"
    )
    data = _make_data_with_vendors(tmp_path, orders, [["1", "p1", 1]], vendors_csv)
    captured: dict = {}
    _install_fake_start(monkeypatch, captured)
    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    monkeypatch.setenv("ROS_SIM_STOCK_WARM_UP_DAYS", "14")
    monkeypatch.chdir(tmp_path)
    # per-region startup: load_cluster is stubbed with the two-region
    # fixture, and no --kitchen-target is given
    import restaurantos.sim.playback_cli as playback_cli
    from restaurantos.common.cluster import Cluster

    cluster = Cluster(regions=2)
    monkeypatch.setattr(playback_cli, "load_cluster", lambda _path: cluster)

    # the counter goes on after the cache is built: only the start's
    # own reads are counted
    opens: list[str] = []
    real_open = Path.open

    def counting_open(self, *args, **kwargs):
        if self.name == "vendors.csv":
            opens.append(str(self))
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)
    rc = app(["playback", "start", "--from-day", "14", "--to-day", "14", "--port", "1"])
    assert rc == 0
    assert len(opens) == 1

    # the map the engine received is bootstrap's own map: the same
    # checked rows, sharded the same way
    from restaurantos.sim.playback_cli import restock_vendors
    from restaurantos.sim.shard import assign_shards, vendor_counts

    cell_region = dict(assign_shards(vendor_counts(data), cluster.region_ids))
    expected = {vendor: cell_region[geohash] for vendor, geohash in restock_vendors(data)}
    # both regions of the two-region fixture carry vendors, so the
    # start really routed per region
    assert set(expected.values()) == {"r1", "r2"}
    assert captured["vendor_regions"] == expected
    assert set(captured["kitchen_urls"]) == {"r1", "r2"}


def test_an_empty_vendors_csv_is_a_value_error_naming_the_file(tmp_path):
    """Review round 4, item 7: next() with no default used to raise
    StopIteration out of the CLI on an empty file; check_header now
    reports the missing header, as a ValueError naming vendors.csv."""
    from restaurantos.sim.playback_cli import restock_vendors

    data = _vendors_csv_dir(tmp_path, "")
    with pytest.raises(ValueError, match="vendors.csv: header is"):
        restock_vendors(data)


def test_vendor_counts_gives_the_same_row_errors_as_playback(tmp_path):
    """Review round 4, item 7: bootstrap's vendor_counts goes through
    the same checked vendor_pairs as playback, so a short row is the
    same ValueError with its line number - and a broken header is a
    ValueError naming the file, not a RuntimeError escaping the
    CLI."""
    from restaurantos.sim.shard import vendor_counts

    (tmp_path / "vendors.csv").write_text(
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match=r"vendors.csv line 2: expected 4 values, got 3"):
        vendor_counts(tmp_path)

    (tmp_path / "vendors.csv").write_text("wrong,header\n", encoding="utf-8")
    with pytest.raises(ValueError, match="vendors.csv: header is"):
        vendor_counts(tmp_path)


def test_start_removes_its_log_handler_on_every_exit(tmp_path, monkeypatch, capsys, fresh_settings):
    """Review round 4, item 5: start_command installs a stderr handler
    on the playback logger and removes it on every exit - the early
    return for a bad --event-sample-rate included - so two starts in
    one process leave the logger exactly as they found it."""
    import logging

    orders = [["1", "c1", "v1", "w21z7", "3", 14, "12:00:00"]]
    data = _make_data_with_vendors(
        tmp_path,
        orders,
        [["1", "p1", 1]],
        "vendor_id,chain_id,geohash,primary_cuisine\nv1,,w21z7,x\n",
    )
    captured: dict = {}
    _install_fake_start(monkeypatch, captured)
    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    monkeypatch.chdir(tmp_path)
    logger = logging.getLogger("restaurantos.sim.playback")
    before = len(logger.handlers)

    # the early return, after the handler was installed
    rc_bad = app(
        [
            "playback",
            "start",
            "--from-day",
            "14",
            "--to-day",
            "14",
            "--kitchen-target",
            "http://127.0.0.1:8900/api/v1",
            "--event-sample-rate",
            "2",
            "--port",
            "1",
        ]
    )
    assert rc_bad == 1
    # a full run to the end
    rc_good = app(
        [
            "playback",
            "start",
            "--from-day",
            "14",
            "--to-day",
            "14",
            "--kitchen-target",
            "http://127.0.0.1:8900/api/v1",
            "--port",
            "1",
        ]
    )
    assert rc_good == 0
    assert len(logger.handlers) == before

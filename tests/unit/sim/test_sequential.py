"""Single-region sequential playback: the vendor-to-region resolver, the
region filter over the sorted cache, the edge target URL, and the
one-at-a-time send the brief's `--sequential` promises.

Two engine tests pin the sequential mode down. A gated transport under
the fake clock proves no request starts before the previous one was
answered; a slow transport with a drain timeout far smaller than the
total send time proves the drain can no longer cancel the tail of a
sequential run. The CLI tests drive `start_command` through the real
`rosctl` parser with an injected transport, so they assert what the CLI
actually sends: URLs, bodies, order, overlap - against a hermetic
two-region data directory, no free local ports or live services needed.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import socket
import time
from pathlib import Path

import httpx
import pytest
from conftest import FakeWallClock, drive, make_order, make_playback, settle

from restaurantos.common.cluster import load_cluster
from restaurantos.common.config import get_settings
from restaurantos.sim import playback_cli
from restaurantos.sim.cli import app, build_parser
from restaurantos.sim.loading import REFERENCE_COLUMNS, REFERENCE_ORDER
from restaurantos.sim.playback_cli import restock_vendors
from restaurantos.sim.regions import edge_target_url, region_order_counts, vendor_region_map
from restaurantos.sim.replaydata import build_cache, count_orders, iter_orders
from restaurantos.sim.shard import assign_shards, counts_by_cell
from restaurantos.sim.stub import make_stub_app

MINI = Path("tests/fixtures/mini")

# The hermetic two-region fixture: two far-apart cells, one vendor each,
# so the greedy cut puts aaaaa in r1 and zzzzz in r2, and six orders
# whose replay order alternates the vendors.
R1_ORDERS = ["1", "3", "5"]
ALL_ORDERS = ["1", "2", "3", "4", "5", "6"]


def make_region_data_dir(root: Path, *, rogue: bool = False) -> Path:
    """A data directory that passes check_manifest and splits in two.

    `rogue=True` rewrites one order to a vendor that is not in
    vendors.csv: the run must refuse, not silently drop it.
    """
    data = root / ("data-rogue" if rogue else "data")
    data.mkdir(parents=True)
    for name, columns in REFERENCE_COLUMNS.items():
        if name != "vendors.csv":
            (data / name).write_text(",".join(columns) + "\n", encoding="utf-8")
    (data / "vendors.csv").write_text(
        "vendor_id,chain_id,geohash,primary_cuisine\nvA,,aaaaa,american\nvB,,zzzzz,noodles\n",
        encoding="utf-8",
    )
    order_rows = [
        "1,cA,vA,aaaaa,3,14,08:00:00",
        f"2,cB,{'vX' if rogue else 'vB'},zzzzz,3,14,09:00:00",
        "3,cA,vA,aaaaa,3,14,10:00:00",
        "4,cB,vB,zzzzz,3,14,11:00:00",
        "5,cA,vA,aaaaa,4,15,08:00:00",
        "6,cB,vB,zzzzz,4,16,08:00:00",
    ]
    (data / "orders.csv").write_text(
        "order_id,customer_id,vendor_id,geohash,day_of_week,order_day,order_time\n"
        + "\n".join(order_rows)
        + "\n",
        encoding="utf-8",
    )
    (data / "order_items.csv").write_text(
        "order_id,product_id,quantity\n" + "".join(f"{oid},pA,1\n" for oid in ALL_ORDERS),
        encoding="utf-8",
    )
    manifest = ["file,rows,sha256"]
    for name in [*REFERENCE_ORDER, "orders.csv", "order_items.csv"]:
        raw = (data / name).read_bytes()
        rows = len(raw.decode().strip().splitlines()) - 1
        manifest.append(f"{name},{rows},{hashlib.sha256(raw).hexdigest()}")
    (data / "MANIFEST.csv").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    return data


def two_region_cluster_file(root: Path) -> Path:
    path = root / "cluster.toml"
    path.write_text("[cluster]\nregions = 2\nstandbys = false\n", encoding="utf-8")
    return path


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --- the resolver -----------------------------------------------------------


def test_resolver_matches_the_bootstrap_split_on_the_mini_fixture(tmp_path: Path) -> None:
    # Hand-checked greedy cut for cells w21z6(1), w21z7(2), w21zt(1) cut
    # in two: w21z6 and w21z7 fall to r1, w21zt to r2. The pairs are the
    # same rows `restock_vendors` yields in production, read straight
    # from vendors.csv here because the mini fixture ships no
    # MANIFEST.csv to check against.
    with (MINI / "vendors.csv").open(newline="", encoding="utf-8") as f:
        pairs = sorted((row["vendor_id"], row["geohash"]) for row in csv.DictReader(f))
    counts = counts_by_cell(pairs)
    cluster = load_cluster(two_region_cluster_file(tmp_path))
    got = vendor_region_map(pairs, counts, cluster)
    assert got == {"v001": "r1", "v002": "r1", "v003": "r1", "v004": "r2"}
    # one region: every cell falls to it
    one_file = tmp_path / "one.toml"
    one_file.write_text("[cluster]\nregions = 1\nstandbys = false\n", encoding="utf-8")
    single = load_cluster(one_file)
    assert set(vendor_region_map(pairs, counts, single).values()) == {"r1"}
    # the map is the assign_shards walk, no more and no less
    cell_region = dict(assign_shards(counts, cluster.region_ids))
    assert {v: cell_region[g] for v, g in pairs} == got


def test_resolver_covers_every_vendor_exactly_once(tmp_path: Path) -> None:
    """A synthetic multi-region vendor set: 5 cells x 100 vendors, 3
    regions. Every vendor lands in exactly one region, every region is
    used, and each vendor's region is the one its cell was assigned.
    The pairs are built in memory - the resolver never reads a file."""
    pairs = [(f"v{i:04d}", f"c{i % 5:04d}") for i in range(500)]
    counts = counts_by_cell(pairs)
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 3\nstandbys = false\n", encoding="utf-8")
    cluster = load_cluster(cluster_file)

    got = vendor_region_map(pairs, counts, cluster)
    assert len(got) == 500  # every vendor, once
    assert set(got.values()) == set(cluster.region_ids)  # no empty region

    cell_region = dict(assign_shards(counts, cluster.region_ids))
    assert {v: cell_region[g] for v, g in pairs} == got

    per_region = [sum(1 for r in got.values() if r == rid) for rid in cluster.region_ids]
    # hand-checked greedy cut: shares at 500/3 and 1000/3, so r1 takes
    # c0000+c0001 (200), r2 takes c0002+c0003 (200), r3 takes c0004 (100)
    assert per_region == [200, 200, 100]


# --- the region filter over the cache ---------------------------------------


def test_region_filter_keeps_replay_order_and_count_is_the_share(
    tmp_path: Path,
) -> None:
    data = make_region_data_dir(tmp_path)
    build_cache(data, warm_up_days=14, quiet=True)
    cache = data / "cached" / "orders-sorted.jsonl"
    pairs = restock_vendors(data)
    vendor_region = vendor_region_map(
        pairs, counts_by_cell(pairs), load_cluster(two_region_cluster_file(tmp_path))
    )

    def r1(order: dict) -> bool:
        return vendor_region.get(order["vendor_id"]) == "r1"

    kept = [order["order_id"] for order in iter_orders(cache, where=r1)]
    assert kept == R1_ORDERS  # replay order preserved, r2 orders dropped
    assert count_orders(cache, where=r1) == len(R1_ORDERS)
    assert count_orders(cache) == len(ALL_ORDERS)

    # the window and the filter compose: only r1's orders, only the days
    assert [o["order_id"] for o in iter_orders(cache, from_day=15, where=r1)] == ["5"]
    assert [o["order_id"] for o in iter_orders(cache, to_day=14, where=r1)] == ["1", "3"]

    # the one-pass share counts: whole cache vs the run window
    share = region_order_counts(cache, vendor_region, "r1", from_day=15, to_day=16)
    assert share.region_orders == 3
    assert share.window_orders == 1  # only order 5 is in days 15-16
    assert share.unknown_orders == 0


def test_orders_with_a_vendor_outside_the_map_are_counted_not_dropped(
    tmp_path: Path,
) -> None:
    data = make_region_data_dir(tmp_path, rogue=True)
    build_cache(data, warm_up_days=14, quiet=True)
    pairs = restock_vendors(data)
    share = region_order_counts(
        data / "cached" / "orders-sorted.jsonl",
        vendor_region_map(
            pairs, counts_by_cell(pairs), load_cluster(two_region_cluster_file(tmp_path))
        ),
        "r1",
        from_day=14,
        to_day=16,
    )
    assert share.unknown_orders == 1
    assert share.unknown_example == "vX"


# --- one at a time -----------------------------------------------------------


class GatedTransport(httpx.AsyncBaseTransport):
    """Answers 201, but each request blocks until the test releases it;
    the log of starts and ends is the evidence of (de)serialization."""

    def __init__(self) -> None:
        self.gates: list[asyncio.Event] = []
        self.log: list[tuple[str, int]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        gate = asyncio.Event()
        index = len(self.gates)
        self.gates.append(gate)
        self.log.append(("start", index))
        await gate.wait()
        self.log.append(("end", index))
        return httpx.Response(201, json={"outcome": "accepted"})


async def test_inflight_one_waits_for_the_ack_before_the_next_send(
    tmp_path: Path,
) -> None:
    """inflight=1 serializes through the engine's FIFO semaphore; only a
    test that holds a response back can prove the next request starts
    strictly after the previous one was answered."""
    transport = GatedTransport()
    orders = [make_order(14, 0, 0, 0, str(1_000 + i)) for i in range(4)]  # all due now
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=1, speed=1e9, transport=transport)
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="serial")
    await settle()
    # exactly one request is out; the other three are queued for the slot
    assert transport.log == [("start", 0)]
    await settle()  # and it stays that way: nothing else may sneak out
    assert transport.log == [("start", 0)]
    for index in range(len(orders)):
        transport.gates[index].set()
        await settle()
        expected = [event for i in range(index + 1) for event in (("start", i), ("end", i))]
        if index < len(orders) - 1:
            expected.append(("start", index + 1))  # the next send, only now
        assert transport.log == expected
    await drive(pb, clock)
    assert pb.status()["state"] == "done"
    log = (tmp_path / "serial" / "orders.jsonl").read_text().splitlines()
    assert [json.loads(line)["seq"] for line in log] == [0, 1, 2, 3]


async def test_sequential_run_sends_every_order_despite_a_tiny_drain_timeout(
    tmp_path: Path,
) -> None:
    """The drain timeout must not be able to cancel a sequential run's
    tail. The target is slow (20 ms per request), the drain timeout is a
    millisecond, and six orders are all due at once: inflight=1 alone
    would queue five orders behind the semaphore and let the drain
    cancel them at the end of the stream; sequential mode has the
    dispatch loop await each send, so every order is sent and answered
    before the run can even end."""
    orders = [make_order(14, 0, 0, 0, str(1_000 + i)) for i in range(6)]
    transport = httpx.ASGITransport(app=make_stub_app(delay_seconds=0.02))
    pb = make_playback(
        orders,
        tmp_path,
        inflight=1,
        sequential=True,
        speed=1e9,
        drain_timeout_s=0.001,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="tail")
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["orders_acked"] == 6
    lines = [
        json.loads(line) for line in (tmp_path / "tail" / "orders.jsonl").read_text().splitlines()
    ]
    assert len(lines) == 6  # every order got its line
    assert all(line["http_status"] == 201 for line in lines)
    assert not any("error" in line for line in lines)  # nothing cancelled


async def test_stop_during_a_sequential_run_is_bounded(tmp_path: Path) -> None:
    """A stop lands between orders, bounded by the one in-flight send,
    not after the whole stream: with the dispatch loop awaiting each
    send, only the current order is ever at risk, and it completes."""
    orders = [make_order(14, 0, 0, 0, str(1_000 + i)) for i in range(6)]
    transport = httpx.ASGITransport(app=make_stub_app(delay_seconds=0.05))
    pb = make_playback(
        orders,
        tmp_path,
        inflight=1,
        sequential=True,
        speed=1e9,
        drain_timeout_s=30.0,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="stoppable")
    await settle()  # the first send is in flight; the rest are not dispatched
    started = time.monotonic()
    await pb.stop()
    elapsed = time.monotonic() - started
    assert pb.status()["state"] == "stopped"
    # bounded by the in-flight send (0.05 s), not the drain timeout (30 s)
    assert elapsed < 5
    assert 1 <= pb.status()["orders_sent"] < 6
    lines = [
        json.loads(line)
        for line in (tmp_path / "stoppable" / "orders.jsonl").read_text().splitlines()
    ]
    assert len(lines) == pb.status()["orders_sent"]
    assert all(line["http_status"] == 201 for line in lines)
    assert not any("error" in line for line in lines)


# --- the edge target URL ------------------------------------------------------


def test_edge_target_url_follows_the_cluster(tmp_path: Path) -> None:
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 3\nstandbys = false\n", encoding="utf-8")
    cluster = load_cluster(cluster_file)
    assert edge_target_url(cluster, "r1") == "http://127.0.0.1:8011/api/v1"
    assert edge_target_url(cluster, "r3") == "http://127.0.0.1:8013/api/v1"
    with pytest.raises(KeyError, match="no region 'r4'"):
        edge_target_url(cluster, "r4")


# --- the CLI surface ----------------------------------------------------------


@pytest.fixture
def region_env(tmp_path: Path, monkeypatch):
    """Factory for a hermetic two-region data dir with its cache built,
    env pointed at it; `rogue=True` swaps one order's vendor for one
    that is not in vendors.csv."""
    built: list[Path] = []

    def make(*, rogue: bool = False) -> Path:
        data = make_region_data_dir(tmp_path, rogue=rogue)
        built.append(data)
        build_cache(data, warm_up_days=14, quiet=True)
        monkeypatch.setenv("ROS_DATA_DIR", str(data))
        monkeypatch.setenv("ROS_CLUSTER_FILE", str(two_region_cluster_file(tmp_path)))
        monkeypatch.chdir(tmp_path)
        get_settings.cache_clear()
        return data

    yield make
    get_settings.cache_clear()


def test_start_refuses_a_region_the_cluster_does_not_have(region_env, capsys) -> None:
    region_env()
    rc = app(["playback", "start", "--region", "r9", "--port", "1"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no region 'r9'" in err
    assert "this cluster has r1, r2" in err


def test_start_target_edge_needs_a_region(region_env, capsys) -> None:
    region_env()
    rc = app(["playback", "start", "--target", "edge", "--port", "1"])
    assert rc == 1
    assert "--target edge needs --region" in capsys.readouterr().err


def test_start_sequential_refuses_a_wider_inflight(region_env, capsys) -> None:
    region_env()
    rc = app(["playback", "start", "--sequential", "--inflight", "4", "--port", "1"])
    assert rc == 1
    assert "one order at a time" in capsys.readouterr().err


def test_start_reports_a_missing_cluster_file_not_a_traceback(region_env, capsys) -> None:
    """Review round 4, item 2: `load_cluster` runs at the top of the
    start, outside the big handler, so a missing cluster file used to be
    a traceback. Now it is one line and rc 1, like every other refusal."""
    region_env()
    missing = Path("no-such-cluster.toml")
    rc = app(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--config",
            str(missing),
            "--port",
            "1",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert err.startswith("rosctl playback start: ")
    assert "no-such-cluster.toml" in err
    assert "Traceback" not in err


def test_target_edge_without_region_exits_before_the_cache_is_built(
    tmp_path, monkeypatch, capsys
) -> None:
    """Review round 4, item 2: the flag checks run before `ensure_cache`,
    so a bad combination must not pay for a cache first. Proved with a
    data dir that has no cache: the refusal leaves no `cached/` behind."""
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setenv("ROS_DATA_DIR", str(data))
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()
    try:
        rc = app(["playback", "start", "--target", "edge", "--port", "1"])
    finally:
        get_settings.cache_clear()
    assert rc == 1
    assert "--target edge needs --region" in capsys.readouterr().err
    assert not (data / "cached").exists()


class RecordingTransport(httpx.AsyncBaseTransport):
    """Answers 201 and records every request, so the CLI tests assert
    what the run actually sent: the URL, the body, the order, and the
    overlap. `sleep(0)` gives the event loop a turn, so an overlapping
    request would show up in `max_active`."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.active = 0
        self.max_active = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0)
            return httpx.Response(201, json={"outcome": "accepted"})
        finally:
            self.active -= 1


async def test_region_run_through_the_cli(region_env, capsys) -> None:
    """The whole mode through the public CLI surface, with the transport
    injected: every request went to the derived edge URL, the bodies are
    exactly r1's orders in replay order, no two requests overlapped, and
    the run's meta records the mode."""
    region_env()
    args = build_parser().parse_args(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--target",
            "edge",
            "--sequential",
            "--speed",
            "1000000",
            "--port",
            str(free_port()),
        ]
    )
    transport = RecordingTransport()
    rc = await playback_cli.start_command(args, transport=transport)
    assert rc == 0
    err = capsys.readouterr().err
    assert "region r1: 3 of 6 cached orders (3 in days 14-16)" in err

    # what was sent: the derived edge URL, r1's orders, in replay order
    assert {str(r.url) for r in transport.requests} == {"http://127.0.0.1:8011/api/v1/orders"}
    bodies = [json.loads(r.content) for r in transport.requests]
    assert [body["external_order_id"] for body in bodies] == R1_ORDERS
    assert all(body["vendor_id"] == "vA" for body in bodies)
    assert transport.max_active == 1  # sequential: no two requests overlap

    run_dir = next((Path("runs")).iterdir())
    meta = json.loads((run_dir / "meta.json").read_text())
    assert meta["region"] == "r1"
    assert meta["region_cached_orders"] == 3
    assert meta["region_window_orders"] == 3
    assert meta["sequential"] is True
    assert meta["inflight"] == 1
    assert meta["target_url"] == "http://127.0.0.1:8011/api/v1/orders"


async def test_region_without_edge_target_still_filters(region_env) -> None:
    """--region is a filter, not only half of the single-region mode: with
    an explicit target it must send the same filtered set."""
    region_env()
    args = build_parser().parse_args(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--target",
            "http://127.0.0.1:1/api/v1",
            "--speed",
            "1000000",
            "--port",
            str(free_port()),
        ]
    )
    transport = RecordingTransport()
    rc = await playback_cli.start_command(args, transport=transport)
    assert rc == 0
    assert {str(r.url) for r in transport.requests} == {"http://127.0.0.1:1/api/v1/orders"}
    assert [json.loads(r.content)["external_order_id"] for r in transport.requests] == R1_ORDERS


async def test_start_exits_1_when_no_order_got_an_answer(region_env, capsys) -> None:
    """A run that sent orders and never got one API answer is a failure,
    not a success: connection refused everywhere must not exit 0."""

    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    region_env()
    args = build_parser().parse_args(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--target",
            "http://127.0.0.1:1/api/v1",
            "--speed",
            "1000000",
            "--port",
            str(free_port()),
        ]
    )
    rc = await playback_cli.start_command(args, transport=httpx.MockTransport(unreachable))
    assert rc == 1
    err = capsys.readouterr().err
    assert "no order was acked, parked or refused" in err
    assert "http://127.0.0.1:1/api/v1" in err
    assert "orders.jsonl" in err


async def test_start_exits_1_when_every_answer_is_a_404(region_env, capsys) -> None:
    """A 404 IS an answer - exactly the case on the current edge API,
    which 404s POST /api/v1/orders until WP2 registers its routes. The
    run must still exit 1: no order was acked, parked or refused."""

    def not_found(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "not_found"})

    region_env()
    args = build_parser().parse_args(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--target",
            "http://127.0.0.1:8011/api/v1",
            "--restock-days",
            "99999",
            "--speed",
            "1000000",
            "--port",
            str(free_port()),
        ]
    )
    rc = await playback_cli.start_command(args, transport=httpx.MockTransport(not_found))
    assert rc == 1
    err = capsys.readouterr().err
    assert "no order was acked, parked or refused" in err
    run_dir = next((Path("runs")).iterdir())
    lines = [json.loads(line) for line in (run_dir / "orders.jsonl").read_text().splitlines()]
    order_lines = [line for line in lines if line.get("kind") == "order"]
    assert order_lines and all(line["http_status"] == 404 for line in order_lines)


def test_edge_mode_app_serves_the_orders_route_playback_targets(monkeypatch) -> None:
    """Playback's --target edge POSTs to /api/v1/orders on the region's
    edge app, and edge mode registers that route (PR #18). This pins the
    contract from playback's side. The route is resolved through the
    app's OpenAPI spec, not `app.routes`: FastAPI includes sub-routers
    lazily, so at creation time `app.routes` holds only an
    `_IncludedRouter` marker and the route is not visible there."""
    from restaurantos.common.config import Settings
    from restaurantos.gateway.app import create_app

    edge_settings = Settings(mode="edge", region_id="r1", network="host")
    monkeypatch.setattr("restaurantos.gateway.app.get_settings", lambda: edge_settings)
    app = create_app()
    orders = app.openapi()["paths"].get("/api/v1/orders", {})
    assert "post" in orders


async def test_control_start_racing_the_cli_start_loses_cleanly(
    region_env, monkeypatch, capsys, tmp_path
) -> None:
    """Round 3's race: a POST /sim/playback start that lands between the
    control server binding and the CLI's own start. Simulated by a
    startup hook on the control app that starts a run before uvicorn
    reports started. The CLI must bow out with rc 1 and a clear message,
    and the control-started run must end stopped with its log closed."""
    from restaurantos.sim import control

    region_env()
    real_make_control_app = control.make_control_app
    captured: dict = {}

    def racing_make_control_app(playback):
        app = real_make_control_app(playback)
        captured["playback"] = playback

        async def start_before_the_cli_does() -> None:
            # a control-API start with its own run id; paused so it
            # is still live when the CLI's own start fires
            await playback.start(
                seed=0, from_day=14, to_day=14, speed=1e9, run_id="control-start-race"
            )
            await playback.pause()

        app.router.on_startup.append(start_before_the_cli_does)
        return app

    monkeypatch.setattr(control, "make_control_app", racing_make_control_app)
    args = build_parser().parse_args(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--target",
            "edge",
            "--sequential",
            "--speed",
            "1000000",
            "--port",
            str(free_port()),
        ]
    )
    rc = await playback_cli.start_command(args, transport=RecordingTransport())
    assert rc == 1
    err = capsys.readouterr().err
    assert "a run was started through the control API before this one" in err

    # the control-started run: one run dir, no CLI-only extras (that is
    # the point of the race), ended stopped with its log on disk and
    # closed. The engine-wide facts travel with the engine (round 4,
    # item 4), so this run carries the mode too - just not `argv`.
    run_dirs = list((Path("runs")).iterdir())
    assert len(run_dirs) == 1
    meta = json.loads((run_dirs[0] / "meta.json").read_text())
    assert "argv" not in meta
    assert "region_window_orders" not in meta
    assert meta["region"] == "r1"
    assert meta["sequential"] is True
    status = captured["playback"].status()
    assert status["state"] == "stopped"
    # paused before any dispatch: an empty log, created and closed
    log_file = run_dirs[0] / "orders.jsonl"
    assert log_file.exists()
    lines = log_file.read_text().splitlines()
    assert all(json.loads(line) for line in lines)


async def test_a_control_api_start_keeps_the_runs_mode_in_meta(
    region_env, monkeypatch, capsys, tmp_path
) -> None:
    """Review round 4, item 4: a run started through the control API's
    start action on a region engine sends with the same filter, target
    and sequential flag, so its meta.json must carry the same mode:
    `region`, `sequential`, `region_cached_orders` - engine-wide, via
    `run_meta` - while `argv` and the first run's `region_window_orders`
    stay CLI-only and must not appear."""
    from restaurantos.sim import control

    region_env()
    real_make_control_app = control.make_control_app
    captured: dict = {}

    def capturing_make_control_app(playback):
        captured["playback"] = playback
        return real_make_control_app(playback)

    monkeypatch.setattr(control, "make_control_app", capturing_make_control_app)
    args = build_parser().parse_args(
        [
            "playback",
            "start",
            "--region",
            "r1",
            "--target",
            "edge",
            "--sequential",
            "--speed",
            "1000000",
            "--port",
            str(free_port()),
        ]
    )
    rc = await playback_cli.start_command(args, transport=RecordingTransport())
    assert rc == 0

    first_dir = next((Path("runs")).iterdir())
    first = json.loads((first_dir / "meta.json").read_text())
    # the CLI run: engine-wide facts and the CLI-only extras both present
    assert first["region"] == "r1"
    assert first["sequential"] is True
    assert first["region_cached_orders"] == 3
    assert first["region_window_orders"] == 3
    assert first["argv"]

    # the second run, started through the control app's start action on
    # the same engine; the app is driven in this loop, so the run's
    # tasks live on it (no portal thread that stops mid-run)
    playback = captured["playback"]
    control_app = real_make_control_app(playback)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=control_app), base_url="http://control"
    ) as client:
        response = await client.post(
            "/sim/playback",
            json={
                "action": "start",
                "from_day": 14,
                "to_day": 14,
                "seed": 0,
                "run_id": "control-start-mode-meta",
            },
        )
    assert response.status_code == 200
    await playback.wait_done()

    second = json.loads((Path("runs") / "control-start-mode-meta" / "meta.json").read_text())
    assert second["region"] == "r1"
    assert second["sequential"] is True
    assert second["region_cached_orders"] == 3
    assert second["cache"]["cached_orders"] == 6
    assert second["cluster"]["config"]
    # CLI-only facts describe the first run, not the engine
    assert "argv" not in second
    assert "region_window_orders" not in second


async def test_orders_with_an_unknown_vendor_refuse_the_run(region_env, capsys) -> None:
    """An order whose vendor is not in vendors.csv belongs to no region
    and would be silently dropped from every run; the run refuses."""
    region_env(rogue=True)
    args = build_parser().parse_args(
        ["playback", "start", "--region", "r1", "--port", str(free_port())]
    )
    rc = await playback_cli.start_command(args)
    assert rc == 1
    err = capsys.readouterr().err
    assert "vendor outside the vendor-to-region map" in err
    assert "1 cached orders" in err
    assert "'vX'" in err

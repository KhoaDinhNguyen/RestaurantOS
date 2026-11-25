"""What `Playback.status()` reports for a snapshot: the dispatch frontier
(the last dispatched item's own instant, which the wall-driven clock can
run ahead of) and the engine facts every run on the engine shares."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
from conftest import FakeWallClock, drive, make_order, make_playback

from restaurantos.sim.playback import Playback


def _accepting(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content)
    return httpx.Response(
        201, json={"order_id": f"u-{body['external_order_id']}", "region_id": "r1"}
    )


async def test_the_frontier_is_none_before_the_first_dispatch(tmp_path: Path) -> None:
    pb = make_playback(
        [make_order(14, 9, 0, 0, "1")], tmp_path, transport=httpx.MockTransport(_accepting)
    )
    status = pb.status()
    assert status["dispatched_sim_day"] is None and status["dispatched_sim_time"] is None


async def test_the_frontier_is_the_last_dispatched_instant(tmp_path: Path) -> None:
    clock = FakeWallClock()
    orders = [make_order(14, 9, 0, 0, "1"), make_order(14, 10, 30, 5, "2")]
    pb = make_playback(orders, tmp_path, clock=clock, transport=httpx.MockTransport(_accepting))
    await pb.start(seed=0, from_day=14, to_day=14, speed=100000.0, run_id="frontier")
    await drive(pb, clock)
    status = pb.status()
    assert (status["dispatched_sim_day"], status["dispatched_sim_time"]) == (14, "10:30:05")


async def test_a_new_run_resets_the_frontier(tmp_path: Path) -> None:
    clock = FakeWallClock()
    pb = make_playback(
        [make_order(14, 9, 0, 0, "1")],
        tmp_path,
        clock=clock,
        transport=httpx.MockTransport(_accepting),
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100000.0, run_id="first")
    await drive(pb, clock)
    assert pb.status()["dispatched_sim_day"] == 14
    await pb.start(seed=0, from_day=14, to_day=14, speed=1.0, run_id="second")
    # started, nothing due yet at 1x: nothing dispatched in this run
    assert pb.status()["dispatched_sim_day"] is None
    await pb.stop()


async def test_the_engine_facts_match_meta_json(tmp_path: Path) -> None:
    clock = FakeWallClock()
    pb = Playback(
        lambda a, b: iter([make_order(14, 9, 0, 0, "1")]),
        target_url="http://127.0.0.1:8011/api/v1",
        inflight=1,
        http_timeout_s=5.0,
        drain_timeout_s=5.0,
        runs_dir=tmp_path,
        wall=clock,
        transport=httpx.MockTransport(_accepting),
        status_events=True,
        event_sample_rate=0.5,
        event_jitter_s=30.0,
        restock_every=7,
        kitchen_url="http://stub.test/api/v1",
        run_meta={"region": "r1", "sequential": True},
        sequential=True,
        duplicate_offset_s=2.0,
        shuffle_window=4,
    )
    engine = pb.status()["engine"]
    assert engine == {
        "sequential": True,
        "region": "r1",
        "target_url": "http://127.0.0.1:8011/api/v1/orders",
        "inflight": 1,
        "events": {"enabled": True, "sample_rate": 0.5, "jitter_s": 30.0},
        "restock_every": 7,
        "duplicate_offset_s": 2.0,
        "shuffle_window": 4,
        "kitchen": {"mode": "override", "url": "http://stub.test/api/v1"},
    }
    await pb.start(seed=0, from_day=14, to_day=14, speed=100000.0, run_id="facts")
    await drive(pb, clock)
    meta = json.loads((tmp_path / "facts" / "meta.json").read_text())
    assert meta["target_url"] == engine["target_url"]
    assert meta["kitchen"] == engine["kitchen"]
    assert meta["events"] == engine["events"]
    assert meta["region"] == engine["region"] and meta["sequential"] is True


async def test_status_reports_held_orders_and_the_next_due_item(tmp_path: Path) -> None:
    """Shuffle holds up to a window of orders that may be EARLIER than the
    frontier; a snapshot needs the earliest of them, and the earliest
    item still to go out of any kind."""
    pb = make_playback(
        [make_order(14, 9, 0, 0, "1")], tmp_path, transport=httpx.MockTransport(_accepting)
    )
    empty = pb.status()
    assert empty["held_sim_day"] is None and empty["next_due_sim_day"] is None
    pb._held = [
        (0.7, 0, make_order(15, 0, 10, 0, "b")),
        (0.2, 1, make_order(14, 23, 50, 0, "a")),
    ]
    pb._heap = [(15 * 86400 + 5, 0, 0, 0, "event", None)]
    status = pb.status()
    assert (status["held_sim_day"], status["held_sim_time"]) == (14, "23:50:00")
    assert (status["next_due_sim_day"], status["next_due_sim_time"]) == (14, "23:50:00")
    pb._held = [(0.7, 0, make_order(15, 0, 10, 0, "b"))]
    status = pb.status()
    assert (status["next_due_sim_day"], status["next_due_sim_time"]) == (15, "00:00:05")

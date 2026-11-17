"""The playback extras: status events and restock. Scheduling, the
outcome gate, sampling, the run log's new lines and counters, and the
determinism acceptance with events and jitter on.

Fake time (conftest.FakeWallClock) drives the scheduling tests; the
outcome-gate test uses the real clock against a stub that really
delays, because the point is what happens while an ack is outstanding.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
import pytest
from conftest import FakeWallClock, drive, make_order, make_playback, sample_orders, settle
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from restaurantos.sim.control import make_control_app
from restaurantos.sim.determinism import stable_float
from restaurantos.sim.playback import EVENT_SCHEDULE, Playback
from restaurantos.sim.stub import make_stub_app

KITCHEN = [status.value for status, _offset in EVENT_SCHEDULE]


def log_file(runs_dir: Path, pb: Playback) -> Path:
    status = pb.status()
    assert status["run_id"], f"no run yet (state {status['state']})"
    return runs_dir / status["run_id"] / "orders.jsonl"


def run_log_lines(runs_dir: Path, pb: Playback) -> list[dict]:
    return [json.loads(line) for line in log_file(runs_dir, pb).read_text().splitlines()]


def stub_order_id(external_order_id: str) -> str:
    """The order id the stub derives from a request (uuid5, fixed epoch)."""
    return str(uuid5(NAMESPACE_URL, f"dhrd:{external_order_id}"))


def expected_event_id(external_order_id: str, to_status: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"{stub_order_id(external_order_id)}:{to_status}"))


def expected_jitter(seed: int, external_order_id: str, index: int) -> float:
    return stable_float(seed, external_order_id, f"events-jitter-{index}") * 60.0


# --- the four kitchen events -------------------------------------------------


async def test_accepted_orders_get_their_four_events(tmp_path: Path) -> None:
    """accepted, preparing, ready, picked_up at +1, +3, +15, +25
    simulated minutes plus jitter, to the region's own route, with
    deterministic event ids."""
    orders = [make_order(14, 0, 0, 10, "1001"), make_order(14, 0, 0, 20, "1002")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, status_events=True)
    await pb.start(seed=42, from_day=14, to_day=14, speed=100.0, run_id="events")
    await drive(pb, clock)
    assert pb.status()["state"] == "done"

    lines = run_log_lines(tmp_path, pb)
    events = [line for line in lines if line["kind"] == "status_event"]
    assert len(events) == 2 * 4  # four per accepted order

    by_order: dict[str, list[dict]] = {}
    for line in events:
        by_order.setdefault(line["external_order_id"], []).append(line)
    for external_id, order_events in by_order.items():
        assert [line["to_status"] for line in order_events] == KITCHEN
        # at their scheduled simulated offsets, plus a bounded jitter
        order_line = next(
            line
            for line in lines
            if line["kind"] == "order" and line["external_order_id"] == external_id
        )
        for index, line in enumerate(order_events):
            jitter = expected_jitter(42, external_id, index)
            assert line["jitter_s"] == round(jitter, 3)
            assert 0.0 <= jitter < 60.0
            assert line["sim_sent_at"] == round(
                order_line["sim_sent_at"] + jitter + EVENT_SCHEDULE[index][1], 3
            )
            # the house style: deterministic event ids, the order's own
            # ids, the region's status route
            assert line["event_id"] == expected_event_id(external_id, line["to_status"])
            assert line["order_id"] == stub_order_id(external_id)
            assert line["http_status"] == 201
            assert line["applied"] is True
    status = pb.status()
    assert status["events_sent"] == 8
    assert status["events_acked"] == 8
    assert status["events_skipped"] == 0


async def test_events_join_the_log_in_send_order(tmp_path: Path) -> None:
    """The log stays send-ordered once events are interleaved: dense
    sequence numbers, and no event goes out before its simulated
    instant."""
    orders = [make_order(14, 0, 0, 10, "1001"), make_order(14, 0, 0, 20, "1002")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="interleaved")
    await drive(pb, clock)

    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    # every line's simulated instant is non-decreasing in send order
    instants = [line["sim_sent_at"] for line in lines]
    assert instants == sorted(instants)
    # the two orders went out first (10 s and 20 s), the earliest event
    # of the first order (70 s) sits between them and nothing else
    kinds = [line["kind"] for line in lines]
    assert kinds[0] == "order" and kinds[1] == "order"
    assert lines[1]["external_order_id"] == "1002"
    assert kinds[2] == "status_event"
    assert lines[2]["external_order_id"] == "1001"


async def test_order_lines_say_what_they_are(tmp_path: Path) -> None:
    """Order lines carry their kind from this PR on, so a reader can
    tell the three timelines apart without guessing."""
    orders = [make_order(14, 0, 0, 10, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="kind")
    await drive(pb, clock)
    lines = run_log_lines(tmp_path, pb)
    assert [line["kind"] for line in lines] == ["order"]


# --- the outcome gate --------------------------------------------------------


class OrderingApp:
    """Answers orders (with a real delay) and records every request."""

    def __init__(self, delay_s: float) -> None:
        self.app = make_stub_app()
        self.delay_s = delay_s
        self.paths: list[str] = []

    def transport(self) -> httpx.AsyncBaseTransport:
        outer = self

        class Transport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                outer.paths.append(request.url.path)
                if request.url.path.endswith("/orders"):
                    await asyncio.sleep(outer.delay_s)
                return await httpx.ASGITransport(app=outer.app).handle_async_request(request)

        return Transport()


async def test_an_event_waits_for_its_orders_outcome(tmp_path: Path) -> None:
    """The event needs the order id from the response, and a refused
    order must not move through the kitchen: no status POST may reach
    the wire before its order's answer did."""
    outer = OrderingApp(delay_s=0.05)
    orders = [make_order(14, 0, 0, 1, "1001"), make_order(14, 0, 0, 2, "1002")]
    pb = make_playback(
        orders, tmp_path, inflight=1, speed=1e9, status_events=True, transport=outer.transport()
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="gated")
    await pb.wait_done()
    assert pb.status()["state"] == "done"

    lines = run_log_lines(tmp_path, pb)
    order_acks = {
        line["external_order_id"]: line["wall_acked_at"]
        for line in lines
        if line["kind"] == "order"
    }
    events = [line for line in lines if line["kind"] == "status_event"]
    assert len(events) == 8
    for line in events:
        assert line["wall_sent_at"] >= order_acks[line["external_order_id"]]
    # every status POST on the wire followed its order's POST
    order_posts = [i for i, path in enumerate(outer.paths) if path.endswith("/orders")]
    event_posts = [i for i, path in enumerate(outer.paths) if path.endswith("/status")]
    assert len(order_posts) == 2 and len(event_posts) == 8
    assert min(event_posts) > max(order_posts)


async def test_refused_parked_and_failed_orders_skip_their_events(tmp_path: Path) -> None:
    """Only accepted orders move through the kitchen; every scheduled
    event of the others still gets its line, with the reason."""

    def answer_app() -> FastAPI:
        app = FastAPI()

        @app.post("/orders")
        @app.post("/api/v1/orders")
        async def place(request: dict) -> object:
            oid = request["external_order_id"]
            if oid.endswith("p"):
                return JSONResponse(status_code=202, content={"queued": True})
            if oid.endswith("r"):
                return JSONResponse(status_code=409, content={"outcome": "refused"})
            if oid.endswith("x"):
                return JSONResponse(status_code=500, content={"boom": True})
            return JSONResponse(
                status_code=201,
                content={
                    "outcome": "accepted",
                    "order_id": f"00000000-0000-4000-8000-{int(oid):012d}",
                },
            )

        @app.post("/vendors/{vendor_id}/orders/{order_id}/status")
        @app.post("/api/v1/vendors/{vendor_id}/orders/{order_id}/status")
        async def status_change(request: dict) -> object:
            return JSONResponse(status_code=201, content={"applied": True})

        return app

    orders = [
        make_order(14, 0, 0, 1, "1"),
        make_order(14, 0, 0, 2, "2p"),
        make_order(14, 0, 0, 3, "3r"),
        make_order(14, 0, 0, 4, "4x"),
    ]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        status_events=True,
        transport=httpx.ASGITransport(app=answer_app()),
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="skipped")
    await drive(pb, clock)

    status = pb.status()
    assert status["events_sent"] == 4  # only the accepted order's
    assert status["events_skipped"] == 12
    lines = run_log_lines(tmp_path, pb)
    skipped = [line for line in lines if line["kind"] == "status_event" and line.get("skipped")]
    reasons = {line["external_order_id"]: line["reason"] for line in skipped}
    assert reasons["2p"] == "parked"
    assert reasons["3r"] == "refused"
    assert reasons["4x"] == "failed"
    sent = [line for line in lines if line["kind"] == "status_event" and not line.get("skipped")]
    assert [line["external_order_id"] for line in sent] == ["1"] * 4
    # no skipped event carries an id: there is nothing to address it to
    assert all("event_id" not in line for line in skipped)


# --- the flag and the sample rate --------------------------------------------


async def test_events_off_and_rate_zero(tmp_path: Path) -> None:
    orders = sample_orders(6)
    variants: list[tuple[str, dict[str, Any]]] = [
        ("off", {"status_events": False}),
        ("rate0", {"status_events": True, "event_sample_rate": 0.0}),
    ]
    for run_id, kwargs in variants:
        clock = FakeWallClock()
        pb = make_playback(orders, tmp_path, clock=clock, **kwargs)
        await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id=run_id)
        await drive(pb, clock)
        lines = run_log_lines(tmp_path, pb)
        assert not [line for line in lines if line["kind"] == "status_event"], run_id
        assert pb.status()["events_sent"] == 0


async def test_the_sample_is_a_stable_per_order_decision(tmp_path: Path) -> None:
    """A rate below 1 keeps a deterministic subset of the orders; an
    order is in or out entirely, and the subset depends on the seed."""
    orders = sample_orders(30)

    async def run(seed: int, run_id: str) -> list[str]:
        clock = FakeWallClock()
        pb = make_playback(orders, tmp_path, clock=clock, status_events=True, event_sample_rate=0.5)
        await pb.start(seed=seed, from_day=14, to_day=14, speed=100.0, run_id=run_id)
        await drive(pb, clock)
        events = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
        sampled = {line["external_order_id"] for line in events}
        # in or out entirely: four events per sampled order
        assert all(
            sum(1 for line in events if line["external_order_id"] == oid) == 4 for oid in sampled
        )
        assert pb.status()["events_sent"] == 4 * len(sampled)
        return sorted(sampled)

    assert await run(seed=42, run_id="a1") == await run(seed=42, run_id="a2")
    subset = await run(seed=42, run_id="b")
    assert 0 < len(subset) < len(orders)  # a real subset for this seed
    other = await run(seed=7, run_id="c")
    assert other != subset  # the sample follows the seed


async def test_status_conflicts_are_counted_not_failed(tmp_path: Path) -> None:
    """A 409 from the status route is the architecture's conflict
    answer, not an error - a re-replayed window meets it on purpose."""

    class ConflictingStatus(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.inner = httpx.ASGITransport(app=make_stub_app())

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/status"):
                return httpx.Response(409, json={"applied": False, "conflict": True})
            return await self.inner.handle_async_request(request)

    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders, tmp_path, clock=clock, status_events=True, transport=ConflictingStatus()
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="conflict")
    await drive(pb, clock)
    status = pb.status()
    assert status["events_conflicted"] == 4
    assert status["events_failed"] == 0
    assert status["events_acked"] == 0
    events = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    assert [line["http_status"] for line in events] == [409] * 4
    assert all(line["conflict"] is True for line in events)


# --- restock ------------------------------------------------------------------


async def test_restock_goes_out_before_the_days_first_order(tmp_path: Path) -> None:
    """At 00:00:00 of each restock day, every vendor, ahead of that
    day's first order - even one placed at exactly midnight."""
    orders = [
        make_order(21, 0, 0, 0, "1001"),  # midnight sharp
        make_order(21, 0, 0, 10, "1002"),
        make_order(21, 0, 0, 20, "1003"),
    ]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        restock_every=7,  # W=14: restock days are 21, 28, ...
        warm_up_days=14,
        restock_vendors=("v002", "v001"),  # engine sends them in this order
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=100.0, run_id="restock")
    await drive(pb, clock)

    lines = run_log_lines(tmp_path, pb)
    assert [line["kind"] for line in lines[:2]] == ["restock", "restock"]
    assert [line["vendor_id"] for line in lines[:2]] == ["v002", "v001"]
    assert [line["restock_day"] for line in lines[:2]] == [21, 21]
    assert [line["sim_sent_at"] for line in lines[:2]] == [0.0, 0.0]
    assert [line["http_status"] for line in lines[:2]] == [201, 201]
    assert [line["kind"] for line in lines[2:]] == ["order"] * 3
    status = pb.status()
    assert status["restocks_sent"] == 2
    assert status["restocks_acked"] == 2


async def test_restock_days_follow_each_runs_window(tmp_path: Path) -> None:
    """The restock days are computed per run, from the run's own window:
    a second start on the same engine never sees the first run's days
    (CodeRabbit round 1, 2). A day before the new from_day would fire
    at once; a day after the new to_day would keep the run alive."""
    engine_orders = [make_order(21, 0, 0, 5, "1001"), make_order(28, 0, 0, 5, "1002")]
    pb = make_playback(
        engine_orders,
        tmp_path,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
    )

    await pb.start(seed=0, from_day=21, to_day=21, speed=1e9, run_id="day21")
    await pb.wait_done()
    days_21 = [
        line["restock_day"] for line in run_log_lines(tmp_path, pb) if line["kind"] == "restock"
    ]
    assert days_21 == [21]

    await pb.start(seed=0, from_day=28, to_day=28, speed=1e9, run_id="day28")
    await pb.wait_done()
    lines = run_log_lines(tmp_path, pb)
    restocks = [line for line in lines if line["kind"] == "restock"]
    assert [line["restock_day"] for line in restocks] == [28]
    # every restock of the second run starts inside its own window
    assert all(line["sim_sent_at"] >= 0 for line in restocks)
    # and the meta of the second run describes the second run
    meta = json.loads((tmp_path / "day28" / "meta.json").read_text())
    assert meta["restock"]["days"] == [28]


async def test_a_window_without_restock_days_sends_none(tmp_path: Path) -> None:
    """A window that ends before the first restock day (W+R) contains
    no restock, and the run ends with its last order, not with a wait
    for a restock that no order needs."""
    orders = [make_order(14, 0, 0, 5, "1001"), make_order(15, 0, 0, 5, "1002")]
    pb = make_playback(
        orders,
        tmp_path,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
    )
    await pb.start(seed=0, from_day=14, to_day=20, speed=1e9, run_id="early")
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["restocks_sent"] == 0
    assert all(line["kind"] == "order" for line in run_log_lines(tmp_path, pb))


async def test_restock_needs_a_to_day(tmp_path: Path) -> None:
    """Without to_day the window has no end to intersect with the
    interval days: restock refuses to guess (review item 1)."""
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, restock_every=7, warm_up_days=14)
    with pytest.raises(ValueError, match="restock needs to_day"):
        await pb.start(seed=0, from_day=14, to_day=None, speed=1e9, run_id="open")
    # and the refusal did not burn the run id
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="open")
    await drive(pb, clock)
    assert pb.status()["state"] == "done"


async def test_restock_on_the_first_day_goes_out_first(tmp_path: Path) -> None:
    """A restock day at the very start of the window fires at simulated
    zero, before the first order of the run (W=7 and R=7: day 14 is
    W+R)."""
    orders = [make_order(14, 0, 0, 1, "1001")]
    pb = make_playback(
        orders,
        tmp_path,
        restock_every=7,
        warm_up_days=7,
        restock_vendors=("v001",),
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="day14")
    await pb.wait_done()
    lines = run_log_lines(tmp_path, pb)
    assert lines[0]["kind"] == "restock"
    assert lines[0]["sim_sent_at"] == 0.0
    assert lines[1]["kind"] == "order"


# --- the acceptance line, with the extras on ---------------------------------


async def _extras_scenario(runs_dir: Path, orders, seed: int) -> str:
    """One full scenario with events, jitter and restock on: start,
    send a while, pause, change speed, resume, run to the end - all
    through the control routes."""
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        runs_dir,
        clock=clock,
        inflight=4,
        status_events=True,
        restock_every=7,
        warm_up_days=7,  # day 14 is W+R: the scenario restocks at start
        restock_vendors=("v001", "v002"),
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    )
    try:
        response = await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "to_day": 14, "speed": 100.0, "seed": seed},
        )
        assert response.status_code == 200, response.text

        await drive(pb, clock, steps=6)  # 30 simulated seconds out
        assert pb.status()["state"] == "running"

        await client.post("/sim/playback", json={"action": "pause"})
        await settle()
        await client.patch("/sim/playback", json={"speed": 2500.0})
        await client.post("/sim/playback", json={"action": "resume"})
        await drive(pb, clock)  # to the end, past the last event
    finally:
        await client.aclose()
    assert pb.status()["state"] == "done"
    # events and restock actually took part in this run
    status = pb.status()
    assert status["events_sent"] == 4 * len(orders)
    assert status["restocks_sent"] == 2
    return log_file(runs_dir, pb).read_text()


async def test_same_seed_gives_the_same_run_log_with_events(tmp_path: Path) -> None:
    """The determinism bar with the extras on: same seed, byte-identical
    log, including the mid-run speed change, with jitter, events and
    restock interleaved."""
    orders = sample_orders(20)
    log_a = await _extras_scenario(tmp_path / "a", orders, seed=42)
    log_b = await _extras_scenario(tmp_path / "b", orders, seed=42)
    assert log_a == log_b

    # the interleaving is what the schedule says, not what the acks did:
    # dense sequence numbers, non-decreasing simulated instants, and
    # every order's four events in kitchen order
    lines = [json.loads(line) for line in log_a.splitlines()]
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    instants = [line["sim_sent_at"] for line in lines]
    assert instants == sorted(instants)
    for external_id in {line["external_order_id"] for line in lines if line["kind"] == "order"}:
        statuses = [
            line["to_status"]
            for line in lines
            if line["kind"] == "status_event" and line["external_order_id"] == external_id
        ]
        assert statuses == KITCHEN


async def test_pause_parks_scheduled_events(tmp_path: Path) -> None:
    """Pause stops the whole merged timeline: orders and events alike
    wait for the resume."""
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="parked")
    await drive(pb, clock, steps=3)  # the order is out; its first event is not due yet
    await pb.pause()
    await drive(pb, clock, steps=30)  # fake time moves; simulated time must not
    assert pb.status()["events_sent"] == 0
    await pb.resume()
    await drive(pb, clock)
    assert pb.status()["state"] == "done"
    assert pb.status()["events_sent"] == 4


async def test_stop_leaves_a_dense_log_with_events(tmp_path: Path) -> None:
    """A stop mid-run with events scheduled: whatever was dispatched is
    logged densely, nothing half-written, and the run ends stopped."""
    orders = [make_order(14, 0, 0, 1, str(1_000 + i)) for i in range(10)]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=2, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="stopping")
    await drive(pb, clock, steps=4)  # a few orders out, no events yet
    await pb.stop()
    assert pb.status()["state"] == "stopped"
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    # only what was dispatched is in the file; it ends cleanly
    kinds = {line["kind"] for line in lines}
    assert kinds <= {"order", "status_event", "restock"}


async def test_meta_records_the_extras(tmp_path: Path) -> None:
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        status_events=True,
        event_sample_rate=0.5,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
    )
    await pb.start(seed=3, from_day=14, to_day=21, speed=100.0, run_id="meta")
    await pb.stop()
    meta = json.loads((tmp_path / "meta" / "meta.json").read_text())
    assert meta["events"] == {"enabled": True, "sample_rate": 0.5, "jitter_s": 60.0}
    assert meta["restock"] == {"days": [21], "vendors": 1}

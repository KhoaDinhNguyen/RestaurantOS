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
import time
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


class HoldingEvent(httpx.AsyncBaseTransport):
    """Holds the status event with the given event id until released,
    recording every event POST on the wire in order."""

    def __init__(self, hold: str) -> None:
        self.inner = httpx.ASGITransport(app=make_stub_app())
        self.release = asyncio.Event()
        self.hold = hold
        self.wire: list[tuple[str, str]] = []  # (to_status, event_id)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/status"):
            body = json.loads(request.content)
            self.wire.append((body["to_status"], body["event_id"]))
            if body["event_id"] == self.hold:
                await self.release.wait()
        return await self.inner.handle_async_request(request)


async def test_one_orders_events_go_out_strictly_one_after_another(tmp_path: Path) -> None:
    """CodeRabbit round 2: the four events of one order must not commit
    out of order on a slow target - the region's state machine is
    first-committed-change-wins, so preparing committing before accepted
    would make both conflict. Each event now waits, inside its own send
    task, for the previous event of the same order; a second order's
    events are not held up by the first order's slow event."""
    hold = expected_event_id("1001", "accepted")
    transport = HoldingEvent(hold)
    orders = [make_order(14, 0, 0, 1, "1001"), make_order(14, 0, 0, 2, "1002")]
    pb = make_playback(
        orders, tmp_path, inflight=8, speed=1e9, status_events=True, transport=transport
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="chained")
    # enough rounds for both orders' outcomes and the first due events:
    # with this seed 1002's accepted is due before 1001's, so the
    # dispatcher must get past it to reach the held one
    await settle(20)

    on_wire = {event_id for _status, event_id in transport.wire}
    # order 1001's accepted is on the wire, held; nothing later of
    # 1001 may have reached the wire behind it
    assert expected_event_id("1001", "accepted") in on_wire
    assert expected_event_id("1001", "preparing") not in on_wire
    assert expected_event_id("1001", "ready") not in on_wire
    # and order 1002's first event was not held up by 1001's slow one
    assert expected_event_id("1002", "accepted") in on_wire

    transport.release.set()
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["events_acked"] == 8
    # on the wire, each order's events follow the kitchen order
    for external_id in ("1001", "1002"):
        mine = [
            status
            for status, event_id in transport.wire
            if event_id == expected_event_id(external_id, status)
        ]
        assert mine == KITCHEN, external_id


async def test_stop_while_an_event_waits_for_its_predecessor(tmp_path: Path) -> None:
    """A stop while a chained event sits behind its held predecessor:
    the run ends stopped, the waiting task is cancelled by the drain and
    still writes its line, and the log stays dense - every dispatched
    event has exactly one line."""
    hold = expected_event_id("1001", "accepted")
    transport = HoldingEvent(hold)
    orders = [make_order(14, 0, 0, 1, "1001"), make_order(14, 0, 0, 2, "1002")]
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        status_events=True,
        drain_timeout_s=0.3,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="chained-stop")
    await settle()

    before = time.monotonic()
    await pb.stop()
    assert pb.status()["state"] == "stopped"
    assert time.monotonic() - before < 5.0

    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    event_lines = [line for line in lines if line["kind"] == "status_event"]
    # one line per dispatched event, sent or cancelled
    assert len(event_lines) == pb.status()["events_sent"]
    assert all(
        line.get("error") == "cancelled" or line.get("http_status") == 201 for line in event_lines
    )


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


async def test_the_engine_refuses_jitter_and_rate_out_of_range(tmp_path: Path) -> None:
    """Review item 5: tests and API callers build the engine directly,
    so the jitter bound and the sample rate validate in the constructor
    too, not only in the settings."""
    orders = sample_orders(2)
    with pytest.raises(ValueError, match="event_jitter_s"):
        make_playback(orders, tmp_path, status_events=True, event_jitter_s=121)
    with pytest.raises(ValueError, match="event_sample_rate"):
        make_playback(orders, tmp_path, status_events=True, event_sample_rate=-0.1)
    # the bounds are the exact limits, and they still build
    make_playback(orders, tmp_path, status_events=True, event_jitter_s=120.0)


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


async def test_a_replayed_event_counts_as_a_duplicate_not_a_conflict(tmp_path: Path) -> None:
    """Review item 7: a replayed event id is answered with
    duplicate: true (usually 2xx), not 409 - 409 is a real state-machine
    rejection. The counter says which is which."""

    class DuplicateStatus(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.inner = httpx.ASGITransport(app=make_stub_app())

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/status"):
                return httpx.Response(200, json={"applied": False, "duplicate": True})
            return await self.inner.handle_async_request(request)

    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders, tmp_path, clock=clock, status_events=True, transport=DuplicateStatus()
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="replay")
    await drive(pb, clock)
    status = pb.status()
    assert status["events_duplicate"] == 4
    assert status["events_conflicted"] == 0
    assert status["events_acked"] == 0
    events = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    assert all(line["http_status"] == 200 and line["duplicate"] is True for line in events)


async def test_conflicts_are_counted_not_failed(tmp_path: Path) -> None:
    """A 409 from the status route is the architecture's conflict
    answer - a state-machine rejection - not an error."""

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
    assert [line["http_status"] for line in lines[:2]] == [200, 200]
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


class HoldingRestocks(httpx.AsyncBaseTransport):
    """Holds every restock answer until the test releases it, so the
    barrier is observable: nothing else of that day may go out."""

    def __init__(self) -> None:
        self.inner = httpx.ASGITransport(app=make_stub_app())
        self.release = asyncio.Event()
        self.paths: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.url.path.endswith("/restock"):
            await self.release.wait()
        return await self.inner.handle_async_request(request)


async def test_restocks_are_acknowledged_before_the_days_first_order(tmp_path: Path) -> None:
    """Review item 2: dispatch order put the restocks first, but the
    semaphore only orders their sends - up to inflight-1 restocks could
    still be in flight when the day's first orders went out, and against
    a real region the refused-vs-accepted outcome would depend on
    timing. The barrier waits for the whole batch's answers first."""
    orders = [make_order(21, 0, 0, 5, "1001"), make_order(21, 0, 0, 10, "1002")]
    transport = HoldingRestocks()
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=1e9, run_id="barrier")
    await settle()
    # both restocks are dispatched and held; no order has gone out
    assert pb.status()["restocks_sent"] == 2
    assert pb.status()["orders_sent"] == 0

    transport.release.set()
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    lines = run_log_lines(tmp_path, pb)
    restocks = [line for line in lines if line["kind"] == "restock"]
    day_orders = [line for line in lines if line["kind"] == "order"]
    # the day's orders went out after every restock was answered
    last_restock_ack = max(line["wall_acked_at"] for line in restocks)
    assert all(line["wall_sent_at"] >= last_restock_ack for line in day_orders)


async def test_pause_during_the_restock_barrier_parks_the_days_first_order(tmp_path: Path) -> None:
    """Review round 2, item 1: a pause that arrives while the dispatcher
    sits in the restock barrier is honored before the next order goes
    out, not swallowed by the gate. After the resume the order is sent
    and the log stays dense."""
    orders = [make_order(21, 0, 0, 5, "1001")]
    transport = HoldingRestocks()
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=1e9, run_id="pause-gate")
    await settle()
    # the dispatcher sits in the barrier, both restocks held
    assert pb.status()["restocks_sent"] == 2
    assert pb.status()["orders_sent"] == 0

    await pb.pause()
    transport.release.set()
    await settle(10)
    # the restocks are answered now, but the order stays parked
    assert pb.status()["state"] == "paused"
    assert pb.status()["orders_sent"] == 0

    await pb.resume()
    await pb.wait_done()
    status = pb.status()
    assert status["state"] == "done"
    assert status["orders_sent"] == 1
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    assert [line["kind"] for line in lines] == ["restock", "restock", "order"]


async def test_stop_during_the_restock_barrier_ends_stopped(tmp_path: Path) -> None:
    """A stop while the barrier waits for held restocks ends the run
    promptly: the barrier gives way to the stop, the held restocks are
    cancelled by the drain (one line each), and the order behind the
    barrier was never dispatched, so it has no line."""
    orders = [make_order(21, 0, 0, 5, "1001")]
    transport = HoldingRestocks()  # never released
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        drain_timeout_s=0.3,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=1e9, run_id="held-barrier")
    await settle()
    assert pb.status()["orders_sent"] == 0  # the barrier holds the day

    before = time.monotonic()
    await pb.stop()
    assert pb.status()["state"] == "stopped"
    assert time.monotonic() - before < 5.0, "stop sat behind the held restocks"
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    assert [line["kind"] for line in lines] == ["restock", "restock"]
    assert all(line["error"] == "cancelled" for line in lines)


class HoldingOrders(httpx.AsyncBaseTransport):
    """Holds every order answer until released, so the wait before a
    restock batch is observable: the restocks must not go out while an
    order from before midnight is still in flight."""

    def __init__(self) -> None:
        self.inner = httpx.ASGITransport(app=make_stub_app())
        self.release = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/orders"):
            await self.release.wait()
        return await self.inner.handle_async_request(request)


async def test_restocks_wait_for_the_sends_from_before_midnight(tmp_path: Path) -> None:
    """Review round 2, item 2: the round-1 barrier protects the orders
    after a restock, not the orders before it. An order sent at 23:59:58
    and still in flight at 00:00:00 races the restock at the region:
    "order, then restock" ends the cycle at par, "restock, then order"
    at par minus the order. The restock now waits for that order's
    answer, so the end-of-cycle stock never depends on timing."""
    orders = [make_order(20, 23, 59, 58, "1001"), make_order(21, 0, 0, 5, "1002")]
    transport = HoldingOrders()
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=20, to_day=21, speed=1e9, run_id="pre-midnight")
    await settle(20)
    # the order from before midnight is dispatched and held; the restocks
    # have not gone out
    assert pb.status()["orders_sent"] == 1
    assert pb.status()["restocks_sent"] == 0

    transport.release.set()
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    lines = run_log_lines(tmp_path, pb)
    order_line = next(
        line for line in lines if line["kind"] == "order" and line["external_order_id"] == "1001"
    )
    restocks = [line for line in lines if line["kind"] == "restock"]
    # every restock went out after the earlier order was answered
    assert all(line["wall_sent_at"] >= order_line["wall_acked_at"] for line in restocks)
    # and the day's own orders still follow the round-1 barrier
    day_order = next(
        line for line in lines if line["kind"] == "order" and line["external_order_id"] == "1002"
    )
    assert all(day_order["wall_sent_at"] >= line["wall_acked_at"] for line in restocks)


async def test_stop_during_the_pre_restock_wait_ends_stopped(tmp_path: Path) -> None:
    """A stop while the dispatcher waits for an in-flight order before
    the restock batch ends the run as stopped, with a dense log: the
    held order is cancelled by the drain (one line), and the restock
    behind the wait was never dispatched, so it has no line."""
    orders = [make_order(20, 23, 59, 58, "1001")]
    transport = HoldingOrders()  # never released
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        drain_timeout_s=0.3,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=20, to_day=21, speed=1e9, run_id="held-pre-restock")
    await settle(20)
    assert pb.status()["restocks_sent"] == 0

    before = time.monotonic()
    await asyncio.wait_for(pb.stop(), 5)
    assert pb.status()["state"] == "stopped"
    assert time.monotonic() - before < 5.0, "stop sat behind the held order"
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    assert [line["kind"] for line in lines] == ["order"]
    assert lines[0]["error"] == "cancelled"


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


async def test_stop_ends_the_wait_for_an_unanswered_order(tmp_path: Path) -> None:
    """CodeRabbit round 1, 3: `await handle.future` did not watch for
    stop, so a target that never answered held the dispatcher (and
    `stop`, and the CLI's stop timeout) until the HTTP timeout. The
    outcome wait now ends on stop; the event that was never dispatched
    gets no line, and stop returns within the drain timeout."""

    class NeverAnswers(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.inner = httpx.ASGITransport(app=make_stub_app())
            self.release = asyncio.Event()

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/orders"):
                await self.release.wait()  # never, within the test
            return await self.inner.handle_async_request(request)

    transport = NeverAnswers()
    orders = [make_order(14, 0, 0, 1, "1001"), make_order(14, 0, 0, 2, "1002")]
    pb = make_playback(
        orders,
        tmp_path,
        inflight=1,
        speed=1e9,
        status_events=True,
        drain_timeout_s=0.3,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="held")
    await settle()
    # order 1 is in flight forever, order 2 sits queued for the only
    # slot, and the dispatcher waits on order 1's outcome for its event
    assert pb.status()["inflight"] == 2

    before = time.monotonic()
    await pb.stop()
    waited = time.monotonic() - before
    assert pb.status()["state"] == "stopped"
    # one drain window for the never-answered order, not the HTTP
    # timeout on top of it
    assert waited < 5.0, f"stop waited {waited:.1f}s behind an unanswered target"
    lines = run_log_lines(tmp_path, pb)
    # dense, and only the two dispatched orders: their events were
    # never dispatched, so they have no lines
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    assert [line["kind"] for line in lines] == ["order", "order"]
    assert all(line["error"] == "cancelled" for line in lines)


async def test_a_normal_end_waits_for_every_send(tmp_path: Path) -> None:
    """Review round 2, item 3: when the dispatcher finishes normally,
    the run waits for its last in-flight sends instead of cutting them
    off at the drain timeout - a completed run must not lose its tail
    as `cancelled`."""
    hold = expected_event_id("1001", "picked_up")
    transport = HoldingEvent(hold)
    orders = [make_order(14, 0, 0, 1, "1001")]
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        status_events=True,
        drain_timeout_s=0.2,  # far shorter than the hold
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="normal-end")
    await settle(40)
    # everything is dispatched; the last event is held, and the run has
    # not ended - it is waiting for it
    assert pb.status()["events_sent"] == 4
    assert pb.status()["state"] == "running"

    transport.release.set()
    await pb.wait_done()
    status = pb.status()
    assert status["state"] == "done"
    assert status["events_acked"] == 4
    events = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    assert all("error" not in line for line in events)


async def test_stop_during_the_final_wait_ends_stopped(tmp_path: Path) -> None:
    """A stop accepted while the run waits for its last held send ends
    the run as `stopped` (not `done`), inside the drain timeout; the
    held send is cancelled and its line says so, and the log stays
    dense."""
    hold = expected_event_id("1001", "picked_up")
    transport = HoldingEvent(hold)  # never released
    orders = [make_order(14, 0, 0, 1, "1001")]
    pb = make_playback(
        orders,
        tmp_path,
        inflight=8,
        speed=1e9,
        status_events=True,
        drain_timeout_s=0.2,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="final-wait")
    await settle(40)
    assert pb.status()["events_sent"] == 4
    assert pb.status()["state"] == "running"

    before = time.monotonic()
    await asyncio.wait_for(pb.stop(), 5)
    assert pb.status()["state"] == "stopped"
    assert time.monotonic() - before < 5.0
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    held = next(line for line in lines if line["kind"] == "status_event" and line.get("error"))
    assert held["error"] == "cancelled"


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

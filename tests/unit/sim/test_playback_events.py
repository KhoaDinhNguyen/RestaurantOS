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

    # review round 2, item 12: the second order's whole kitchen timeline
    # finishes while the first order's accepted is still held - the
    # chain is per order - and still nothing of 1001 follows its held
    # event. Bounded loop turns, never a real sleep.
    for _ in range(500):
        if expected_event_id("1002", "picked_up") in on_wire:
            break
        await settle(2)
        on_wire = {event_id for _status, event_id in transport.wire}
    else:
        pytest.fail("order 1002's picked_up never reached the wire")
    assert expected_event_id("1001", "preparing") not in on_wire
    assert expected_event_id("1001", "picked_up") not in on_wire

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
    try:
        # a stalled stop must fail the test, not hang the suite
        await asyncio.wait_for(pb.stop(), 5)
    finally:
        transport.release.set()  # never leave the held task behind
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
    # and the successor was cancelled *while waiting*: it never sent
    preparing = next(
        line
        for line in event_lines
        if line["external_order_id"] == "1001" and line["to_status"] == "preparing"
    )
    assert preparing["error"] == "cancelled"
    assert preparing["wall_sent_at"] is None


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


class RecordingTransport(httpx.AsyncBaseTransport):
    """Answers orders with their vendor's region (so the events route
    to it) and records the (host, port, path) of every request, so the
    tests can say where each call actually went."""

    def __init__(self, vendor_region: dict[str, str]) -> None:
        self.inner = httpx.ASGITransport(app=make_stub_app())
        self.vendor_region = vendor_region
        self.wire: list[tuple[str, int | None, str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.wire.append((request.url.host, request.url.port, request.url.path))
        path = request.url.path
        if path.endswith("/orders"):
            body = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "outcome": "accepted",
                    "duplicate": False,
                    "queued": False,
                    "order_id": str(uuid5(NAMESPACE_URL, body["external_order_id"])),
                    "region_id": self.vendor_region[body["vendor_id"]],
                },
            )
        if path.endswith("/status"):
            return httpx.Response(
                201, json={"applied": True, "duplicate": False, "conflict": False}
            )
        if path.endswith("/restock"):
            return httpx.Response(
                200,
                json={
                    "vendor_id": "v",
                    "region_id": "r",
                    "counters_restocked": 0,
                    "products_back_on": 0,
                },
            )
        return await self.inner.handle_async_request(request)


EDGE_URLS = {
    "r1": "http://edge-r1.test:8011/api/v1",
    "r2": "http://edge-r2.test:8012/api/v1",
}


async def test_kitchen_calls_go_to_their_regions_edge(tmp_path: Path) -> None:
    """Review round 2, item 6: status events and restocks are
    region-side actions - the kitchen is on the region side
    (architecture 7.2), and the edge API survives a gateway cut (T2)
    because it talks to its region's primary on the local network. Each
    event goes to its order's region edge, each restock to its vendor's
    region edge, and the orders keep going to the gateway."""
    vendor_region = {"v001": "r1", "v002": "r2"}
    transport = RecordingTransport(vendor_region)
    orders = [
        make_order(21, 0, 0, 5, "1001").model_copy(update={"vendor_id": "v001"}),
        make_order(21, 0, 0, 6, "1002").model_copy(update={"vendor_id": "v002"}),
    ]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        status_events=True,
        event_jitter_s=0.0,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        target_url="http://gateway.test/api/v1",
        kitchen_urls=EDGE_URLS,
        vendor_regions=vendor_region,
        transport=transport,
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=100.0, run_id="edges")
    await drive(pb, clock)

    for host, port, path in transport.wire:
        if path.endswith("/orders"):
            assert host == "gateway.test"
        else:
            vendor = path.split("/vendors/")[1].split("/")[0]
            expected = EDGE_URLS[vendor_region[vendor]]
            assert f"http://{host}:{port}/api/v1" == expected, path
    # every kind of call actually happened, on both regions
    paths = [path for _host, _port, path in transport.wire]
    assert sum(path.endswith("/orders") for path in paths) == 2
    assert sum(path.endswith("/status") for path in paths) == 8
    assert sum(path.endswith("/restock") for path in paths) == 2
    # the lines carry the region each call went to
    lines = run_log_lines(tmp_path, pb)
    assert {line["region_id"] for line in lines if line["kind"] == "status_event"} == {"r1", "r2"}
    assert {line["region_id"] for line in lines if line["kind"] == "restock"} == {"r1", "r2"}
    meta = json.loads((tmp_path / "edges" / "meta.json").read_text())
    assert meta["kitchen"] == {"mode": "edge", "urls": EDGE_URLS}


async def test_the_kitchen_override_sends_every_kitchen_call_to_one_url(tmp_path: Path) -> None:
    """The stub is one process: `--kitchen-target` (the engine's
    kitchen_url) sends every event and restock to that one URL, while
    the orders keep going to the target."""
    vendor_region = {"v001": "r1", "v002": "r2"}
    transport = RecordingTransport(vendor_region)
    orders = [
        make_order(21, 0, 0, 5, "1001").model_copy(update={"vendor_id": "v001"}),
        make_order(21, 0, 0, 6, "1002").model_copy(update={"vendor_id": "v002"}),
    ]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        status_events=True,
        event_jitter_s=0.0,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        target_url="http://gateway.test/api/v1",
        kitchen_url="http://kitchen.test:8900/api/v1",
        vendor_regions=vendor_region,
        transport=transport,
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=100.0, run_id="override")
    await drive(pb, clock)

    for host, port, path in transport.wire:
        if path.endswith("/orders"):
            assert host == "gateway.test"
        else:
            assert (host, port) == ("kitchen.test", 8900), path
    meta = json.loads((tmp_path / "override" / "meta.json").read_text())
    assert meta["kitchen"] == {"mode": "override", "url": "http://kitchen.test:8900/api/v1"}


async def test_meta_records_the_kitchen_routing_default(tmp_path: Path) -> None:
    """With neither the override nor the edges configured - the
    single-URL mode the stub and the unit tests run on - meta.json
    says the kitchen calls went to the target."""
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="single")
    await drive(pb, clock)
    meta = json.loads((tmp_path / "single" / "meta.json").read_text())
    assert meta["kitchen"] == {"mode": "target"}


async def test_a_restock_answered_201_is_a_failure(tmp_path: Path) -> None:
    """Review round 2, item 5: the merged spec says a restock answers
    200 and playback reads nothing else - 201 is not an ack."""

    class Restock201(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.inner = httpx.ASGITransport(app=make_stub_app())

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/restock"):
                return httpx.Response(201, json={})
            return await self.inner.handle_async_request(request)

    orders = [make_order(21, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
        transport=Restock201(),
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=100.0, run_id="restock-201")
    await drive(pb, clock)
    status = pb.status()
    assert status["restocks_sent"] == 1
    assert status["restocks_acked"] == 0
    assert status["restocks_failed"] == 1
    (line,) = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "restock"]
    assert line["http_status"] == 201


async def test_an_event_answer_saying_nothing_is_a_failure(tmp_path: Path) -> None:
    """Review round 2, item 5: a success code alone is not an ack. A 2xx
    whose body did not parse, and a 2xx that says applied: false, are
    failures; a 2xx that says conflict: true is a conflict, not an
    ack - the counters say which is which."""

    def canned(body: bytes, note: str) -> httpx.AsyncBaseTransport:
        class Canned(httpx.AsyncBaseTransport):
            def __init__(self) -> None:
                self.inner = httpx.ASGITransport(app=make_stub_app())

            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("/status"):
                    return httpx.Response(
                        200, content=body, headers={"content-type": "application/json"}
                    )
                return await self.inner.handle_async_request(request)

        Canned.__name__ = note
        return Canned()

    for body, note in [
        (b"not json at all", "unparsable"),
        (b'{"applied": false}', "not-applied"),
    ]:
        orders = [make_order(14, 0, 0, 1, "1001")]
        clock = FakeWallClock()
        pb = make_playback(
            orders, tmp_path, clock=clock, status_events=True, transport=canned(body, note)
        )
        await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id=f"ev-{note}")
        await drive(pb, clock)
        status = pb.status()
        assert status["events_failed"] == 4, note
        assert status["events_acked"] == 0, note

    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        status_events=True,
        transport=canned(b'{"applied": false, "conflict": true}', "conflicted"),
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="ev-conflicted")
    await drive(pb, clock)
    status = pb.status()
    assert status["events_conflicted"] == 4
    assert status["events_acked"] == 0
    assert status["events_failed"] == 0
    events = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    assert all(line["conflict"] is True for line in events)


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
    try:
        await asyncio.wait_for(pb.stop(), 5)
    finally:
        transport.release.set()  # never leave the held restocks behind
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
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        inflight=8,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=20, to_day=21, speed=1e9, run_id="pre-midnight")
    # fake time makes the dispatches deterministic; the run cannot end
    # while the order's answer is held
    await drive(pb, clock, steps=5)
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
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        inflight=8,
        drain_timeout_s=0.3,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
        transport=transport,
    )
    await pb.start(seed=0, from_day=20, to_day=21, speed=1e9, run_id="held-pre-restock")
    await drive(pb, clock, steps=5)
    assert pb.status()["restocks_sent"] == 0

    before = time.monotonic()
    try:
        await asyncio.wait_for(pb.stop(), 5)
    finally:
        transport.release.set()  # never leave the held order behind
    assert pb.status()["state"] == "stopped"
    assert time.monotonic() - before < 5.0, "stop sat behind the held order"
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    assert [line["kind"] for line in lines] == ["order"]
    assert lines[0]["error"] == "cancelled"


async def test_a_to_day_past_the_data_is_shortened_per_run(tmp_path: Path) -> None:
    """The engine's own window check shortens an explicit to_day to the
    last cached day (review round 2, item 4): no restock lands beyond
    the data, the run ends done without waiting for days with no
    orders, and meta.json records both the requested and the effective
    end."""
    from restaurantos.sim.replaydata import CacheMeta, require_window

    meta = CacheMeta(
        orders_sha256="x",
        order_items_sha256="y",
        warm_up_days=7,
        first_day=14,
        last_day=15,
        orders=2,
        itemless_orders=0,
    )
    orders = [make_order(14, 0, 0, 5, "1001"), make_order(15, 0, 0, 5, "1002")]
    pb = make_playback(
        orders,
        tmp_path,
        restock_every=7,
        warm_up_days=7,
        restock_vendors=("v001",),
        window_check=lambda f, t: require_window(meta, f, t),
    )
    # W=7 and R=7: without the shortening the restock days would run
    # 21, 28, 35 - all past the data
    await pb.start(seed=0, from_day=14, to_day=40, speed=1e9, run_id="short")
    await pb.wait_done()
    status = pb.status()
    assert status["state"] == "done"
    assert status["to_day"] == 15
    assert status["restocks_sent"] == 1  # day 14 alone
    meta_json = json.loads((tmp_path / "short" / "meta.json").read_text())
    assert meta_json["to_day"] == 15
    assert meta_json["to_day_requested"] == 40
    assert meta_json["restock"]["days"] == [14]
    lines = run_log_lines(tmp_path, pb)
    assert all(line["restock_day"] <= 15 for line in lines if line["kind"] == "restock")


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

        # review round 2, item 13: each control answer is checked before
        # the scenario moves on
        paused = (await client.post("/sim/playback", json={"action": "pause"})).json()
        assert paused["state"] == "paused"
        await settle()
        sped_up = (await client.patch("/sim/playback", json={"speed": 2500.0})).json()
        assert sped_up["speed"] == 2500.0
        resumed = (await client.post("/sim/playback", json={"action": "resume"})).json()
        assert resumed["state"] == "running"
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
    try:
        # a stalled stop must fail the test, not hang the suite
        await asyncio.wait_for(pb.stop(), 5)
    finally:
        transport.release.set()  # never leave the unanswered order behind
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
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        inflight=8,
        status_events=True,
        drain_timeout_s=0.2,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="normal-end")
    await drive(pb, clock, steps=5)
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
    picked_up = next(line for line in events if line["to_status"] == "picked_up")
    assert picked_up["wall_sent_at"] is not None  # acked, not cancelled


async def test_stop_during_the_final_wait_ends_stopped(tmp_path: Path) -> None:
    """A stop accepted while the run waits for its last held send ends
    the run as `stopped` (not `done`), inside the drain timeout; the
    held send is cancelled and its line says so, and the log stays
    dense."""
    hold = expected_event_id("1001", "picked_up")
    transport = HoldingEvent(hold)  # never released
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        inflight=8,
        status_events=True,
        drain_timeout_s=0.2,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="final-wait")
    await drive(pb, clock, steps=5)
    assert pb.status()["events_sent"] == 4
    assert pb.status()["state"] == "running"

    before = time.monotonic()
    try:
        await asyncio.wait_for(pb.stop(), 5)
    finally:
        transport.release.set()  # never leave the held event behind
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


# --- kitchen-call retries (review round 3, item 1) ------------------------


class FakeEdge(httpx.AsyncBaseTransport):
    """A fake region edge implementing WP1's change_status rules on the
    engine's own wire: each event id lands in processed_events and a
    repeat answers duplicate; an impossible move answers conflict and
    lands in the exception log; POST /orders is idempotent by
    (channel, external_order_id) - a repeat gets the original
    order_id back, so a re-run replays against known orders.

    Scripted outcomes are popped per call, so a test decides what the
    region does to each attempt: "apply-then-timeout" commits the
    change and then loses the answer (httpx.ReadTimeout - the edge
    does not cancel a call that missed its deadline), "503" and "409"
    answer that code once. Without a script entry the region behaves:
    a status applies per the state machine, a restock answers 200.
    """

    def __init__(self) -> None:
        from restaurantos.common.models import OrderStatus, can_transition

        self._can_transition = can_transition
        self._statuses = OrderStatus
        self.seen: set[str] = set()  # the region's processed_events
        self.status_of: dict[str, str] = {}  # order_id -> current status
        self.exception_log: list[dict] = []
        self.placed: set[str] = set()  # external ids through the inbox
        self.status_calls: dict[str, int] = {}  # event_id -> POSTs seen
        self.restock_calls: dict[str, int] = {}  # vendor_id -> POSTs seen
        self.status_script: dict[str, list[str]] = {}  # order_id -> outcomes
        self.restock_script: dict[str, list[str]] = {}  # vendor_id -> outcomes

    @staticmethod
    def _pop(script: dict[str, list[str]], key: str) -> str | None:
        outcomes = script.get(key)
        if outcomes:
            return outcomes.pop(0)
        return None

    def _apply(self, order_id: str, event_id: str, to_status: str) -> None:
        self.seen.add(event_id)
        self.status_of[order_id] = to_status

    def _status_result(
        self, order_id: str, event_id: str, to_status: str, applied: bool, **flags: bool
    ) -> httpx.Response:
        return httpx.Response(
            201,
            json={
                "event_id": event_id,
                "order_id": order_id,
                "to_status": to_status,
                "applied": applied,
                "duplicate": flags.get("duplicate", False),
                "conflict": flags.get("conflict", False),
            },
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if path.endswith("/orders"):
            external = body["external_order_id"]
            first = external not in self.placed
            self.placed.add(external)
            return httpx.Response(
                201,
                json={
                    "outcome": "accepted",
                    "duplicate": not first,
                    "order_id": stub_order_id(external),
                    "region_id": "r-fake",
                    "queued": False,
                },
            )
        if path.endswith("/status"):
            order_id = path.split("/orders/")[1].split("/")[0]
            event_id = body["event_id"]
            to_status = body["to_status"]
            self.status_calls[event_id] = self.status_calls.get(event_id, 0) + 1
            outcome = self._pop(self.status_script, order_id)
            if outcome == "apply-then-timeout":
                # the change commits, the answer never arrives
                self._apply(order_id, event_id, to_status)
                raise httpx.ReadTimeout("answer lost", request=request)
            if outcome == "503":
                return httpx.Response(503, json={"detail": "failover"})
            if outcome == "409":
                return httpx.Response(409, json={"detail": "state machine"})
            if outcome == "string-flags":
                # a flag that is not a real boolean (item 4)
                return httpx.Response(201, json={"applied": "true"})
            if event_id in self.seen:
                return self._status_result(order_id, event_id, to_status, False, duplicate=True)
            current = self._statuses(self.status_of.get(order_id, "received"))
            if not self._can_transition(current, self._statuses(to_status)):
                self.exception_log.append(
                    {"order_id": order_id, "event_id": event_id, "to_status": to_status}
                )
                return self._status_result(order_id, event_id, to_status, False, conflict=True)
            self._apply(order_id, event_id, to_status)
            return self._status_result(order_id, event_id, to_status, True)
        vendor_id = path.split("/vendors/")[1].split("/")[0]
        self.restock_calls[vendor_id] = self.restock_calls.get(vendor_id, 0) + 1
        if self._pop(self.restock_script, vendor_id) == "503":
            return httpx.Response(503, json={"detail": "failover"})
        return httpx.Response(
            200,
            json={
                "vendor_id": vendor_id,
                "region_id": "r-fake",
                "counters_restocked": 0,
                "products_back_on": 0,
            },
        )


async def test_a_retry_after_a_landed_attempt_counts_as_acked(tmp_path: Path) -> None:
    """Review round 3, item 1: the edge commits the change and then the
    answer is lost (a timeout). The retry sends the same event_id, the
    region answers duplicate, and the event counts as acked with
    landed_earlier on the line - no exception at the region, and the
    order's next event applies on its first attempt."""
    fake = FakeEdge()
    fake.status_script[stub_order_id("1001")] = ["apply-then-timeout"]
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, transport=fake, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="retry-landed")
    await drive(pb, clock)
    status = pb.status()
    assert status["state"] == "done"
    assert status["events_acked"] == 4
    assert status["events_duplicate"] == 0
    assert fake.exception_log == []
    lines = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    accepted = next(line for line in lines if line["to_status"] == "accepted")
    assert accepted["attempts"] == 2
    assert accepted["landed_earlier"] is True
    assert fake.status_calls[accepted["event_id"]] == 2
    preparing = next(line for line in lines if line["to_status"] == "preparing")
    assert preparing["attempts"] == 1
    assert preparing.get("applied") is True


async def test_a_retry_after_a_503_applies_and_is_acked(tmp_path: Path) -> None:
    """Review round 3, item 1: a 503 (the edge answers that for a few
    seconds during a failover) is retried; the event applies on the
    second attempt with no exception at the region."""
    fake = FakeEdge()
    fake.status_script[stub_order_id("1001")] = ["503"]
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, transport=fake, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="retry-503")
    await drive(pb, clock)
    assert pb.status()["events_acked"] == 4
    assert fake.exception_log == []
    accepted = next(
        line
        for line in run_log_lines(tmp_path, pb)
        if line["kind"] == "status_event" and line["to_status"] == "accepted"
    )
    assert accepted["attempts"] == 2
    assert accepted["http_status"] == 201
    assert "error" not in accepted


async def test_a_rerun_of_the_same_window_is_all_duplicates(tmp_path: Path) -> None:
    """Review round 3, item 1: a re-run of the same window sends the
    same event ids (no run id in them), and every one is answered
    duplicate by the region - no conflicts, no exceptions at HQ."""
    fake = FakeEdge()
    orders = [make_order(14, 0, 0, 5, "1001"), make_order(14, 0, 0, 6, "1002")]
    clock_a = FakeWallClock()
    first = make_playback(orders, tmp_path, clock=clock_a, transport=fake, status_events=True)
    await first.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="rerun-a")
    await drive(first, clock_a)
    assert first.status()["events_acked"] == 8

    clock_b = FakeWallClock()
    second = make_playback(orders, tmp_path, clock=clock_b, transport=fake, status_events=True)
    await second.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="rerun-b")
    await drive(second, clock_b)
    status = second.status()
    assert status["state"] == "done"
    assert status["events_acked"] == 0
    assert status["events_duplicate"] == 8
    assert status["events_conflicted"] == 0
    assert fake.exception_log == []
    for line in run_log_lines(tmp_path, second):
        if line["kind"] == "status_event":
            assert line["attempts"] == 1
            assert line.get("duplicate") is True


async def test_a_409_is_never_retried(tmp_path: Path) -> None:
    """Review round 3, item 1: a 409 is the state machine rejecting the
    change - the region answered, so there is nothing to retry: one
    attempt, counted as conflicted."""
    fake = FakeEdge()
    fake.status_script[stub_order_id("1001")] = ["409"] * 4
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, transport=fake, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="no-retry-409")
    await drive(pb, clock)
    assert pb.status()["events_conflicted"] == 4
    lines = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    assert len(lines) == 4
    for line in lines:
        assert line["attempts"] == 1
        assert line["http_status"] == 409
        assert fake.status_calls[line["event_id"]] == 1


async def test_restocks_retry_a_503_and_a_still_failing_restock_is_not_fatal(
    tmp_path: Path,
) -> None:
    """Review round 3, item 1: a restock answered 503 is retried and
    acked on the second attempt; a restock that never gets through ends
    failed after its attempts, and the run still ends done - in a
    fault harness the failed call is a result to report, not a crash."""
    fake = FakeEdge()
    fake.restock_script["v001"] = ["503"]  # one blip, then through
    fake.restock_script["v002"] = ["503", "503", "503"]  # never gets through
    orders = [make_order(21, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        transport=fake,
        status_events=True,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001", "v002"),
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=100.0, run_id="restock-retry")
    await drive(pb, clock)
    status = pb.status()
    assert status["state"] == "done"
    assert status["restocks_acked"] == 1
    assert status["restocks_failed"] == 1
    by_vendor = {
        line["vendor_id"]: line for line in run_log_lines(tmp_path, pb) if line["kind"] == "restock"
    }
    assert by_vendor["v001"]["attempts"] == 2
    assert by_vendor["v001"]["http_status"] == 200
    assert by_vendor["v002"]["attempts"] == 3
    assert by_vendor["v002"]["error"] == "http 503"
    assert fake.restock_calls["v001"] == 2
    assert fake.restock_calls["v002"] == 3


async def test_a_stop_during_a_backoff_cancels_the_retry(tmp_path: Path) -> None:
    """Review round 3, item 1: a stop while a retry sits in its backoff
    cancels the task - the line says cancelled with the attempts that
    were made, the log stays dense, and the run ends stopped."""
    fake = FakeEdge()
    fake.restock_script["v001"] = ["503", "503", "503"]
    orders = [make_order(21, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        transport=fake,
        status_events=True,
        drain_timeout_s=0.3,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
    )
    await pb.start(seed=0, from_day=21, to_day=21, speed=1e9, run_id="backoff-stop")
    await drive(pb, clock, steps=3)  # attempt 1 has its 503; the task backs off
    await settle()
    assert fake.restock_calls["v001"] == 1
    await asyncio.wait_for(pb.stop(), 5)
    assert pb.status()["state"] == "stopped"
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    restock_line = lines[0]
    assert restock_line["kind"] == "restock"
    assert restock_line["error"] == "cancelled"
    assert restock_line["attempts"] == 1


async def test_a_status_flag_that_is_not_a_boolean_fails_the_event(tmp_path: Path) -> None:
    """Review round 3, item 4: {"applied": "false"} is a non-empty
    string, which Python treats as true - a flag that is not a real
    boolean makes the answer malformed, and the event fails instead of
    counting as acked."""
    fake = FakeEdge()
    fake.status_script[stub_order_id("1001")] = ["string-flags"] * 4
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, transport=fake, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="string-flags")
    await drive(pb, clock)
    assert pb.status()["events_failed"] == 4
    assert pb.status()["events_acked"] == 0
    lines = [line for line in run_log_lines(tmp_path, pb) if line["kind"] == "status_event"]
    assert len(lines) == 4
    for line in lines:
        assert line["error"] == "malformed status answer"


async def test_ids_in_kitchen_paths_are_encoded_per_segment(tmp_path: Path) -> None:
    """Review round 3, item 10: the paths are built from data, so the
    ids are encoded per segment - a vendor id containing "/" and a
    space reaches the route as one path segment, not as extra path."""
    transport = RegionAnswering("r1")  # records every URL it is asked for
    orders = [make_order(14, 0, 0, 5, "1001")]
    orders[0] = orders[0].model_copy(update={"vendor_id": "v/1 x"})
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, transport=transport, status_events=True)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="encoded-ids")
    await drive(pb, clock)
    status_urls = [url for url in transport.requests if url.endswith("/status")]
    assert len(status_urls) == 4
    for url in status_urls:
        assert "/vendors/v%2F1%20x/" in url
        assert "v/1 x" not in url


# --- an unknown region is not sent to the gateway (review round 3, item 2)


class RegionAnswering(httpx.AsyncBaseTransport):
    """Answers every order with a fixed region_id, and records every
    URL it is asked for, so a test can prove nothing but the order
    POST ever left the target."""

    def __init__(self, region_id: object) -> None:
        self.region_id = region_id
        self.requests: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        body = json.loads(request.content) if request.content else {}
        if request.url.path.endswith("/orders"):
            return httpx.Response(
                201,
                json={
                    "outcome": "accepted",
                    "order_id": stub_order_id(body["external_order_id"]),
                    "region_id": self.region_id,
                    "queued": False,
                },
            )
        # the edge's own routes: applied, whatever the path was
        return httpx.Response(201, json={"applied": True, "duplicate": False, "conflict": False})


async def test_an_unknown_region_is_failed_not_sent_to_the_gateway(tmp_path: Path) -> None:
    """Review round 3, item 2: in edge mode an order whose answer names
    a region the routing does not know sends no kitchen calls at all -
    each event is failed with "unknown region" on its line, nothing
    reaches the gateway (or anywhere else), and the log has no missing
    placeholder."""
    transport = RegionAnswering("r9")
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        transport=transport,
        status_events=True,
        kitchen_urls={"r1": "http://edge-1.test/api/v1"},
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="unknown-region")
    await drive(pb, clock)
    status = pb.status()
    assert status["state"] == "done"
    assert status["events_failed"] == 4
    assert status["events_acked"] == 0
    # only the order POST ever went out - to the target, nowhere else
    assert transport.requests == ["http://stub.test/api/v1/orders"]
    lines = run_log_lines(tmp_path, pb)
    assert not any(line["kind"] == "missing" for line in lines)
    for line in lines:
        if line["kind"] == "status_event":
            assert line["error"] == "unknown region"
            assert "http_status" not in line
            assert line["wall_sent_at"] is None


async def test_a_non_string_region_id_is_failed_the_same_way(tmp_path: Path) -> None:
    """Review round 3, item 2: a region_id that is not even a string
    (a bad answer put a list there) is the same unknown region: the
    event fails with the error on its line, the task does not die, and
    the log has no missing placeholder."""
    transport = RegionAnswering(["r1"])
    orders = [make_order(14, 0, 0, 5, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        transport=transport,
        status_events=True,
        kitchen_urls={"r1": "http://edge-1.test/api/v1"},
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="non-string-region")
    await drive(pb, clock)
    assert pb.status()["events_failed"] == 4
    assert transport.requests == ["http://stub.test/api/v1/orders"]
    lines = run_log_lines(tmp_path, pb)
    assert not any(line["kind"] == "missing" for line in lines)
    for line in lines:
        if line["kind"] == "status_event":
            assert line["error"] == "unknown region"

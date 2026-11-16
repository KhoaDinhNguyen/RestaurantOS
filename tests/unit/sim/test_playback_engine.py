"""The playback engine: scheduling, the in-flight bound, the run log's
send-order guarantee, and the determinism acceptance test.

Fake time (conftest.FakeWallClock) drives every scheduling test, so no
test sleeps and every send lands at an exact, reproducible wall second.
The in-flight and drain tests use the real clock with a stub that
answers after a real, tiny delay.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import httpx
import pytest
from conftest import FakeWallClock, drive, make_order, make_playback, sample_orders, settle
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from restaurantos.sim.control import make_control_app
from restaurantos.sim.playback import Playback, RunLog, StateError
from restaurantos.sim.stub import make_stub_app


def log_file(runs_dir: Path, pb: Playback) -> Path:
    status = pb.status()
    assert status["run_id"], f"no run yet (state {status['state']})"
    return runs_dir / status["run_id"] / "orders.jsonl"


def run_log_lines(runs_dir: Path, pb: Playback) -> list[dict]:
    return [json.loads(line) for line in log_file(runs_dir, pb).read_text().splitlines()]


# --- the acceptance line ----------------------------------------------------


async def _run_scenario(runs_dir: Path, orders, seed: int) -> str:
    """One full scenario: start, send a while, pause, change speed,
    resume, run to the end - all through the control routes."""
    clock = FakeWallClock()
    pb = make_playback(orders, runs_dir, clock=clock, inflight=4)
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    )
    try:
        # 100x keeps the run long enough that the speed change below
        # lands mid-run, not after the last order (sample_orders spans
        # 200 simulated seconds = 40 steps at 100x).
        response = await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "speed": 100.0, "seed": seed},
        )
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "running"

        await drive(pb, clock, steps=6)  # 30 simulated seconds out
        assert pb.status()["orders_acked"] > 0
        assert pb.status()["state"] == "running"

        await client.post("/sim/playback", json={"action": "pause"})
        await settle()
        await client.patch("/sim/playback", json={"speed": 2500.0})
        await client.post("/sim/playback", json={"action": "resume"})
        await drive(pb, clock)  # to the end
    finally:
        await client.aclose()
    assert pb.status()["state"] == "done", pb.status()
    return log_file(runs_dir, pb).read_text()


async def test_same_seed_gives_the_same_run_log_twice(tmp_path: Path) -> None:
    """The brief's `unit` acceptance line, including the mid-run speed
    change: two runs of the same scenario produce byte-identical logs."""
    orders = sample_orders(40)
    log_a = await _run_scenario(tmp_path / "a", orders, seed=42)
    log_b = await _run_scenario(tmp_path / "b", orders, seed=42)
    assert log_a == log_b
    # every order went out
    assert log_a.count("\n") == len(orders)
    # same-second triples kept their stream order in both runs
    ids_a = [json.loads(line)["external_order_id"] for line in log_a.splitlines()]
    assert ids_a == [order.external_order_id for order in orders]


# --- scheduling -------------------------------------------------------------


async def test_send_offsets_follow_the_speed(tmp_path: Path) -> None:
    """t simulated seconds after the start go out t/speed wall seconds
    after the start; a speed change re-anchors from the current instant."""
    clock = FakeWallClock()
    orders = [
        make_order(14, 0, 0, 10, "1"),  # 10 sim s after the midnight start
        make_order(14, 0, 0, 20, "2"),  # 20 sim s
    ]
    pb = make_playback(orders, tmp_path, clock=clock, inflight=2)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="offsets")

    await drive(pb, clock, steps=1)  # wall 0.05 = sim 5 s
    assert pb.status()["orders_sent"] == 0
    await drive(pb, clock, steps=1)  # wall 0.10 = sim 10 s: first order due
    assert pb.status()["orders_sent"] == 1

    await pb.pause()  # fake time moves below, simulated time must not
    await pb.set_speed(50.0)
    await pb.resume()
    await drive(pb, clock, steps=3)  # 0.15, 0.20, 0.25: not yet 20 sim s
    assert pb.status()["orders_sent"] == 1
    await drive(pb, clock, steps=1)  # 0.30: 0.2 wall s at 50x = 10 more sim s
    await pb.wait_done()

    lines = run_log_lines(tmp_path, pb)
    assert [line["sim_sent_at"] for line in lines] == [10.0, 20.0]
    assert [line["wall_sent_at"] for line in lines] == [0.10, 0.30]


async def test_pause_freezes_and_resume_finishes(tmp_path: Path) -> None:
    orders = sample_orders(5)
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=2)
    # 100x: each 0.05 step is 5 simulated seconds, so two steps send
    # exactly the first two orders and the run is still live
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="paused")
    await drive(pb, clock, steps=2)
    sent_at_pause = pb.status()["orders_sent"]
    assert sent_at_pause == 2

    await pb.pause()
    await drive(pb, clock, steps=5)  # fake time moves; nothing may be sent
    assert pb.status()["orders_sent"] == sent_at_pause
    assert pb.clock()["state"] == "paused"

    await pb.resume()
    await drive(pb, clock)
    assert pb.status()["state"] == "done"
    assert pb.status()["orders_sent"] == len(orders)


async def test_stop_before_any_send_leaves_an_empty_log(tmp_path: Path) -> None:
    orders = sample_orders(5)
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=2)
    await pb.start(seed=0, from_day=14, to_day=14, speed=1000.0, run_id="stopped")
    await pb.stop()
    assert pb.status()["state"] == "stopped"
    assert pb.status()["orders_sent"] == 0
    assert log_file(tmp_path, pb).read_text() == ""
    # the slot accepts a new run afterwards
    await pb.start(seed=0, from_day=14, to_day=14, speed=1000.0, run_id="again")
    await drive(pb, clock)
    assert pb.status()["state"] == "done"


async def test_wrong_state_is_rejected(tmp_path: Path) -> None:
    pb = make_playback(sample_orders(2), tmp_path, clock=FakeWallClock())
    with pytest.raises(StateError):
        await pb.pause()  # idle
    with pytest.raises(StateError):
        await pb.set_speed(2.0)  # idle
    with pytest.raises(ValueError, match="before from_day"):
        await pb.start(seed=0, from_day=14, to_day=13, speed=1.0, run_id="bad")


# --- the in-flight bound, drain, and log order -----------------------------


class CountingTransport(httpx.AsyncBaseTransport):
    """Wraps the stub and counts how many requests overlap."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.active = 0
        self.max_active = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            return await self.inner.handle_async_request(request)
        finally:
            self.active -= 1


async def test_inflight_bound_is_held(tmp_path: Path) -> None:
    transport = CountingTransport(httpx.ASGITransport(app=make_stub_app(delay_seconds=0.05)))
    orders = sample_orders(10)
    # everything is due immediately: the bound is the only brake
    pb = make_playback(orders, tmp_path, inflight=2, speed=1e9, transport=transport)
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="bound")
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert transport.max_active == 2  # held exactly at the bound
    assert len(run_log_lines(tmp_path, pb)) == len(orders)


async def test_log_is_in_send_order_despite_reordered_acks(tmp_path: Path) -> None:
    """Responses come back scrambled (the delay is a hash of the order
    id); the file still lists sends in stream order."""

    class SlowStub(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.inner = httpx.ASGITransport(app=make_stub_app())

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            digest = hashlib.sha256(body["external_order_id"].encode()).digest()
            await asyncio.sleep(0.001 * digest[0])  # 0 to 0.255 s, scrambled
            return await self.inner.handle_async_request(request)

    orders = sample_orders(20)
    pb = make_playback(orders, tmp_path, inflight=20, speed=1e9, transport=SlowStub())
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="reordered")
    await pb.wait_done()
    lines = run_log_lines(tmp_path, pb)
    assert [line["seq"] for line in lines] == list(range(len(orders)))
    assert [line["external_order_id"] for line in lines] == [
        order.external_order_id for order in orders
    ]


async def test_drain_timeout_records_every_dispatched_order(tmp_path: Path) -> None:
    """A burst faster than the consumer: the drain timeout cancels
    tasks still queued for a slot, and every dispatched order must
    still get its log line - found by the real-data throughput probe,
    which lost 460 lines before the record moved before the
    semaphore."""
    transport = CountingTransport(httpx.ASGITransport(app=make_stub_app(delay_seconds=0.2)))
    orders = sample_orders(6)
    pb = Playback(
        lambda a, b: iter(orders),
        target_url="http://stub.test/api/v1",
        inflight=1,
        http_timeout_s=5.0,
        drain_timeout_s=0.05,  # nothing can finish in time
        runs_dir=tmp_path,
        transport=transport,
    )  # real wall clock: the stub's 0.2 s delay must actually elapse
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="burst")
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["orders_sent"] == len(orders)
    lines = run_log_lines(tmp_path, pb)
    assert len(lines) == len(orders)  # one line per dispatched order
    assert [line["seq"] for line in lines] == list(range(len(orders)))
    assert all(line["error"] == "cancelled" for line in lines)
    assert pb.status()["orders_failed"] == len(orders)


async def test_stop_drains_in_flight_requests(tmp_path: Path) -> None:
    transport = CountingTransport(httpx.ASGITransport(app=make_stub_app(delay_seconds=0.05)))
    orders = sample_orders(6)
    pb = make_playback(orders, tmp_path, inflight=6, speed=1e9, transport=transport)
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="drain")
    await settle()
    assert pb.status()["inflight"] > 0
    await pb.stop()
    # everything already sent was answered and logged
    lines = run_log_lines(tmp_path, pb)
    assert len(lines) == pb.status()["orders_sent"]
    assert all(line["http_status"] == 201 for line in lines)
    assert pb.status()["orders_acked"] == len(lines)


# --- what gets logged -------------------------------------------------------


def _answer_app() -> FastAPI:
    """Answers the whole POST /orders status set: 201 committed, 202
    parked, 409 refused, one 500."""
    app = FastAPI()

    @app.post("/orders")
    @app.post("/api/v1/orders")
    async def place(request: dict) -> object:
        oid = request["external_order_id"]
        if oid.endswith("p"):
            return JSONResponse(
                status_code=202,
                content={"queued": True, "region_id": "r1", "received_at": "2020-01-01T00:00:00Z"},
            )
        if oid.endswith("r"):
            return JSONResponse(
                status_code=409,
                content={"outcome": "refused", "refusal_reason": "out_of_stock"},
            )
        if oid.endswith("x"):
            return JSONResponse(status_code=500, content={"boom": True})
        return JSONResponse(
            status_code=201,
            content={
                "outcome": "accepted",
                "duplicate": False,
                "order_id": "00000000-0000-4000-8000-000000000001",
                "region_id": "r1",
                "menu_version": 3,
                "received_at": "2020-01-02T03:04:05Z",
                "committed_at": "2020-01-02T03:04:05Z",
                "acked_at": "2020-01-02T03:04:06Z",
            },
        )

    return app


async def test_status_counts_and_passthrough(tmp_path: Path) -> None:
    orders = [
        make_order(14, 0, 0, 1, "1"),
        make_order(14, 0, 0, 2, "2p"),
        make_order(14, 0, 0, 3, "3r"),
        make_order(14, 0, 0, 4, "4x"),
    ]
    pb = make_playback(
        orders, tmp_path, inflight=4, speed=1e9, transport=httpx.ASGITransport(app=_answer_app())
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="statuses")
    await pb.wait_done()
    status = pb.status()
    assert status["orders_acked"] == 1
    assert status["orders_parked"] == 1
    assert status["orders_refused"] == 1
    assert status["orders_failed"] == 1

    lines = {line["external_order_id"]: line for line in run_log_lines(tmp_path, pb)}
    assert lines["1"]["menu_version"] == 3
    assert lines["1"]["http_status"] == 201
    assert lines["2p"]["queued"] is True
    assert lines["3r"]["refusal_reason"] == "out_of_stock"
    assert lines["4x"]["http_status"] == 500


async def test_transport_errors_are_logged_not_fatal(tmp_path: Path) -> None:
    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    orders = sample_orders(3)
    pb = make_playback(
        orders, tmp_path, inflight=3, speed=1e9, transport=httpx.MockTransport(unreachable)
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="unreachable")
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["orders_failed"] == 3  # counted, not just logged
    assert [line["error"] for line in run_log_lines(tmp_path, pb)] == ["ConnectError"] * 3


async def test_non_http_errors_are_logged_not_fatal(tmp_path: Path) -> None:
    """Anything a send can raise - not only httpx.HTTPError - must end
    up as the order's log line and in the failure count (review A2:
    e.g. httpx.InvalidURL is not an HTTPError)."""

    def boom(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("boom")

    orders = sample_orders(3)
    pb = make_playback(orders, tmp_path, inflight=3, speed=1e9, transport=httpx.MockTransport(boom))
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="boom")
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["orders_failed"] == 3
    lines = run_log_lines(tmp_path, pb)
    assert [line["error"] for line in lines] == ["RuntimeError"] * 3
    assert [line["seq"] for line in lines] == [0, 1, 2]  # no gap held the buffer hostage


async def test_run_meta_is_written(tmp_path: Path) -> None:
    pb = make_playback(sample_orders(2), tmp_path, clock=FakeWallClock())
    await pb.start(
        seed=7,
        from_day=14,
        to_day=20,
        speed=100.0,
        run_id="meta",
        meta_extra={"cache": {"warm_up_days": 14}},
    )
    meta = json.loads((tmp_path / "meta" / "meta.json").read_text())
    assert meta["seed"] == 7
    assert meta["from_day"] == 14
    assert meta["to_day"] == 20
    assert meta["speed"] == 100.0
    assert meta["cache"] == {"warm_up_days": 14}
    assert meta["target_url"].endswith("/orders")
    # B6: the monotonic baseline that maps wall_sent_at/wall_acked_at to
    # absolute time (wall_start_epoch + (t - wall_start_monotonic))
    assert meta["wall_start_monotonic"] == 0.0  # the fake clock starts at zero


# --- RunLog -----------------------------------------------------------------


def test_runlog_writes_in_sequence_order(tmp_path: Path) -> None:
    """Out-of-order adds wait in the buffer until the gap in front of
    them is filled (review A3: the old test never had anything
    waiting)."""
    log = RunLog(tmp_path / "orders.jsonl")
    log.add(2, {"seq": 2})
    log.add(1, {"seq": 1})
    log.flush()
    # both are waiting behind the missing seq 0; nothing was consumed
    assert (tmp_path / "orders.jsonl").read_text() == ""
    log.add(0, {"seq": 0})
    log.flush()
    seqs = [
        json.loads(line)["seq"] for line in (tmp_path / "orders.jsonl").read_text().splitlines()
    ]
    assert seqs == [0, 1, 2]  # the gap filled, all three consumed in order
    log.close()
    assert [
        json.loads(line)["seq"] for line in (tmp_path / "orders.jsonl").read_text().splitlines()
    ] == [0, 1, 2]


def test_runlog_close_marks_only_the_missing_sends(tmp_path: Path) -> None:
    """A gap gets a placeholder line; the records that did complete are
    written unchanged, not relabelled as missing (review A3)."""
    log = RunLog(tmp_path / "orders.jsonl")
    log.add(0, {"seq": 0, "http_status": 201})
    log.add(2, {"seq": 2, "http_status": 201})
    log.close()
    lines = [json.loads(line) for line in (tmp_path / "orders.jsonl").read_text().splitlines()]
    assert [line["seq"] for line in lines] == [0, 1, 2]
    assert lines[0]["http_status"] == 201 and "error" not in lines[0]
    assert lines[1]["error"] == "missing: the send task produced no result"
    assert lines[2]["http_status"] == 201 and "error" not in lines[2]


# --- review round 1 ---------------------------------------------------------


async def test_cleanup_failure_still_ends_the_run(tmp_path: Path, monkeypatch) -> None:
    """A log whose close() raises must not hang stop()/wait_done() and
    must end the run FAILED (review A1)."""
    from restaurantos.sim import playback as playback_module

    class FailingCloseLog(RunLog):
        def close(self) -> None:
            raise OSError("disk full")

    monkeypatch.setattr(playback_module, "RunLog", FailingCloseLog)
    orders = sample_orders(2)
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock)
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="badlog")
    await drive(pb, clock)  # the run completes; its log then refuses to close
    await asyncio.wait_for(pb.wait_done(), timeout=5)
    assert pb.status()["state"] == "failed"


async def test_failed_run_cannot_restart_until_cleanup_ends(tmp_path: Path) -> None:
    """A stream that blows up mid-run leaves the state non-final while
    the drain is still working; a start must be refused until cleanup
    has finished, then allowed (review A1)."""

    def orders_fn(a, b):
        def generate():
            yield make_order(14, 0, 0, 1, "1")
            raise RuntimeError("corrupt cache")

        return generate()

    transport = CountingTransport(httpx.ASGITransport(app=make_stub_app(delay_seconds=0.2)))
    pb = Playback(
        orders_fn,
        target_url="http://stub.test/api/v1",
        inflight=4,
        http_timeout_s=5.0,
        drain_timeout_s=5.0,
        runs_dir=tmp_path,
        transport=transport,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="boom")
    await settle()
    # the send is still in flight; the run is not final yet
    with pytest.raises(StateError):
        await pb.start(seed=1, from_day=14, to_day=14, speed=1.0, run_id="too-early")
    await asyncio.wait_for(pb.wait_done(), timeout=5)
    assert pb.status()["state"] == "failed"
    # cleanup is over: the slot is free again
    await pb.start(seed=1, from_day=14, to_day=14, speed=1.0, run_id="after")
    assert pb.status()["state"] == "running"


async def test_run_id_is_rejected_unless_one_safe_component(tmp_path: Path) -> None:
    """Traversal, separators, absolute paths and reused ids are refused,
    without half-resetting the engine (review A4)."""
    orders = sample_orders(2)
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock)

    for bad in ("..", "a/b", "/tmp/evil", "x" * 65, ".hidden"):
        with pytest.raises(ValueError):
            await pb.start(seed=0, from_day=14, to_day=14, speed=1.0, run_id=bad)

    # a valid run goes through, and its id cannot be reused
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="good")
    await drive(pb, clock)
    assert pb.status()["state"] == "done"
    with pytest.raises(ValueError, match="already exists"):
        await pb.start(seed=0, from_day=14, to_day=14, speed=1.0, run_id="good")
    # the engine survived the refusals and still starts fresh runs
    await pb.start(seed=0, from_day=14, to_day=14, speed=100.0, run_id="good2")
    await drive(pb, clock)
    assert pb.status()["state"] == "done"


async def test_route_refuses_an_unsafe_run_id(tmp_path: Path) -> None:
    """The API start path validates run ids too (review A4)."""
    pb = make_playback(sample_orders(2), tmp_path, clock=FakeWallClock())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        response = await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "run_id": "../escape"},
        )
        # refused at the schema (the documented pattern) before the
        # engine's own check even runs; either way it is a 422
        assert response.status_code == 422
        assert "run_id" in response.text
        # and the engine is still usable
        ok = await client.post(
            "/sim/playback", json={"action": "start", "from_day": 14, "run_id": "fine"}
        )
        assert ok.status_code == 200
        assert ok.json()["state"] == "running"


async def test_window_check_refuses_api_starts(tmp_path: Path) -> None:
    """The replay window is refused, never clamped, on the API path too
    (review B1)."""
    from restaurantos.sim.replaydata import CacheMeta, require_window

    meta = CacheMeta(
        orders_sha256="x",
        order_items_sha256="y",
        warm_up_days=14,
        first_day=14,
        last_day=90,
        orders=10,
        itemless_orders=0,
    )
    orders = sample_orders(3)
    clock = FakeWallClock()
    pb = make_playback(
        orders, tmp_path, clock=clock, window_check=lambda f, t: require_window(meta, f, t)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        # into the warm-up window: 422
        response = await client.post("/sim/playback", json={"action": "start", "from_day": 0})
        assert response.status_code == 422
        assert "warm-up" in response.json()["detail"]
        # past the last cached day: 422 as well
        response = await client.post("/sim/playback", json={"action": "start", "from_day": 95})
        assert response.status_code == 422
        assert "last cached day" in response.json()["detail"]
        # a valid window still starts
        ok = await client.post("/sim/playback", json={"action": "start", "from_day": 14})
        assert ok.status_code == 200
        await drive(pb, clock)
        assert pb.status()["state"] == "done"


async def test_pause_is_honoured_during_a_burst(tmp_path: Path) -> None:
    """When a whole burst is due at once, the dispatcher must yield
    between orders so a pause actually lands mid-burst (review B3).
    Before the fix, all 100 were dispatched in one unbroken run and the
    pause request could not be processed until after."""
    orders = [make_order(14, 0, 0, 0, str(1_000 + i)) for i in range(100)]
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=100)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        await client.post("/sim/playback", json={"action": "start", "from_day": 14})
        await settle()
        await client.post("/sim/playback", json={"action": "pause"})
        sent = pb.status()["orders_sent"]
        assert sent < len(orders), "pause did not land until the whole burst was dispatched"
        await client.post("/sim/playback", json={"action": "resume"})
        await drive(pb, clock)
        assert pb.status()["state"] == "done"
        assert pb.status()["orders_sent"] == len(orders)

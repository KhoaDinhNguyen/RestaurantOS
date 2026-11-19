"""The /sim control routes: the state machine, the status and clock
answers, and the 409/422 errors the architecture's contract implies."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
from conftest import FakeWallClock, drive, make_order, make_playback, sample_orders, settle


@pytest.fixture
def pb(tmp_path: Path) -> object:
    return make_playback(sample_orders(10), tmp_path, clock=FakeWallClock())


@pytest.fixture
async def client(pb):
    from restaurantos.sim.control import make_control_app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)),
        base_url="http://control.test",
    ) as client:
        yield client


async def test_idle_answers(client):
    status = (await client.get("/sim/playback")).json()
    assert status["state"] == "idle"
    assert status["orders_sent"] == 0
    clock = (await client.get("/sim/clock")).json()
    assert clock["sim_day"] is None and clock["speed"] is None


async def test_start_requires_from_day(client):
    response = await client.post("/sim/playback", json={"action": "start"})
    assert response.status_code == 422


async def test_unknown_action_is_rejected(client):
    # the schema rejects it before the engine runs: 422, FastAPI's body
    response = await client.post("/sim/playback", json={"action": "rewind", "from_day": 14})
    assert response.status_code == 422


async def test_wrong_state_answers_the_project_error_body(client):
    """API review 4.5: a command the run state does not allow is 409
    with {"error": "invalid_state", "state": ..., "message": ...}, on
    every route that can raise it - not FastAPI's {"detail": ...}."""
    response = await client.patch("/sim/playback", json={"speed": 2.0})  # idle
    assert response.status_code == 409
    body = response.json()
    assert body == {
        "error": "invalid_state",
        "state": "idle",
        "message": "cannot change speed while the run is idle",
    }


async def test_full_cycle_through_the_routes(client, pb, tmp_path):
    start = (
        await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "speed": 1000.0, "seed": 5},
        )
    ).json()
    assert start["state"] == "running"
    assert start["seed"] == 5

    # a start on a live run is a conflict, not a restart - in the
    # project's error body, saying what the run is in
    again = await client.post("/sim/playback", json={"action": "start", "from_day": 14})
    assert again.status_code == 409
    assert again.json() == {
        "error": "invalid_state",
        "state": "running",
        "message": "cannot start a run while the run is running",
    }

    clock = (await client.get("/sim/clock")).json()
    assert clock["sim_day"] == 14
    assert clock["speed"] == 1000.0

    paused = (await client.post("/sim/playback", json={"action": "pause"})).json()
    assert paused["state"] == "paused"
    # pause is visible to any other process that asks - that is what
    # baseline sync will use to refuse while a run is live
    assert (await client.get("/sim/playback")).json()["state"] == "paused"

    double_pause = await client.post("/sim/playback", json={"action": "pause"})
    assert double_pause.status_code == 409
    assert double_pause.json()["state"] == "paused"
    assert double_pause.json()["message"] == "cannot pause while the run is paused"

    faster = (await client.patch("/sim/playback", json={"speed": 5000.0})).json()
    assert faster["speed"] == 5000.0
    bad_speed = await client.patch("/sim/playback", json={"speed": 0.0})
    assert bad_speed.status_code == 422

    resumed = (await client.post("/sim/playback", json={"action": "resume"})).json()
    assert resumed["state"] == "running"

    stopped = (await client.post("/sim/playback", json={"action": "stop"})).json()
    assert stopped["state"] == "stopped"
    assert stopped["orders_sent"] == 0  # stopped before the first order was due

    idle_ops = [
        ("post", {"action": "pause"}),
        ("post", {"action": "resume"}),
        ("post", {"action": "stop"}),
    ]
    for method, body in idle_ops:
        response = await client.request(method.upper(), "/sim/playback", json=body)
        assert response.status_code == 409, (method, body)
        assert response.json()["error"] == "invalid_state"
        # the run above ended stopped, and the body says so
        assert response.json()["state"] == "stopped"


async def test_api_start_past_the_data_is_shortened(tmp_path: Path, caplog) -> None:
    """Review round 2, item 4: an API start with a to_day past the last
    cached day still succeeds - the answer shows the effective day, the
    process's logger says so (review round 3, item 9 moved the warning
    from a bare print to logging), and meta.json records both ends.

    Review round 4, item 8: caplog's level is set on the exact logger,
    so the capture is what the test claims, not an accident of the
    root logger's level."""
    import json
    import logging

    from restaurantos.sim.control import make_control_app
    from restaurantos.sim.replaydata import CacheMeta, require_window

    caplog.set_level(logging.WARNING, logger="restaurantos.sim.playback")
    meta = CacheMeta(
        orders_sha256="x",
        order_items_sha256="y",
        warm_up_days=14,
        first_day=14,
        last_day=14,
        orders=1,
        itemless_orders=0,
    )
    clock = FakeWallClock()
    pb = make_playback(
        sample_orders(2),
        tmp_path,
        clock=clock,
        window_check=lambda f, t: require_window(meta, f, t),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        response = await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "to_day": 200, "speed": 100.0},
        )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["to_day"] == meta.last_day
    assert f"data only goes to day {meta.last_day}" in caplog.text
    run_meta = json.loads((tmp_path / body["run_id"] / "meta.json").read_text())
    assert run_meta["to_day"] == meta.last_day
    assert run_meta["to_day_requested"] == 200
    await drive(pb, clock)


async def test_the_shortening_warning_survives_a_broken_stderr(tmp_path: Path, caplog) -> None:
    """Review round 3, item 9: the warning goes through logging, and a
    handler that cannot write - a closed stderr pipe - is reported by
    logging itself instead of raising into the route: the start that
    already succeeded still answers 200 with the shortened to_day.

    Review round 4, item 8: WARNING is enabled on the exact logger and
    the broken stream counts its write attempts, so the test proves the
    handler really was asked to write."""
    import logging

    from restaurantos.sim.control import make_control_app
    from restaurantos.sim.replaydata import CacheMeta, require_window

    class BrokenStderr:
        """A stderr that is a closed pipe: every write raises, and
        every attempt is counted."""

        def __init__(self) -> None:
            self.writes = 0

        def write(self, *args):
            self.writes += 1
            raise BrokenPipeError("stderr is a closed pipe")

        def flush(self):
            raise BrokenPipeError("stderr is a closed pipe")

    caplog.set_level(logging.WARNING, logger="restaurantos.sim.playback")
    broken = BrokenStderr()
    logger = logging.getLogger("restaurantos.sim.playback")
    # a real StreamHandler over the broken stream: its emit catches the
    # write error and reports it through handleError, which swallows
    # the OSError it meets while reporting - the route never sees it
    handler = logging.StreamHandler(broken)
    logger.addHandler(handler)
    try:
        meta = CacheMeta(
            orders_sha256="x",
            order_items_sha256="y",
            warm_up_days=14,
            first_day=14,
            last_day=14,
            orders=1,
            itemless_orders=0,
        )
        clock = FakeWallClock()
        pb = make_playback(
            sample_orders(2),
            tmp_path,
            clock=clock,
            window_check=lambda f, t: require_window(meta, f, t),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=make_control_app(pb)),
            base_url="http://control.test",
        ) as client:
            response = await client.post(
                "/sim/playback",
                json={"action": "start", "from_day": 14, "to_day": 200, "speed": 100.0},
            )
        assert response.status_code == 200, response.text
        assert response.json()["to_day"] == meta.last_day
        # the handler really was asked to write, and the route never
        # saw the failure
        assert broken.writes >= 1
        await drive(pb, clock)
    finally:
        logger.removeHandler(handler)


async def test_the_clock_stands_still_when_paused_or_ended(tmp_path: Path):
    """API review 4.5: while paused, and after a run has ended (done or
    stopped), the clock stands at its last moment; simulated time must
    not keep moving behind GET /sim/clock.

    Review round 3, item 14: the snapshot pairs around pause and stop
    are read without any clock advance in between and must match
    exactly, and after a normal end the clock stands at the final
    simulated instant - the last dispatched item's deadline, rounded
    down to the second.

    Review round 4, item 3 says which contract that instant is: the
    clock shows where the replay stopped, so a normal end freezes it
    when the dispatcher dispatches its last item, and a stop freezes
    it at the moment the stop is accepted - never after the cleanup
    waits (the final wait for outstanding sends, the drain)."""
    clock = FakeWallClock()
    pb = make_playback(sample_orders(6), tmp_path, clock=clock, inflight=4)
    from restaurantos.sim.control import make_control_app

    def moment(snapshot: dict) -> dict:
        # the simulated moment, without the run's state, which is the
        # one field a pause or a stop is allowed to change
        return {key: value for key, value in snapshot.items() if key != "state"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        await client.post("/sim/playback", json={"action": "start", "from_day": 14, "speed": 100.0})
        await drive(pb, clock, steps=4)  # 20 simulated seconds in

        before_pause = (await client.get("/sim/clock")).json()
        await client.post("/sim/playback", json={"action": "pause"})
        after_pause = (await client.get("/sim/clock")).json()
        assert after_pause["state"] == "paused"
        assert moment(after_pause) == moment(before_pause)  # the pause moves nothing
        frozen = after_pause
        await drive(pb, clock, steps=10)  # wall time moves; the clock must not
        assert (await client.get("/sim/clock")).json() == frozen

        await client.post("/sim/playback", json={"action": "resume"})
        # fine steps to the end (0.001 wall = 0.1 simulated seconds), so
        # the clock never overshoots the last deadline by a whole
        # coarse step
        from conftest import FINAL_STATES, settle

        for _ in range(400):
            if pb.status()["state"] in FINAL_STATES:
                break
            await clock.advance(clock.now() + 0.001)
            await settle()
        assert pb.status()["state"] == "done"
        ended = (await client.get("/sim/clock")).json()
        assert ended["state"] == "done"
        # the clock stands at the final simulated instant: sample_orders(6)
        # ends 30 seconds after midnight, and the last dispatched item's
        # deadline is exactly that, rounded down to the second
        assert ended["sim_day"] == 14
        assert ended["sim_time"] == "00:00:30"
        await clock.advance(clock.now() + 1.0)
        assert (await client.get("/sim/clock")).json() == ended

        # a stopped run freezes the same way, and the stop itself moves
        # nothing either
        await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "speed": 100.0, "run_id": "second"},
        )
        await drive(pb, clock, steps=2)
        before_stop = (await client.get("/sim/clock")).json()
        stopped = (await client.post("/sim/playback", json={"action": "stop"})).json()
        assert stopped["state"] == "stopped"
        after_stop = (await client.get("/sim/clock")).json()
        assert moment(after_stop) == moment(before_stop)  # the stop moves nothing
        halted = after_stop
        await clock.advance(clock.now() + 1.0)
        assert (await client.get("/sim/clock")).json() == halted


async def test_the_clock_stays_at_the_stop_request_while_the_drain_runs(tmp_path: Path):
    """Review round 4, item 3: the clock shows how far the replay got,
    not how long the cleanup took. stop() freezes it at the moment the
    stop is accepted, so while the drain still waits for a held order -
    and with the wall clock running on - /sim/clock keeps showing that
    moment."""
    from test_playback_events import HoldingOrders

    clock = FakeWallClock()
    transport = HoldingOrders()
    orders = [make_order(14, 0, 0, 5, "1001"), make_order(14, 0, 0, 50, "1002")]
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        inflight=8,
        drain_timeout_s=2.0,  # the drain waits on the held order, not on its own timeout
        transport=transport,
    )
    from restaurantos.sim.control import make_control_app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        await client.post("/sim/playback", json={"action": "start", "from_day": 14, "speed": 100.0})
        await drive(pb, clock, steps=2)  # 10 simulated seconds in: order 1001 is out, held
        before_stop = (await client.get("/sim/clock")).json()
        stop_task = asyncio.create_task(pb.stop())
        await settle()  # the stop is accepted: the clock freezes here
        at_stop = (await client.get("/sim/clock")).json()
        assert at_stop["sim_day"] == before_stop["sim_day"]
        assert at_stop["sim_time"] == before_stop["sim_time"]
        # the drain is waiting for the held order; the wall clock moves
        # on, and the reported clock does not
        await clock.advance(clock.now() + 5.0)
        await settle()
        held_reading = (await client.get("/sim/clock")).json()
        assert held_reading == at_stop
        try:
            transport.release.set()
            await asyncio.wait_for(stop_task, 5)
        finally:
            transport.release.set()
    assert pb.status()["state"] == "stopped"
    final = pb.clock()
    assert final["sim_day"] == at_stop["sim_day"]
    assert final["sim_time"] == at_stop["sim_time"]


async def test_the_clock_stays_at_the_last_dispatch_through_the_final_wait(tmp_path: Path):
    """Review round 4, item 3: a normal end freezes the clock when the
    dispatcher dispatched its last item - the final wait for a held
    send, and the wall clock moving during it, never move the reported
    moment."""
    from conftest import settle
    from test_playback_events import HoldingEvent, expected_event_id

    hold = expected_event_id("1001", "picked_up")
    transport = HoldingEvent(hold)
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    # no jitter, so the last deadline is exactly +25 minutes; fine
    # steps so the fake clock lands on it without a coarse overshoot
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        status_events=True,
        event_jitter_s=0.0,
        transport=transport,
    )
    from restaurantos.sim.control import make_control_app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        await client.post("/sim/playback", json={"action": "start", "from_day": 14, "speed": 100.0})
        # speed 100 with 0.005 steps: the wake overshoot stays under a
        # simulated second, so the frozen reading is exactly the
        # deadline's own second
        for _ in range(3500):
            if pb.status()["events_sent"] == 4:
                break
            await clock.advance(clock.now() + 0.005)
            await settle()
        await settle(10)
        assert pb.status()["events_sent"] == 4
        assert pb.status()["state"] == "running"  # waiting for the held event
        at_end = (await client.get("/sim/clock")).json()
        # the last dispatched item is picked_up, at +25 simulated
        # minutes; the slice-wise wake may cross the deadline's second
        # by a step, so the frozen reading is the deadline's own second
        # or one past it - and never past that
        assert at_end["sim_day"] == 14
        assert at_end["sim_time"] in {"00:25:00", "00:25:01"}
        # the final wait runs with the wall clock moving on: the
        # reported clock does not
        await clock.advance(clock.now() + 5.0)
        await settle()
        assert (await client.get("/sim/clock")).json() == at_end
        try:
            transport.release.set()
            await asyncio.wait_for(pb.wait_done(), 5)
        finally:
            transport.release.set()
        assert pb.status()["state"] == "done"
    final = pb.clock()
    assert final["sim_day"] == at_end["sim_day"]
    assert final["sim_time"] == at_end["sim_time"]


async def test_only_stop_is_accepted_once_dispatch_has_ended(tmp_path: Path):
    """Review round 5, item 1: once the dispatcher dispatched its last
    item, the run only waits for the last answers - pause, resume and
    a speed change are refused (a resume would restart the clock that
    round 4 froze at the last item's instant), while stop still ends
    the run and the status keeps showing running until it does."""
    from conftest import settle
    from test_playback_events import HoldingEvent, expected_event_id

    hold = expected_event_id("1001", "picked_up")
    transport = HoldingEvent(hold)
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders, tmp_path, clock=clock, status_events=True, event_jitter_s=0.0, transport=transport
    )
    from restaurantos.sim.control import make_control_app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        await client.post("/sim/playback", json={"action": "start", "from_day": 14, "speed": 100.0})
        for _ in range(3500):
            if pb.status()["events_sent"] == 4:
                break
            await clock.advance(clock.now() + 0.005)
            await settle()
        await settle(10)
        assert pb.status()["state"] == "running"  # sitting in the final wait

        # pause, resume and a speed change are all refused, with the
        # message naming the state
        for body in ({"action": "pause"}, {"action": "resume"}):
            answer = await client.post("/sim/playback", json=body)
            assert answer.status_code == 409, answer.text
            assert answer.json()["error"] == "invalid_state"
            assert answer.json()["message"] == "dispatch has finished; waiting for the last answers"
        patched = await client.patch("/sim/playback", json={"speed": 200.0})
        assert patched.status_code == 409, patched.text
        assert patched.json()["error"] == "invalid_state"
        assert patched.json()["message"] == "dispatch has finished; waiting for the last answers"
        # the status still shows running, not some new state
        assert (await client.get("/sim/playback")).json()["state"] == "running"

        # the clock is frozen and stays frozen while the wait goes on
        frozen = (await client.get("/sim/clock")).json()
        await clock.advance(clock.now() + 5.0)
        await settle()
        assert (await client.get("/sim/clock")).json() == frozen

        # stop still works and ends the run as stopped
        stopped = await client.post("/sim/playback", json={"action": "stop"})
        assert stopped.status_code == 200
        try:
            transport.release.set()
            await asyncio.wait_for(pb.wait_done(), 5)
        finally:
            transport.release.set()
        assert pb.status()["state"] == "stopped"


async def test_speed_change_before_to_day_zero(client, pb):
    """A paused run can change speed: the anchor moves, the clock stays
    frozen, and simulated time never jumps when it resumes."""
    await client.post("/sim/playback", json={"action": "start", "from_day": 14})
    await client.post("/sim/playback", json={"action": "pause"})
    patched = (await client.patch("/sim/playback", json={"speed": 200.0})).json()
    assert patched["state"] == "paused"
    assert patched["speed"] == 200.0
    clock = (await client.get("/sim/clock")).json()
    assert clock["state"] == "paused"
    assert clock["sim_time"] == "00:00:00"

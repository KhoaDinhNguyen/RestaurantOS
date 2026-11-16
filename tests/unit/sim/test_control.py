"""The /sim control routes: the state machine, the status and clock
answers, and the 409/422 errors the architecture's contract implies."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from conftest import FakeWallClock, make_playback, sample_orders


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
    response = await client.post("/sim/playback", json={"action": "rewind", "from_day": 14})
    assert response.status_code == 422


async def test_full_cycle_through_the_routes(client, pb, tmp_path):
    start = (
        await client.post(
            "/sim/playback",
            json={"action": "start", "from_day": 14, "speed": 1000.0, "seed": 5},
        )
    ).json()
    assert start["state"] == "running"
    assert start["seed"] == 5

    # a start on a live run is a conflict, not a restart
    again = await client.post("/sim/playback", json={"action": "start", "from_day": 14})
    assert again.status_code == 409

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

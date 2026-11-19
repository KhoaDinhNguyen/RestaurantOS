# probe: does the normal-end freeze+seek land, and when?
import asyncio
from pathlib import Path

import httpx
from conftest import FakeWallClock, make_order, make_playback, settle
from test_playback_events import HoldingEvent, expected_event_id

from restaurantos.sim.control import make_control_app


async def main() -> None:
    tmp = Path("/tmp/probe_r5")
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    hold = expected_event_id("1001", "picked_up")
    transport = HoldingEvent(hold)
    orders = [make_order(14, 0, 0, 1, "1001")]
    clock = FakeWallClock()
    pb = make_playback(
        orders, tmp, clock=clock, status_events=True, event_jitter_s=0.0, transport=transport
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        await client.post("/sim/playback", json={"action": "start", "from_day": 14, "speed": 100.0})
        for _ in range(3500):
            if pb.status()["events_sent"] == 4:
                break
            await clock.advance(clock.now() + 0.005)
            await settle()
        print("dispatched 4; state:", pb.status()["state"])
        for i in range(6):
            await settle(10)
            reading = (await client.get("/sim/clock")).json()
            print(f"after settle round {i}: {reading['sim_time']}  ended={pb._dispatch_ended}")
        await clock.advance(clock.now() + 5.0)
        await settle()
        print("after wall advance:", (await client.get("/sim/clock")).json()["sim_time"])


asyncio.run(main())

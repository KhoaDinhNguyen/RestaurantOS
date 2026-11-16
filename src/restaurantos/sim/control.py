"""The playback slice of the `/sim` control-plane API (architecture
Section 6.3): the routes that observe and steer a live run.

The architecture puts this API on the control process, port 8002, so
that the UI (WP7) can drive playback and other tools can tell a run is
live (`rosctl baseline sync` refuses while playback is running by
reading `GET /sim/playback`). The WP6b control process does not exist
yet, so until it does, `rosctl playback start` hosts these routes in
the playback process itself, on 127.0.0.1 by default. When WP6b lands
it takes the port over and fronts (or proxies) this slice.

Known seam, accepted: two processes cannot both bind the same control
port. WP6b will resolve it; the port is a setting either way.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from restaurantos.sim.playback import Playback, StateError


class PlaybackCommand(BaseModel):
    """POST /sim/playback: the architecture's `{action, from_day,
    to_day, speed, seed, ...}` body (speed here is the run's initial
    speed; `PATCH /sim/playback` is the mid-run speed change)."""

    action: str
    from_day: int | None = Field(default=None, ge=0)
    to_day: int | None = Field(default=None, ge=0)
    speed: float | None = Field(default=None, gt=0)
    seed: int = 0
    run_id: str | None = None


class PlaybackSpeed(BaseModel):
    speed: float = Field(gt=0)


def make_control_app(playback: Playback) -> FastAPI:
    app = FastAPI(title="RestaurantOS playback control", docs_url="/docs")

    def _state_error(error: StateError) -> None:
        raise HTTPException(status_code=409, detail=str(error)) from error

    @app.get("/sim/clock")
    async def sim_clock() -> dict[str, Any]:
        return playback.clock()

    @app.get("/sim/playback")
    async def sim_playback_status() -> dict[str, Any]:
        return playback.status()

    @app.post("/sim/playback")
    async def sim_playback_command(command: PlaybackCommand) -> dict[str, Any]:
        try:
            if command.action == "start":
                if command.from_day is None:
                    raise HTTPException(
                        status_code=422,
                        detail="start needs from_day (and optionally to_day, speed, seed)",
                    )
                await playback.start(
                    seed=command.seed,
                    from_day=command.from_day,
                    to_day=command.to_day,
                    speed=command.speed or playback.default_speed,
                    run_id=command.run_id,
                )
            elif command.action == "pause":
                await playback.pause()
            elif command.action == "resume":
                await playback.resume()
            elif command.action == "stop":
                await playback.stop()
            else:
                raise HTTPException(
                    status_code=422,
                    detail=f"unknown action {command.action!r}; "
                    "expected start, pause, resume or stop",
                )
        except StateError as error:
            _state_error(error)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return playback.status()

    @app.patch("/sim/playback")
    async def sim_playback_speed(change: PlaybackSpeed) -> dict[str, Any]:
        try:
            await playback.set_speed(change.speed)
        except StateError as error:
            _state_error(error)
        return playback.status()

    return app

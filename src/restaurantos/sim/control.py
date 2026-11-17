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

from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from restaurantos.sim.playback import Playback, StateError


class PlaybackCommand(BaseModel):
    """POST /sim/playback: the architecture's `{action, from_day,
    to_day, speed, seed, ...}` body (speed here is the run's initial
    speed; `PATCH /sim/playback` is the mid-run speed change)."""

    action: Literal["start", "pause", "resume", "stop"]
    from_day: int | None = Field(default=None, ge=0)
    to_day: int | None = Field(default=None, ge=0)
    speed: float | None = Field(default=None, gt=0)
    seed: int = 0
    # one path component, no traversal; the engine checks it too
    run_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class PlaybackSpeed(BaseModel):
    speed: float = Field(gt=0)


def make_control_app(playback: Playback) -> FastAPI:
    app = FastAPI(title="RestaurantOS playback control", docs_url="/docs")

    @app.exception_handler(StateError)
    async def _invalid_state(request: Request, error: StateError) -> JSONResponse:
        """The project's error body, on both routes that can raise it:
        409 with what the run is in and what it refuses to do. The CLI
        prints `message`."""
        return JSONResponse(
            status_code=409,
            content={
                "error": "invalid_state",
                "state": playback.state,
                "message": str(error),
            },
        )

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
        except StateError:
            # StateError subclasses ValueError; left here it would be
            # swallowed by the 422 below. It goes to the app's handler:
            # the project's 409 body, not FastAPI's {"detail": ...}
            raise
        except ValueError as error:
            # a bad window or a bad run id: 422 with the message, like
            # the gateway's validation answers
            raise HTTPException(status_code=422, detail=str(error)) from error
        return playback.status()

    @app.patch("/sim/playback")
    async def sim_playback_speed(change: PlaybackSpeed) -> dict[str, Any]:
        await playback.set_speed(change.speed)
        return playback.status()

    return app

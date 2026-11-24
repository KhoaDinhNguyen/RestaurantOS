"""The playback slice of the `/sim` control-plane API (architecture
Section 6.3): the routes that observe and steer a live run.

The architecture puts this API on the control process, port 8002, so
that the UI (WP7) can drive playback and other tools can tell a run is
live (`rosctl baseline sync` refuses while playback is running by
reading `GET /sim/playback`). Two processes can host it: `rosctl
playback start` for one foreground run, or `rosctl control` (WP6b),
which embeds this same factory next to its links, faults, nodes and
jobs routes (`sim/control_api.py`) and hosts every run started through
`POST /sim/playback`. Only one of them can own the port; the other
reports the taken port at bind.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from restaurantos.sim.playback import Playback, StateError

# The control process's diagnostics go through logging, not a bare
# print to stderr (review round 3, item 9): the CLI process installs
# the stderr handler, and a handler that cannot write - a closed pipe
# - reports the problem itself instead of raising into a route that
# has already succeeded.
_LOGGER = logging.getLogger("restaurantos.sim.playback")


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
    # Duplicates and shuffle (architecture 6.3/7.2), per run like the
    # seed they derive from. The engine refuses shuffle in sequential
    # mode and a rate outside [0, 1]; pydantic answers 422 first here.
    duplicate_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    shuffle: bool = False


class PlaybackSpeed(BaseModel):
    speed: float = Field(gt=0)


def make_control_app(playback: Playback) -> FastAPI:
    app = FastAPI(title="RestaurantOS playback control", docs_url="/docs")
    # state-changing playback commands get the same cross-site rejection
    # as the control routes (the control process embeds both)
    from restaurantos.sim.control_api import add_origin_guard, configure_control_cors

    add_origin_guard(app)

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
                    duplicate_rate=command.duplicate_rate,
                    shuffle=command.shuffle,
                )
                status = playback.status()
                if (
                    command.to_day is not None
                    and status["to_day"] is not None
                    and command.to_day > status["to_day"]
                ):
                    # the window check shortened to_day to the last day
                    # the data holds; the answer already shows the
                    # effective day, so say so through the logger (the
                    # CLI process prints it to stderr)
                    _LOGGER.warning(
                        f"playback start: data only goes to day {status['to_day']}; "
                        f"running to day {status['to_day']} instead of {command.to_day}"
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

    # last: CORS must sit outside the origin guard, so its 403s carry
    # CORS headers and a listed browser client can read them. No-op when
    # add_control_routes already registered it (the flag guards it).
    configure_control_cors(app)

    return app

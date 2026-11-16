"""A stand-in for the gateway, so playback runs long before WP1 and
WP2 exist ("Playback can be developed against a stub HTTP server that
answers 201 to every order", the brief's "Run it on its own").

The stub speaks the POST /orders contract exactly: it parses the body
as `OrderRequest` (so every request playback sends is validated against
the real model) and answers `201` with a real `OrderResponse` shape.
That keeps playback's response handling identical against the stub and
the gateway - switching to the real system is a URL change, not a code
change - and it is what makes the throughput probe honest.

Everything the stub puts in the response is derived from the request,
never from a clock: `order_id` is `uuid5` of the channel and external
order id, and the three gateway timestamps are the order's own
simulated time. A stub that stamped wall-clock time would make the
determinism acceptance test compare noise; this one compares bytes.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from fastapi import FastAPI

from restaurantos.common.models import (
    OrderOutcome,
    OrderRequest,
    OrderResponse,
    OrderStatus,
)
from restaurantos.common.simclock import to_sim_seconds

# Wall-clock datetimes are only for measurement; the stub anchors them
# to a fixed epoch so its answers stay reproducible.
STUB_EPOCH = datetime(2020, 1, 1, tzinfo=UTC)


def make_stub_app(delay_seconds: float = 0.0) -> FastAPI:
    """The stub; `delay_seconds` adds artificial latency per request."""
    app = FastAPI(title="RestaurantOS order stub")
    app.state.delay_seconds = delay_seconds

    async def place_order(request: OrderRequest) -> OrderResponse:
        if app.state.delay_seconds:
            await asyncio.sleep(app.state.delay_seconds)
        moment = STUB_EPOCH + timedelta(
            seconds=to_sim_seconds(request.order_day, request.order_time)
        )
        return OrderResponse(
            outcome=OrderOutcome.ACCEPTED,
            order_id=uuid5(NAMESPACE_URL, f"{request.channel}:{request.external_order_id}"),
            order_day=request.order_day,
            status=OrderStatus.RECEIVED,
            region_id="r-stub",
            menu_version=1,
            received_at=moment,
            committed_at=moment,
            acked_at=moment,
        )

    # The gateway serves POST /orders behind the /api/v1 prefix
    # (architecture Section 6.1); both paths work so the stub is a
    # drop-in for any base URL.
    app.add_api_route("/orders", place_order, methods=["POST"], status_code=201)
    app.add_api_route("/api/v1/orders", place_order, methods=["POST"], status_code=201)

    @app.get("/sim/stub/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "delay_seconds": app.state.delay_seconds}

    return app


app = make_stub_app()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m restaurantos.sim.stub",
        description="Answer 201 to every order: the gateway stand-in for playback runs.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8900)
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=0.0,
        help="artificial per-request latency, for probing the in-flight bound",
    )
    args = parser.parse_args(argv)
    app.state.delay_seconds = args.delay_ms / 1000

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

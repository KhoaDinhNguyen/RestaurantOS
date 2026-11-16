"""Shared helpers for the playback tests.

The engine waits on a `WallClock`; here is a fake one whose time moves
only when the test advances it, so every send lands at an exact,
reproducible wall time and no test ever sleeps. The driver steps fake
time forward until the run is over, settling the event loop after each
step so the sends a step releases are acked and logged before the next
one.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import os
import sys
from datetime import time as dt_time

# importlib import mode does not put this directory on sys.path, and the
# sibling test files import the helpers below; insert it once, here.
sys.path.insert(0, os.path.dirname(__file__))

import httpx
import pytest

from restaurantos.common.models import OrderItemIn, OrderRequest
from restaurantos.sim.control import make_control_app
from restaurantos.sim.playback import Playback
from restaurantos.sim.stub import make_stub_app

FINAL_STATES = ("done", "stopped", "failed")


class FakeWallClock:
    """A WallClock that only advances when the test says so."""

    def __init__(self) -> None:
        self._now = 0.0
        self._sleepers: list[tuple[float, int, asyncio.Future]] = []
        self._tiebreak = itertools.count()

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self._now + seconds, next(self._tiebreak), future))
        await future

    async def advance(self, to: float) -> None:
        """Move to wall time `to`, waking sleepers with earlier deadlines.

        The epsilon absorbs float drift (0.05 + 0.25 is not exactly 0.3):
        a sleeper registered at 0.1 for 0.2 seconds must wake when the
        test advances to 0.3. The real clock self-corrects because a
        sleep never wakes early; this clock is the only authority, so
        the comparison carries the slack.
        """
        assert to >= self._now, "fake time only moves forward"
        self._now = to
        while self._sleepers and self._sleepers[0][0] <= to + 1e-9:
            _, _, future = heapq.heappop(self._sleepers)
            if not future.done():
                future.set_result(None)


async def settle(rounds: int = 8) -> None:
    """Give in-flight coroutines the loop, without any real waiting."""
    for _ in range(rounds):
        await asyncio.sleep(0)


async def drive(playback: Playback, clock: FakeWallClock, *, steps: int | None = None) -> int:
    """Advance fake time until the run is over (or `steps` steps taken).

    Returns the number of steps taken. Raises if the run never ends, so
    a scheduling bug fails the test instead of hanging it.
    """
    taken = 0
    limit = steps if steps is not None else 10_000
    while playback.state not in FINAL_STATES and taken < limit:
        await clock.advance(clock.now() + 0.05)
        await settle()
        taken += 1
    if steps is None:
        assert playback.state in FINAL_STATES, f"run never ended (state {playback.state})"
        assert taken < limit, "run never ended"
    return taken


def make_order(day: int, hh: int, mm: int, ss: int, oid: str, product: str = "p1") -> OrderRequest:
    return OrderRequest(
        channel="dhrd",
        external_order_id=oid,
        vendor_id="v001",
        customer_id="c000000001",
        customer_geohash="w21z7",
        order_day=day,
        order_time=dt_time(hh, mm, ss),
        items=[OrderItemIn(product_id=product, quantity=1)],
    )


def sample_orders(count: int = 40, *, day: int = 14) -> list[OrderRequest]:
    """`count` orders spread over seconds-after-midnight, in stream
    order, with triples that share a second (same-second orders must
    keep their sequence, whatever the in-flight interleaving does)."""
    orders: list[OrderRequest] = []
    for i in range(count):
        after_midnight = 5 * (i + 1)
        offset_day, rest = divmod(after_midnight, 86_400)
        hour, rest = divmod(rest, 3_600)
        minute, second = divmod(rest, 60)
        oid = str(1_000 + i)
        orders.append(make_order(day + offset_day, hour, minute, second, oid, product=f"p{i % 3}"))
        if i % 7 == 3:  # two more orders in the same simulated second
            orders.append(make_order(day + offset_day, hour, minute, second, oid + "a", "p0"))
            orders.append(make_order(day + offset_day, hour, minute, second, oid + "b", "p2"))
    return orders


def make_playback(
    orders: list[OrderRequest],
    runs_dir,
    *,
    clock=None,
    inflight: int = 8,
    speed: float = 1000.0,
    transport: httpx.AsyncBaseTransport | None = None,
    from_day: int = 14,
    drain_timeout_s: float = 5.0,
    window_check=None,
) -> Playback:
    """An engine over a fixed order list, posting to an in-process stub."""
    if transport is None:
        transport = httpx.ASGITransport(app=make_stub_app())
    return Playback(
        lambda a, b: iter(orders),
        target_url="http://stub.test/api/v1",
        inflight=inflight,
        http_timeout_s=5.0,
        drain_timeout_s=drain_timeout_s,
        runs_dir=runs_dir,
        default_speed=speed,
        wall=clock,
        transport=transport,
        window_check=window_check,
    )


@pytest.fixture
def fake_clock() -> FakeWallClock:
    return FakeWallClock()


@pytest.fixture
def control_client():
    """An httpx client wired to an in-process control app, as a factory:
    `client = control_client(playback)`."""

    def make(playback: Playback) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=make_control_app(playback)),
            base_url="http://control.test",
        )

    return make

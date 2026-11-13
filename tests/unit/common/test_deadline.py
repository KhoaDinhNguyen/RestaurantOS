"""The caller-side deadline, tested without a database.

A fake connection stands in for a link: `silence` makes every call on it wait
until `heal` is called, which is what a Toxiproxy `timeout` toxic does.
"""

import asyncio
import time
from contextlib import asynccontextmanager

import pytest

from restaurantos.common.deadline import abandoned_calls, run_with_deadline
from restaurantos.common.errors import RegionUnreachableError


class FakeLink:
    def __init__(self):
        self.open = asyncio.Event()
        self.open.set()
        self.checked_out = 0
        self.committed = 0
        self.rolled_back = 0

    def silence(self):
        self.open.clear()

    def heal(self):
        self.open.set()

    @asynccontextmanager
    async def connection(self):
        self.checked_out += 1
        try:
            yield self
        except BaseException:
            self.rolled_back += 1
            raise
        else:
            self.committed += 1
        finally:
            self.checked_out -= 1

    async def query(self, value):
        # A stuck psycopg call does not stop when it is cancelled: it waits for
        # the server, over the same link. Model that by ignoring cancellation
        # until the link is back.
        while not self.open.is_set():
            try:
                await self.open.wait()
            except asyncio.CancelledError:
                continue
        return value


def run(coro):
    return asyncio.run(coro)


def test_a_healthy_call_returns_its_value_and_commits():
    async def scenario():
        link = FakeLink()
        result = await run_with_deadline(link.connection, lambda c: c.query(7), 1.0)
        return result, link

    result, link = run(scenario())
    assert result == 7
    assert (link.committed, link.rolled_back, link.checked_out) == (1, 0, 0)


def test_an_error_from_the_work_comes_through_and_rolls_back():
    async def boom(conn):
        raise ValueError("no such vendor")

    async def scenario():
        link = FakeLink()
        with pytest.raises(ValueError, match="no such vendor"):
            await run_with_deadline(link.connection, boom, 1.0)
        return link

    link = run(scenario())
    assert (link.committed, link.rolled_back, link.checked_out) == (0, 1, 0)


def test_a_silent_link_fails_at_the_deadline_and_cleans_up_after_it_heals():
    async def scenario():
        link = FakeLink()
        link.silence()
        started = time.monotonic()
        with pytest.raises(RegionUnreachableError) as caught:
            await run_with_deadline(link.connection, lambda c: c.query(7), 0.05, region_id="r1")
        waited = time.monotonic() - started

        # Given up on, but still holding its connection in the background.
        during = (abandoned_calls(), link.checked_out)

        link.heal()
        for _ in range(100):
            if abandoned_calls() == 0:
                break
            await asyncio.sleep(0.01)
        return caught.value, waited, during, (abandoned_calls(), link.checked_out)

    error, waited, during, after = run(scenario())
    assert error.region_id == "r1"
    assert "r1" in str(error)
    assert waited < 0.5
    assert during == (1, 1)
    assert after == (0, 0)


def test_a_slow_link_counts_as_unreachable_too():
    async def slow(conn):
        await asyncio.sleep(0.5)
        return 7

    async def scenario():
        link = FakeLink()
        with pytest.raises(RegionUnreachableError):
            await run_with_deadline(link.connection, slow, 0.05)
        await asyncio.sleep(0.05)
        return link

    link = run(scenario())
    # An ordinary await does stop when cancelled: nothing is left behind.
    assert abandoned_calls() == 0
    assert (link.committed, link.rolled_back, link.checked_out) == (0, 1, 0)


def test_a_cancelled_caller_does_not_wait_for_the_stuck_call():
    async def scenario():
        link = FakeLink()
        link.silence()
        caller = asyncio.create_task(run_with_deadline(link.connection, lambda c: c.query(7), 10.0))
        await asyncio.sleep(0.01)
        caller.cancel()
        started = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await caller
        waited = time.monotonic() - started
        link.heal()
        await asyncio.sleep(0.05)
        return waited, link

    waited, link = run(scenario())
    assert waited < 0.5
    assert abandoned_calls() == 0
    assert link.checked_out == 0

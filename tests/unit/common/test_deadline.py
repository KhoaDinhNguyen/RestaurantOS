"""The caller-side deadline, tested without a database.

A fake connection stands in for a link: `silence` makes every call on it wait
until `heal` is called, which is what a Toxiproxy `timeout` toxic does. The
fake also counts every cancellation it receives. The real driver reacts to a
cancellation by blocking the whole process on a dead link, so that count must
stay at zero.
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
        self.cancellations = 0

    def silence(self):
        self.open.clear()

    def heal(self):
        self.open.set()

    @asynccontextmanager
    async def connection(self):
        self.checked_out += 1
        try:
            yield self
        except asyncio.CancelledError:
            self.cancellations += 1
            raise
        except BaseException:
            self.rolled_back += 1
            raise
        else:
            self.committed += 1
        finally:
            self.checked_out -= 1

    async def query(self, value):
        try:
            await self.open.wait()
        except asyncio.CancelledError:
            self.cancellations += 1
            raise
        return value

    def state(self):
        return (self.committed, self.rolled_back, self.checked_out)


def run(coro):
    return asyncio.run(coro)


async def drained(timeout=2.0):
    until = time.monotonic() + timeout
    while abandoned_calls() and time.monotonic() < until:
        await asyncio.sleep(0.01)
    return abandoned_calls() == 0


def test_a_healthy_call_returns_its_value_and_commits():
    async def scenario():
        link = FakeLink()
        result = await run_with_deadline(link.connection, lambda c: c.query(7), 1.0)
        return result, link

    result, link = run(scenario())
    assert result == 7
    assert link.state() == (1, 0, 0)


def test_an_error_from_the_work_comes_through_and_rolls_back():
    async def boom(conn):
        raise ValueError("no such vendor")

    async def scenario():
        link = FakeLink()
        with pytest.raises(ValueError, match="no such vendor"):
            await run_with_deadline(link.connection, boom, 1.0)
        return link

    link = run(scenario())
    assert link.state() == (0, 1, 0)


def test_a_silent_link_fails_at_the_deadline_and_is_never_cancelled():
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
        assert await drained()
        return caught.value, waited, during, link

    error, waited, during, link = run(scenario())
    assert error.region_id == "r1"
    assert "r1" in str(error)
    assert waited < 0.5
    assert during == (1, 1)
    # Its work finished after the fault, and was rolled back, not committed.
    assert link.state() == (0, 1, 0)
    assert link.cancellations == 0


def test_every_caller_during_a_fault_is_bounded():
    async def scenario():
        link = FakeLink()
        link.silence()
        waits = []
        for _ in range(3):
            started = time.monotonic()
            with pytest.raises(RegionUnreachableError):
                await run_with_deadline(link.connection, lambda c: c.query(7), 0.05)
            waits.append(time.monotonic() - started)
        during = abandoned_calls()
        link.heal()
        assert await drained()
        return waits, during, link

    waits, during, link = run(scenario())
    assert max(waits) < 0.5
    assert during == 3
    assert link.state() == (0, 3, 0)
    assert link.cancellations == 0


def test_a_slow_link_counts_as_unreachable_and_its_late_answer_is_rolled_back():
    async def slow(conn):
        await asyncio.sleep(0.2)
        return 7

    async def scenario():
        link = FakeLink()
        with pytest.raises(RegionUnreachableError):
            await run_with_deadline(link.connection, slow, 0.05)
        during = abandoned_calls()
        assert await drained()
        return during, link

    during, link = run(scenario())
    assert during == 1
    assert link.state() == (0, 1, 0)
    assert link.cancellations == 0


def test_a_cancelled_caller_does_not_wait_and_does_not_pass_the_cancellation_on():
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
        during = abandoned_calls()
        link.heal()
        assert await drained()
        return waited, during, link

    waited, during, link = run(scenario())
    assert waited < 0.5
    assert during == 1
    assert link.state() == (0, 1, 0)
    assert link.cancellations == 0

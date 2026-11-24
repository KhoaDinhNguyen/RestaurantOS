"""Shutdown wakes an interval wait without cancelling an active refresh."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from restaurantos.gateway.app import (
    refresh_directory_periodically,
    refresh_edge_primary_periodically,
)


class WaitingEvent(asyncio.Event):
    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()

    async def wait(self):
        self.entered.set()
        return await super().wait()


def loop_for(kind, refresh, interval, stop):
    owner = SimpleNamespace(refresh=refresh)
    if kind == "directory":
        return refresh_directory_periodically(owner, object(), interval, stop_event=stop)
    return refresh_edge_primary_periodically(owner, interval, stop_event=stop)


@pytest.mark.parametrize("kind", ["directory", "edge"])
async def test_stop_wakes_long_interval_without_refresh(kind):
    stop, refresh = WaitingEvent(), AsyncMock()
    task = asyncio.create_task(loop_for(kind, refresh, 300, stop))
    await stop.entered.wait()
    stop.set()
    done, _ = await asyncio.wait({task}, timeout=1)
    assert task in done, "shutdown waited for the 300-second refresh interval"
    await task
    refresh.assert_not_awaited()


@pytest.mark.parametrize("kind", ["directory", "edge"])
async def test_interval_expiry_runs_refresh(kind):
    stop = asyncio.Event()
    refresh = AsyncMock(side_effect=lambda *args: stop.set())
    await loop_for(kind, refresh, 0.001, stop)
    refresh.assert_awaited_once()


@pytest.mark.parametrize("kind", ["directory", "edge"])
async def test_active_refresh_finishes_without_cancellation(kind):
    stop, entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def refresh(*args):
        entered.set()
        await release.wait()

    task = asyncio.create_task(loop_for(kind, refresh, 0.001, stop))
    await entered.wait()
    stop.set()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    await task
    assert not task.cancelled()

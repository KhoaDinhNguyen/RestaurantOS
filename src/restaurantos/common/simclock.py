"""Simulated (business) time.

The dataset has no calendar dates, only order_day (0..90) and order_time.
Simulated time is counted in whole seconds since day 0, 00:00:00.

Nothing here reads the wall clock to decide what "today" is. SimClock only
maps wall-clock time to simulated time for playback, at a speed that can
change while it runs.
"""

from __future__ import annotations

import time as _time
from collections.abc import Callable
from datetime import time

SECONDS_PER_DAY = 86_400


def to_sim_seconds(order_day: int, order_time: time) -> int:
    return (
        order_day * SECONDS_PER_DAY
        + order_time.hour * 3600
        + order_time.minute * 60
        + order_time.second
    )


def from_sim_seconds(sim_seconds: int) -> tuple[int, time]:
    if sim_seconds < 0:
        raise ValueError("simulated time cannot be negative")
    day, rest = divmod(int(sim_seconds), SECONDS_PER_DAY)
    hour, rest = divmod(rest, 3600)
    minute, second = divmod(rest, 60)
    return day, time(hour, minute, second)


class SimClock:
    """Maps wall-clock time to simulated time.

    It keeps an anchor (a wall-clock instant and the simulated time at that
    instant). Changing speed, pausing or resuming moves the anchor to "now",
    so simulated time never jumps.
    """

    def __init__(
        self,
        start_sim_seconds: float = 0.0,
        speed: float = 1000.0,
        *,
        wall: Callable[[], float] = _time.monotonic,
        running: bool = False,
    ) -> None:
        if speed <= 0:
            raise ValueError("speed must be positive")
        self._wall = wall
        self._speed = speed
        self._anchor_wall = wall()
        self._anchor_sim = float(start_sim_seconds)
        self._running = running

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def running(self) -> bool:
        return self._running

    def now(self) -> float:
        """Current simulated time in seconds."""
        if not self._running:
            return self._anchor_sim
        return self._anchor_sim + (self._wall() - self._anchor_wall) * self._speed

    def _reanchor(self) -> None:
        self._anchor_sim = self.now()
        self._anchor_wall = self._wall()

    def start(self) -> None:
        self._reanchor()
        self._running = True

    def pause(self) -> None:
        self._reanchor()
        self._running = False

    def set_speed(self, speed: float) -> None:
        if speed <= 0:
            raise ValueError("speed must be positive")
        self._reanchor()
        self._speed = speed

    def seek(self, sim_seconds: float) -> None:
        self._anchor_sim = float(sim_seconds)
        self._anchor_wall = self._wall()

    def wall_seconds_until(self, sim_seconds: float) -> float:
        """Wall-clock seconds to wait until the given simulated time (0 if past).

        Only valid until the next speed change: callers should wait in short
        slices and ask again rather than sleeping for the whole value.
        """
        return max(0.0, (sim_seconds - self.now()) / self._speed)

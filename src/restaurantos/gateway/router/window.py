"""Shared day and time-window semantics for distributed queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time


@dataclass(frozen=True)
class QueryWindow:
    """Normalized day range and optional repeating time window."""

    from_day: int
    to_day: int
    from_time: time | None = None
    to_time: time | None = None

    @property
    def has_time_window(self) -> bool:
        return self.from_time is not None

    @property
    def crosses_midnight(self) -> bool:
        return (
            self.from_time is not None
            and self.to_time is not None
            and self.from_time > self.to_time
        )


def normalize_window(
    *,
    day: int | None,
    from_day: int | None,
    to_day: int | None,
    from_time: time | None,
    to_time: time | None,
) -> QueryWindow:
    """Validate and normalize the architecture's query-window parameters."""

    if day is not None:
        if from_day is not None or to_day is not None:
            raise ValueError("day cannot be combined with from_day or to_day")

        start_day = day
        end_day = day
    else:
        if from_day is None:
            raise ValueError("day or from_day is required")

        start_day = from_day
        end_day = from_day if to_day is None else to_day

    if start_day < 0 or end_day < 0:
        raise ValueError("days must be non-negative")

    if end_day < start_day:
        raise ValueError("to_day must be greater than or equal to from_day")

    if (from_time is None) != (to_time is None):
        raise ValueError("from_time and to_time must be provided together")

    if (
        from_time is not None
        and to_time is not None
        and (from_time.utcoffset() is not None or to_time.utcoffset() is not None)
    ):
        raise ValueError("from_time and to_time must not include a UTC offset")

    return QueryWindow(
        from_day=start_day,
        to_day=end_day,
        from_time=from_time,
        to_time=to_time,
    )

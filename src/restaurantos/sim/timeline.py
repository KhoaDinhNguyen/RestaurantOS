"""The kitchen timeline shared by the engine and the settings.

`EVENT_SCHEDULE` is the brief's schedule of an accepted order's status
events; `MIN_EVENT_GAP_S` is the smallest base gap between two of them.
Both live here - a module with no dependency past common.models - so
the settings can bound the jitter without importing the engine (and,
with it, httpx and the rest of the playback stack; replaydata avoids
the same import for the same reason).
"""

from __future__ import annotations

from restaurantos.common.models import OrderStatus

# The kitchen timeline: (to_status, seconds after the order).
EVENT_SCHEDULE: tuple[tuple[OrderStatus, int], ...] = (
    (OrderStatus.ACCEPTED, 60),
    (OrderStatus.PREPARING, 180),
    (OrderStatus.READY, 900),
    (OrderStatus.PICKED_UP, 1500),
)

# The jitter bound, derived from EVENT_SCHEDULE, never written a second
# place. J may be at most the smallest gap between two events of one
# order (120 s). Draws are in [0, J), so an event is never due before
# its predecessor.
MIN_EVENT_GAP_S = min(
    later - earlier
    for (_, earlier), (_, later) in zip(EVENT_SCHEDULE, EVENT_SCHEDULE[1:], strict=False)
)

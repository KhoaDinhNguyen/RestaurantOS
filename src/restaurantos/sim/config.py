"""WP6a's own settings, with the ROS_SIM_ prefix (AGENTS.md rule 8).

The stock parameters are documented in docs/replay-stock-parameters.md:
why a warm-up window, and what W, C and F mean. R (restock interval)
drives playback's restock events, so it lives here with them.
"""

from __future__ import annotations

from functools import cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from restaurantos.sim.playback import MIN_EVENT_GAP_S


class SimSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ROS_SIM_", env_file=".env", extra="ignore")

    # Stock: measure usage on the first W days only; start every counter at
    # max(F, ceil(average daily usage x C)), which is also the par level.
    # The constraints are part of the contract: W=0 would mean "no warm-up
    # window", and bootstrap builds the settings before it destroys
    # anything, so a bad value fails before the volumes are deleted.
    stock_warm_up_days: int = Field(default=14, gt=0)
    stock_cover_days: int = Field(default=7, ge=0)
    stock_floor: int = Field(default=5, ge=0)
    # Restock back to the par level every R days of replay
    # (docs/replay-stock-parameters.md, rule 4): at 00:00:00 of days
    # W+R, W+2R, ... playback restocks every vendor.
    stock_restock_days: int = Field(default=7, gt=0)

    # Playback: the replay defaults. The speed is the architecture's
    # documented default (Section 5.4); inflight bounds the requests a
    # run keeps open at once; drain bounds how long `stop` waits for
    # them before abandoning (the log marks abandoned lines).
    playback_speed: float = Field(default=1000.0, gt=0)
    playback_inflight: int = Field(default=64, ge=1)
    playback_drain_timeout_s: float = Field(default=30.0, gt=0)
    # Status events (architecture 6.1's POST /vendors/{vendor_id}/orders/
    # {order_id}/status): accepted, preparing, ready, picked_up at
    # +1, +3, +15, +25 simulated minutes after the order, plus jitter.
    # On by default; a sample rate below 1 gives a deterministic subset
    # of the orders their full event set (an order is in or out, never
    # half). Jitter is uniform in [0, playback_event_jitter_s) simulated
    # seconds per event, drawn per (seed, order, event) - never a shared
    # generator. The bound is capped at the smallest gap between two
    # events of one order (120 s, +1 min to +3 min, derived from the
    # engine's EVENT_SCHEDULE): jitter is drawn from [0, J), so at or
    # above the gap it could reorder the events, and the region would
    # answer conflicts.
    playback_status_events: bool = True
    playback_event_sample_rate: float = Field(default=1.0, ge=0.0, le=1.0)
    playback_event_jitter_s: float = Field(default=60.0, ge=0.0, le=float(MIN_EVENT_GAP_S))
    # Where the region edge APIs listen. Playback routes the kitchen-side
    # calls (status events, restocks) to each region's own edge API -
    # port 8010 + n on this host - because the kitchen is on the region
    # side and the edge survives a gateway cut (architecture 7.2, 4).
    playback_edge_host: str = "127.0.0.1"
    # The gateway's API base, as seen from the machine rosctl runs on:
    # POST goes to {target}/orders. Point it at the stub
    # (http://127.0.0.1:8900) until the gateway exists.
    playback_target_url: str = "http://127.0.0.1:8000/api/v1"
    http_timeout_s: float = Field(default=5.0, gt=0)
    # Where the /sim control routes listen while a playback run is live
    # (architecture Section 6.3 puts them on the control process, port
    # 8002; until WP6b exists the playback process hosts them).
    control_host: str = "127.0.0.1"
    control_port: int = Field(default=8002, ge=1, le=65535)


@cache
def get_sim_settings() -> SimSettings:
    return SimSettings()

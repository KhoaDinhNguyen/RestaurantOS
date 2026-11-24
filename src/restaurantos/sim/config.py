"""WP6a's own settings, with the ROS_SIM_ prefix (AGENTS.md rule 8).

The stock parameters are documented in docs/replay-stock-parameters.md:
why a warm-up window, and what W, C and F mean. R (restock interval)
drives playback's restock events, so it lives here with them.
"""

from __future__ import annotations

import ipaddress
from functools import cache
from urllib.parse import urlsplit

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from restaurantos.sim.timeline import MIN_EVENT_GAP_S


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
    # generator. The bound: J may be at most the smallest gap between
    # two events of one order (120 s, +1 min to +3 min, derived from
    # the engine's EVENT_SCHEDULE). Draws are in [0, J), so an event is
    # never due before its predecessor.
    playback_status_events: bool = True
    playback_event_sample_rate: float = Field(default=1.0, ge=0.0, le=1.0)
    playback_event_jitter_s: float = Field(default=60.0, ge=0.0, le=float(MIN_EVENT_GAP_S))
    # Kitchen calls (status events, restocks) retry a timeout, a
    # connection error or a 5xx: the edge does not cancel a call that
    # missed its deadline, so it may have committed, and giving up
    # after one attempt would send the order's next event into an
    # impossible move. Every attempt sends the same event_id, so a
    # repeat answers duplicate and is counted as acked. A 2xx, 404,
    # 409 or 422 is never retried.
    playback_kitchen_attempts: int = Field(default=3, ge=1)
    # Duplicates and shuffle (architecture 7.2), one block: an order is
    # resent when stable_float(seed, order_id, "duplicate") is below
    # duplicate_rate - a hash threshold, not a quota: the same seed
    # always resends the same orders, and the resent share comes close
    # to the rate over a long stream but is not guaranteed to equal it
    # (a short stream can be well off). The resend
    # is the same request body, so the region's inbox answers duplicate
    # and the idempotency is exercised; it lands at the order's own
    # instant plus a hash-drawn offset in [0, duplicate_offset_s)
    # simulated seconds - a small bound keeps it near-immediate.
    # Shuffle reorders delivery in bounded batches: the next
    # shuffle_window stream items are held as one batch and delivered
    # in shuffle-key order before the next batch is pulled, so an
    # item moves at most shuffle_window-1 places; a window of 1 is
    # no shuffle at all. Never on together with sequential mode
    # (in-order, one-at-a-time delivery is what the oracle matches).
    playback_duplicate_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    playback_duplicate_offset_s: float = Field(default=1.0, ge=0.0)
    playback_shuffle: bool = False
    shuffle_window: int = Field(default=8, ge=1)
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
    # Where the /sim control routes listen (architecture Section 6.3):
    # the control process (`rosctl control`, port 8002), or a standalone
    # `rosctl playback start` while its run is live.
    control_host: str = "127.0.0.1"
    control_port: int = Field(default=8002, ge=1, le=65535)
    # Exact origins (scheme://host[:port], comma-separated) allowed to
    # call the control API from a browser: they get CORS headers and
    # pass the origin guard. CLI clients (rosctl, curl) send no Origin
    # and need nothing. Empty means CLI-only. Entries are validated by
    # parse_allowed_origins: an invalid one stops the process at startup.
    control_allowed_origins: str = ""


_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalise_origin(raw: str) -> str:
    """The canonical form of an origin: lowercase scheme and host, no
    trailing slash, no default port. This is exactly the string a
    browser sends in an Origin header, so both the CORS middleware and
    the origin guard can compare origins as plain strings."""
    parsed = urlsplit(raw.strip().rstrip("/"))
    if (
        parsed.scheme not in _DEFAULT_PORTS
        or not parsed.hostname
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise ValueError(f"invalid origin: {raw!r}")
    host = parsed.hostname  # urlsplit already lower-cases it
    if ":" in host:
        # an IPv6 literal: canonicalise the way browsers write it, so an
        # expanded entry matches what the browser actually sends
        try:
            addr = ipaddress.IPv6Address(host)
        except ValueError:
            raise ValueError(f"invalid origin: {raw!r}") from None
        if addr.scope_id is not None or addr.ipv4_mapped is not None:
            # browsers never send zone ids in Origin; IPv4-mapped forms
            # serialise differently in Python (::ffff:127.0.0.1) and
            # browsers (::ffff:7f00:1), so they can never reliably match
            raise ValueError(f"unsupported IPv6 origin: {raw!r}") from None
        host = f"[{addr.compressed}]"
    port = parsed.port
    if port is None or port == _DEFAULT_PORTS[parsed.scheme]:
        return f"{parsed.scheme}://{host}"
    return f"{parsed.scheme}://{host}:{port}"


def parse_allowed_origins(value: str) -> tuple[str, ...]:
    """The allowlist setting as normalised origins (deduped, order kept).

    Raises ValueError on an invalid entry - so a process that builds a
    control app fails at startup instead of silently ignoring part of
    its configuration."""
    items = [item for item in (entry.strip() for entry in value.split(",")) if item]
    return tuple(dict.fromkeys(normalise_origin(item) for item in items))


@cache
def get_sim_settings() -> SimSettings:
    return SimSettings()

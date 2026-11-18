"""The playback engine: stream the sorted orders, send each at its
simulated instant, log what came back.

Scheduling follows the brief: an order t simulated seconds after the
start goes out t/speed wall-clock seconds after the start; send times
are never precomputed. `common.simclock.SimClock` holds the
(wall-clock, simulated) anchor pair and re-anchors on every speed
change and on pause/resume, so simulated time never jumps.

Besides the order stream, one dispatcher merges two more timelines
(the extras): each accepted order's four status events at +1, +3, +15
and +25 simulated minutes plus hash-based jitter, and a restock for
every vendor at 00:00:00 of each restock day, before that day's first
order. An event's simulated deadline is fixed when its order is
dispatched - it depends on nothing but the order's simulated time, the
seed and the settings - but the event only goes out once the order's
outcome is known, because it needs the order id the response carries
and a refused order must not move through any kitchen. The dispatcher
therefore waits for that outcome at the event's instant; the outcome
always resolves (every send finishes, errors, or is cancelled by the
drain), so the wait cannot hang, and it bounds how far the engine runs
ahead of its target to roughly the first event offset - a self-limit
that keeps the run target-bound rather than schedule-bound.

Three properties the acceptance test depends on:

- Dispatch order is a pure function of the inputs: the stream, the
  simulated deadlines and the deterministic jitter, ordered by
  (simulated time, kind, position in the stream). Orders in the same
  second go out in the same sequence, restocks go before the first
  order of their day, and the in-flight bound is a semaphore acquired
  in dispatch order (asyncio semaphores are FIFO), so it delays but
  never reorders.
- The run log is written in dispatch order whatever order the
  responses arrive in: each line carries its send sequence number, and
  a small reorder buffer holds a completed line until every earlier
  line is written. Byte-identical logs across runs therefore do not
  depend on how acks interleave.
- Every random value comes from
  `determinism.stable_float(seed, id, purpose)` and every event id is
  a uuid5 of the order and the status: no shared generator, nothing
  drawn in send order, nothing from a clock.

Wall time enters through a `WallClock` (protocol) so tests can drive
the scheduler deterministically: the real one wraps `time.monotonic`
and `asyncio.sleep`; a fake one resolves sleeps only when the test
advances it.

The run log's wall times (`wall_sent_at`, `wall_acked_at`) come from
that clock: seconds since the run started, on the same monotonic scale
the engine waited on. To compare them with the gateway's absolute
timestamps: epoch time = meta["wall_start_epoch"] +
(t - meta["wall_start_monotonic"]), both recorded by the same run.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import json
import re
import sys
import time
import traceback
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

import httpx

from restaurantos.common.models import OrderRequest, OrderStatus
from restaurantos.common.simclock import SECONDS_PER_DAY, SimClock, from_sim_seconds, to_sim_seconds
from restaurantos.sim.determinism import stable_float
from restaurantos.sim.timeline import EVENT_SCHEDULE, MIN_EVENT_GAP_S

# Re-check the clock at most this often during a long wait, so a speed
# change is picked up within a slice instead of at the far deadline.
WAIT_SLICE_S = 0.2

# A run id names runs/<run_id>/; it comes from the CLI, the API, or a
# default. One path component, no traversal.
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

IDLE = "idle"
RUNNING = "running"
PAUSED = "paused"
STOPPING = "stopping"
DONE = "done"
STOPPED = "stopped"
FAILED = "failed"

LIVE_STATES = frozenset({RUNNING, PAUSED, STOPPING})
FINAL_STATES = frozenset({IDLE, DONE, STOPPED, FAILED})  # states a start is legal from

# The response fields the run log passes through (the gateway's answer
# to POST /orders, OrderResponse in common/models.py).
RESPONSE_FIELDS = (
    "outcome",
    "duplicate",
    "queued",
    "order_id",
    "region_id",
    "menu_version",
    "received_at",
    "committed_at",
    "acked_at",
    "refusal_reason",
)

# ...and the ones of a status change (StatusResult in common/models.py).
EVENT_RESPONSE_FIELDS = ("applied", "duplicate", "conflict")

# The kitchen timeline (EVENT_SCHEDULE) and the jitter bound
# (MIN_EVENT_GAP_S) live in sim/timeline.py, shared with the settings.

# Wall-time backoff between kitchen-call attempts (review round 3,
# item 1): 0.5 s after the first attempt, then 1 s, then 2 s for every
# attempt after that. Wall time, not simulated time - a retry is about
# the target being unreachable for a moment (a failover, a blip), not
# about the replay's clock, and it must not shift the schedule.
_KITCHEN_BACKOFF_S = (0.5, 1.0, 2.0)

# Dispatch order within one simulated instant: restocks first (they
# must land before the day's first order), then orders, then status
# events.
RANK_RESTOCK, RANK_ORDER, RANK_EVENT = 0, 1, 2


def restock_days(warm_up_days: int, restock_every: int, from_day: int, to_day: int) -> list[int]:
    """Days W+R, W+2R, ... inside one run's replay window.

    The first restock is a full interval after the warm-up window
    (bootstrap set every counter to par on day W); which of the
    interval days a run contains depends on that run's window, so the
    list is computed per run, in `start`, never per engine.
    """
    if restock_every <= 0:
        raise ValueError("the restock interval must be positive")
    first = warm_up_days + restock_every
    return [day for day in range(first, to_day + 1, restock_every) if day >= from_day]


@dataclass(frozen=True)
class _OrderOutcome:
    """What the gateway answered, boiled down to what an event needs.

    `region_id` is where the order landed; an event only ever goes out
    for an accepted order, so the region is always known, and the
    event routes to that region's own edge API."""

    accepted: bool
    order_id: str | None
    reason: str
    region_id: str | None = None


class _OrderHandle:
    """The per-order link between the send and its scheduled events."""

    __slots__ = ("future", "last_event", "order_seq", "outcome", "request", "sim_time")

    def __init__(
        self,
        future: asyncio.Future,
        request: OrderRequest,
        sim_time: float,
        order_seq: int,
    ) -> None:
        self.future = future
        self.request = request
        self.sim_time = sim_time  # the order's simulated instant, absolute
        self.order_seq = order_seq
        self.outcome: _OrderOutcome | None = None
        # the send task of this order's previous event: the kitchen
        # events of one order go out strictly one after another
        self.last_event: asyncio.Task | None = None

    def resolve(self, outcome: _OrderOutcome) -> None:
        self.outcome = outcome
        if not self.future.done():
            self.future.set_result(None)


class StateError(ValueError):
    """A control operation the current run state does not allow."""


class WallClock(Protocol):
    """The wall clock the scheduler waits on."""

    def now(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class RealWallClock:
    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class RunLog:
    """`orders.jsonl`, one line per order, written in send order.

    `exclusive=True` opens with "x": creating the log *is* the run-id
    reservation, atomic across processes, instead of a check-then-open
    pair two processes can both pass.
    """

    def __init__(self, path: Path, *, exclusive: bool = False) -> None:
        self._f = path.open("x" if exclusive else "w", encoding="utf-8")
        self._next = 0
        self._pending: dict[int, dict[str, Any]] = {}

    def add(self, seq: int, record: dict[str, Any]) -> None:
        self._pending[seq] = record

    def flush(self) -> None:
        while self._next in self._pending:
            record = self._pending.pop(self._next)
            self._f.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._next += 1
        # push the buffer out too: the run log should survive a hard kill
        self._f.flush()

    def close(self) -> None:
        # Drain guarantees every live send task added its record; a gap
        # here is a bug. Write a placeholder line for the missing send
        # so the gap is visible in the file, and leave the records that
        # did complete unchanged. The placeholder carries the same
        # `kind` every line has - `missing`, for a send that produced
        # no result at all.
        while self._pending:
            record = self._pending.pop(self._next, None)
            if record is None:
                record = {
                    "kind": "missing",
                    "seq": self._next,
                    "error": "missing: the send task produced no result",
                }
            self._f.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._next += 1
        self._f.close()


class Playback:
    """One run, or the idle slot waiting for one.

    All methods run on the event loop the engine was started on; the
    control routes and the CLI call them from there.

    The extras (status events, restock) are opt-in here: the engine's
    defaults keep a run order-only, while the CLI turns them on from
    the settings (`ROS_SIM_PLAYBACK_STATUS_EVENTS` is on by default).
    `restock_every` is R, the restock interval in days (`None`: no
    restock); the restock days are computed per run, from `warm_up_days`
    (W) and the run's own window, so two runs on one engine never see
    each other's restock days.

    Known seam, deliberately out of scope here (it belongs with the
    snapshot work, which needs "pause and wait for in-flight" anyway):
    the number of send tasks queued for a slot is not bounded
    separately from the in-flight bound, and pause does not recall
    sends already queued for a slot. Both only show when the target is
    slower than the due rate. Status events multiply the *scheduled*
    items about fivefold per order, but those are heap entries, not
    tasks: a task only exists between its dispatch and its ack, and
    the outcome wait bounds the dispatch lead, so the seam keeps its
    shape (a same-instant burst still queues a burst of tasks; events
    add nothing to it).
    """

    def __init__(
        self,
        orders_fn: Callable[[int, int | None], Iterator[OrderRequest]],
        *,
        target_url: str,
        inflight: int,
        http_timeout_s: float,
        drain_timeout_s: float,
        runs_dir: Path,
        default_speed: float = 1000.0,
        wall: WallClock | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        window_check: Callable[[int, int | None], tuple[int, int | None]] | None = None,
        status_events: bool = False,
        event_sample_rate: float = 1.0,
        event_jitter_s: float = 60.0,
        kitchen_attempts: int = 3,
        restock_every: int | None = None,
        warm_up_days: int = 0,
        restock_vendors: Sequence[str] = (),
        kitchen_url: str | None = None,
        kitchen_urls: Mapping[str, str] | None = None,
        vendor_regions: Mapping[str, str] | None = None,
    ) -> None:
        self._orders_fn = orders_fn
        self._api_base = target_url.rstrip("/")
        self._orders_url = self._api_base + "/orders"
        self._inflight = inflight
        self._http_timeout_s = http_timeout_s
        self._drain_timeout_s = drain_timeout_s
        self._runs_dir = runs_dir
        self.default_speed = default_speed
        self._wall = wall or RealWallClock()
        self._transport = transport
        # Refuses impossible replay windows for every caller (CLI and
        # API alike); the CLI passes require_window bound to the cache.
        self._window_check = window_check
        self._status_events = status_events
        self._event_sample_rate = event_sample_rate
        self._event_jitter_s = event_jitter_s
        # Tests and API callers build the engine directly, so the two
        # knobs validate here too, not only in the settings: the jitter
        # bound keeps one order's events in kitchen order, and a
        # sample rate outside [0, 1] means nothing.
        if not 0.0 <= event_jitter_s <= MIN_EVENT_GAP_S:
            raise ValueError(
                f"event_jitter_s {event_jitter_s} must be within [0, {MIN_EVENT_GAP_S}]: "
                "J may be at most the smallest gap between two events of one order "
                f"({MIN_EVENT_GAP_S} s); draws are in [0, J), so an event is never "
                "due before its predecessor"
            )
        if not 0.0 <= event_sample_rate <= 1.0:
            raise ValueError(f"event_sample_rate {event_sample_rate} must be within [0, 1]")
        # Kitchen-call retries (review round 3, item 1): at least one
        # attempt; tests and API callers build the engine directly.
        if kitchen_attempts < 1:
            raise ValueError(f"kitchen_attempts {kitchen_attempts} must be at least 1")
        self._kitchen_attempts = kitchen_attempts
        self._restock_every = restock_every
        self._warm_up_days = warm_up_days
        self._restock_vendors = tuple(restock_vendors)
        # Kitchen-side routing (review round 2, item 6): status events
        # and restocks are region actions - the kitchen is on the region
        # side, and the edge API survives a gateway cut (T2) because it
        # talks to its region's primary on the local network. Orders
        # keep going to the gateway (target_url) either way.
        # `kitchen_url`: one URL for every kitchen call - the stub runs
        # on one process, so stub runs pass the stub's URL here.
        # `kitchen_urls`: region_id -> the region's own edge API base.
        # `vendor_regions`: vendor_id -> region_id, for the restock
        # lines' routing and region_id. With neither URL given, kitchen
        # calls fall back to the target - the single-URL mode the stub
        # and the unit tests run on.
        self._kitchen_url = kitchen_url.rstrip("/") if kitchen_url else None
        self._kitchen_urls = dict(kitchen_urls) if kitchen_urls else None
        self._vendor_regions = dict(vendor_regions) if vendor_regions else None

        self._state = IDLE
        self._wake = asyncio.Event()
        self._done = asyncio.Event()
        self._simclock: SimClock | None = None
        self._client: httpx.AsyncClient | None = None
        self._stream: Iterator[OrderRequest] | None = None
        self._task: asyncio.Task | None = None
        self._log: RunLog | None = None
        self._slots: asyncio.Semaphore | None = None
        self._send_tasks: set[asyncio.Task] = set()
        # the restocks dispatched at the current instant: the barrier
        # waits for them before the day's first order goes out
        self._restock_tasks: set[asyncio.Task] = set()
        # scheduled-but-not-yet-dispatched events and restocks:
        # (sim deadline, rank, tiebreak a, tiebreak b, kind, payload)
        self._heap: list[tuple] = []

        self._run_id = ""
        self._seed = 0
        self._from_day = 0
        self._to_day: int | None = None
        self._seq = 0
        self._sent = 0
        self._acked = 0
        self._parked = 0
        self._refused = 0
        self._failed = 0
        self._events_sent = 0
        self._events_acked = 0
        self._events_conflicted = 0
        self._events_failed = 0
        self._events_skipped = 0
        self._events_duplicate = 0
        self._restocks_sent = 0
        self._restocks_acked = 0
        self._restocks_failed = 0

    def _next_seq(self) -> int:
        """The send sequence number of the next dispatched item."""
        seq = self._seq
        self._seq += 1
        return seq

    def _kitchen_base(self, region_id: object) -> str | None:
        """The API base a kitchen-side call (status event, restock) goes
        to, or None when the call must not go out at all: the one
        override URL (stub runs) takes everything; in edge mode a
        region that is not a known string - including a region_id a
        bad answer made into a non-string - does not fall back to the
        gateway, because the gateway is exactly what does not survive
        a cut (T2): the call is failed with "unknown region" on its
        line instead of being quietly rerouted; with neither
        configured (tests and the old stub mode) the target, as
        before."""
        if self._kitchen_url is not None:
            return self._kitchen_url
        if self._kitchen_urls:
            if isinstance(region_id, str) and region_id in self._kitchen_urls:
                return self._kitchen_urls[region_id]
            return None
        return self._api_base

    # --- state ------------------------------------------------------------

    @property
    def state(self) -> str:
        return self._state

    def _require(self, expected: frozenset[str], what: str) -> None:
        if self._state not in expected:
            raise StateError(f"cannot {what} while the run is {self._state}")

    # --- controls ---------------------------------------------------------

    async def start(
        self,
        *,
        seed: int,
        from_day: int,
        to_day: int | None,
        speed: float,
        run_id: str | None = None,
        meta_extra: dict[str, Any] | None = None,
    ) -> str:
        """Begin a run from `from_day` to `to_day` (None: to the end)."""
        if to_day is not None and to_day < from_day:
            raise ValueError(f"to_day {to_day} is before from_day {from_day}")
        # what the caller asked for, recorded next to the effective days
        # so the run log can be read against the request (a window check
        # may shorten to_day to the last day the data holds)
        requested_to_day = to_day
        # a finished run frees the slot: the control plane may start
        # another run on the same engine without restarting anything
        self._require(FINAL_STATES, "start a run")
        if self._window_check is not None:
            from_day, to_day = self._window_check(from_day, to_day)

        # The restock days belong to this run's window, not to the
        # engine: a later start with a different window must never see
        # a restock from outside it (a day before from_day would fire
        # at once, a day after to_day would keep the run waiting for an
        # order that never comes). Without a to_day the window has no
        # end to intersect with, so restock needs one.
        if self._restock_every is not None:
            if to_day is None:
                raise ValueError(
                    "restock needs to_day: pass to_day so the run knows "
                    "which restock days its window contains"
                )
            run_restock_days = restock_days(
                self._warm_up_days, self._restock_every, from_day, to_day
            )
        else:
            run_restock_days = []

        # Validate and create everything BEFORE any engine state is
        # reset: a bad or reused run id must leave the engine exactly
        # as it was, not half-reset with the previous run's done
        # signal cleared. The candidate id stays local until the
        # commit block, so a refused start never leaks it into status().
        candidate = run_id or default_run_id(seed)
        if not _RUN_ID.fullmatch(candidate):
            raise ValueError(
                f"run_id {candidate!r} must be one path component: "
                "letters, digits, '.', '_' or '-', 1 to 64 characters"
            )
        runs_root = self._runs_dir.resolve()
        run_dir = (self._runs_dir / candidate).resolve()
        if run_dir.parent != runs_root:
            raise ValueError(f"run_id {candidate!r} must stay inside the runs directory")
        run_dir.mkdir(parents=True, exist_ok=True)
        try:
            # exclusive create: two processes cannot both pass an
            # exists() check and then wipe each other's run
            log = RunLog(run_dir / "orders.jsonl", exclusive=True)
        except FileExistsError:
            raise ValueError(f"run {candidate!r} already exists; choose another run id") from None

        meta = {
            "run_id": candidate,
            "seed": seed,
            "from_day": from_day,
            "to_day": to_day,
            "to_day_requested": requested_to_day,
            "speed": speed,
            "inflight": self._inflight,
            "target_url": self._orders_url,
            "state_at_start": RUNNING,
            "wall_start_epoch": time.time(),
            "wall_start_monotonic": self._wall.now(),
            # what the extras of this run do, so a log can be read
            # against the config that produced it
            "events": {
                "enabled": self._status_events,
                "sample_rate": self._event_sample_rate,
                "jitter_s": self._event_jitter_s,
            },
            "restock": {
                "days": run_restock_days,
                "vendors": len(self._restock_vendors),
            },
            # where the kitchen-side calls (events, restocks) went:
            # the region edges, one override URL, or the target itself
            "kitchen": (
                {"mode": "override", "url": self._kitchen_url}
                if self._kitchen_url is not None
                else {"mode": "edge", "urls": self._kitchen_urls}
                if self._kitchen_urls is not None
                else {"mode": "target"}
            ),
            **(meta_extra or {}),
        }
        try:
            (run_dir / "meta.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True, default=str) + "\n",
                encoding="utf-8",
            )
            simclock = SimClock(
                start_sim_seconds=from_day * SECONDS_PER_DAY,
                speed=speed,
                wall=self._wall.now,
                running=False,
            )
            slots = asyncio.Semaphore(self._inflight)
            stream = self._orders_fn(from_day, to_day)
            client = httpx.AsyncClient(timeout=self._http_timeout_s, transport=self._transport)
        except BaseException:
            # the reservation rolls back: no handle leaks and the id is
            # not left burnt for a run that never started
            with contextlib.suppress(Exception):
                log.close()
            with contextlib.suppress(OSError):
                (run_dir / "orders.jsonl").unlink(missing_ok=True)
            raise

        # commit: from here nothing can fail
        self._simclock = simclock
        self._seed = seed
        self._from_day = from_day
        self._to_day = to_day
        self._seq = 0
        self._sent = self._acked = self._parked = self._refused = self._failed = 0
        self._events_sent = 0
        self._events_acked = 0
        self._events_conflicted = 0
        self._events_failed = 0
        self._events_skipped = 0
        self._events_duplicate = 0
        self._restocks_sent = 0
        self._restocks_acked = 0
        self._restocks_failed = 0
        self._heap = [
            (
                day * SECONDS_PER_DAY,
                RANK_RESTOCK,
                day,
                vendor_index,
                "restock",
                None,
            )
            for day in run_restock_days
            for vendor_index in range(len(self._restock_vendors))
        ]
        heapq.heapify(self._heap)
        self._slots = slots
        self._send_tasks.clear()
        self._restock_tasks.clear()
        self._done.clear()
        self._stream = stream
        self._log = log
        self._run_id = candidate
        self._client = client
        self._state = RUNNING
        self._simclock.start()
        self._task = asyncio.create_task(self._run(), name=f"playback-{self._run_id}")
        return self._run_id

    async def pause(self) -> None:
        self._require(frozenset({RUNNING}), "pause")
        assert self._simclock is not None
        self._simclock.pause()
        self._state = PAUSED
        self._wake.set()

    async def resume(self) -> None:
        self._require(frozenset({PAUSED}), "resume")
        assert self._simclock is not None
        self._simclock.start()
        self._state = RUNNING
        self._wake.set()

    async def set_speed(self, speed: float) -> None:
        """Change speed mid-run; the SimClock re-anchors, time never jumps."""
        self._require(frozenset({RUNNING, PAUSED}), "change speed")
        assert self._simclock is not None
        self._simclock.set_speed(speed)
        self._wake.set()

    async def stop(self) -> None:
        """Stop sending, wait for what is in flight, close the log."""
        self._require(LIVE_STATES, "stop")
        self._state = STOPPING
        self._wake.set()
        await self._done.wait()

    async def wait_done(self) -> None:
        """Block until the run is over (done, stopped or failed)."""
        await self._done.wait()

    # --- reporting ---------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """The run's counters, counted by what the answers mean:

        - `events_acked`: the region applied the change;
        - `events_duplicate`: a replayed event id the region already
          knew (2xx with `duplicate`);
        - `events_conflicted`: a real state-machine rejection (409, or
          2xx with `conflict`);
        - `events_failed`: everything else, including a 2xx whose body
          did not parse or that said nothing - a success code alone
          does not make an ack;
        - restocks: only 200 counts as `restocks_acked`, per the merged
          spec; anything else is `restocks_failed`.
        """
        return {
            "state": self._state,
            "run_id": self._run_id or None,
            "seed": self._seed,
            "from_day": self._from_day,
            "to_day": self._to_day,
            "speed": self._simclock.speed if self._simclock else None,
            "orders_sent": self._sent,
            "orders_acked": self._acked,
            "orders_parked": self._parked,
            "orders_refused": self._refused,
            "orders_failed": self._failed,
            "events_sent": self._events_sent,
            "events_acked": self._events_acked,
            "events_conflicted": self._events_conflicted,
            "events_failed": self._events_failed,
            "events_skipped": self._events_skipped,
            "events_duplicate": self._events_duplicate,
            "restocks_sent": self._restocks_sent,
            "restocks_acked": self._restocks_acked,
            "restocks_failed": self._restocks_failed,
            "inflight": len(self._send_tasks),
        }

    def clock(self) -> dict[str, Any]:
        if self._simclock is None:
            return {"state": self._state, "sim_day": None, "sim_time": None, "speed": None}
        day, moment = from_sim_seconds(int(self._simclock.now()))
        return {
            "state": self._state,
            "sim_day": day,
            "sim_time": moment.strftime("%H:%M:%S"),
            "speed": self._simclock.speed,
        }

    # --- the run -----------------------------------------------------------

    async def _run(self) -> None:
        # Cleanup touches only these locals, never self._*: if a start
        # were ever allowed mid-cleanup, the finally block must not
        # close a newer run's log, client or stream.
        log, client, stream = self._log, self._client, self._stream
        assert log is not None and client is not None and stream is not None
        clock = self._simclock
        sim_start = self._from_day * SECONDS_PER_DAY
        stopped = False
        failed = False
        last_restock_deadline: float | None = None
        try:
            # One dispatcher, one total order: the next stream order
            # against the earliest scheduled event or restock, both
            # compared by (simulated instant, kind, stream position).
            # Only this loop mutates the heap or pulls the stream, so a
            # peek, a wait and a pop cannot race anything.
            stream_items = enumerate(stream)
            pending = next(stream_items, None)
            while pending is not None or self._heap:
                if self._state == STOPPING:
                    stopped = True
                    break
                heap_next = self._heap[0][:4] if self._heap else None
                if pending is not None and (heap_next is None or _stream_key(pending) <= heap_next):
                    index, request = pending
                    order_target = to_sim_seconds(request.order_day, request.order_time)
                    await self._wait_until(order_target)
                    if self._state == STOPPING:
                        stopped = True
                        break
                    # the day's restocks are acknowledged before the
                    # first order goes out (review item 2), and a
                    # pause or stop that arrived during that wait is
                    # honored before the order leaves
                    if self._restock_tasks:
                        if not await self._restock_barrier():
                            stopped = True
                            break
                        if not await self._after_gate():
                            stopped = True
                            break
                    self._dispatch_order(order_target - sim_start, request)
                    pending = next(stream_items, None)
                else:
                    deadline, _rank, tie_a, tie_b, kind, payload = heapq.heappop(self._heap)
                    await self._wait_until(deadline)
                    if self._state == STOPPING:
                        stopped = True
                        break
                    if kind == "event":
                        # not a restock: the restocks go first all the
                        # way to their acks (review item 2)
                        if self._restock_tasks:
                            if not await self._restock_barrier():
                                stopped = True
                                break
                            if not await self._after_gate():
                                stopped = True
                                break
                        handle, to_status, jitter = payload
                        # The event goes out only once the order's
                        # outcome is known (it needs the order id, and
                        # a refused order never moves through the
                        # kitchen). The outcome always resolves: every
                        # send finishes, errors within the HTTP
                        # timeout, or is cancelled by the drain - but
                        # a slow target must not hold a stop hostage,
                        # so the wait also ends when the run stops.
                        if not await self._await_or_stop(handle.future):
                            stopped = True
                            break
                        # a pause or stop that arrived during the
                        # outcome wait is honored before the event
                        # goes out
                        if not await self._after_gate():
                            stopped = True
                            break
                        self._dispatch_event(deadline - sim_start, handle, to_status, jitter)
                    else:
                        if deadline != last_restock_deadline:
                            # The first restock of an instant waits for
                            # every in-flight non-restock send first
                            # (review round 2, item 2): an order sent at
                            # 23:59:58 could still be in flight at
                            # 00:00:00, and "order, then restock" ends
                            # the cycle at par while "restock, then
                            # order" ends it at par minus the order -
                            # the oracle applies the order first, so
                            # the wire must too. Waiting for events too
                            # is fine: they never touch stock, and it
                            # makes the restock instant a clean sync
                            # point, at most one in-flight window per
                            # restock day. The later restocks of the
                            # same instant find nothing outstanding:
                            # the dispatcher is sequential, and no
                            # non-restock send started meanwhile.
                            while True:
                                outstanding = {
                                    task
                                    for task in self._send_tasks - self._restock_tasks
                                    if not task.done()
                                }
                                if not outstanding:
                                    break
                                if not await self._await_or_stop(asyncio.wait(outstanding)):
                                    stopped = True
                                    break
                            if stopped:
                                break
                            if not await self._after_gate():
                                stopped = True
                                break
                            last_restock_deadline = deadline
                        self._dispatch_restock(deadline - sim_start, tie_a, tie_b)
                # One loop turn per dispatch, even in a burst: sends and
                # control requests must be able to run between the
                # items that are all due at once.
                await asyncio.sleep(0)
        except Exception as error:  # noqa: BLE001 - the run must not die silently
            traceback.print_exc()
            print(f"playback run {self._run_id} failed: {error}", file=sys.stderr)
            failed = True
        finally:
            cleanup_cancelled = False
            try:
                # Each step is guarded: a cleanup error must not skip
                # the final state or _done.set().
                if not stopped and not failed:
                    # A run that ended normally waits for every send it
                    # started (review round 2, item 3): on a slow target
                    # the bounded drain below would otherwise cut the
                    # tail off a *completed* run and count it cancelled.
                    # Each send is still bounded by the HTTP timeout once
                    # it holds a slot; only a stop (or a failure) falls
                    # through to the drain.
                    while True:
                        outstanding = {task for task in self._send_tasks if not task.done()}
                        if not outstanding:
                            break
                        if not await self._await_or_stop(asyncio.wait(outstanding)):
                            stopped = True
                            break
                try:
                    await self._drain()
                except Exception:  # noqa: BLE001
                    traceback.print_exc()
                    failed = True
                try:
                    log.close()
                except Exception:  # noqa: BLE001
                    traceback.print_exc()
                    failed = True
                try:
                    await client.aclose()
                except Exception:  # noqa: BLE001
                    traceback.print_exc()
                    failed = True
                stream_close = getattr(stream, "close", None)
                if stream_close is not None:
                    try:
                        stream_close()
                    except Exception:  # noqa: BLE001
                        traceback.print_exc()
                        failed = True
            except asyncio.CancelledError:
                # a cancelled cleanup is a failed run, not a hang
                cleanup_cancelled = True
                raise
            finally:
                if clock is not None:
                    # freeze the reported clock: a finished run must not
                    # keep advancing simulated time
                    with contextlib.suppress(Exception):
                        clock.pause()
                # set only here, at the very end: earlier, and a start
                # could slip in while this run is still cleaning up.
                # FAILED always wins; a stop that arrived during the
                # final wait (or during the drain) still reads as
                # STOPPED, never as done.
                self._state = (
                    FAILED
                    if failed or cleanup_cancelled
                    else (STOPPED if stopped or self._state == STOPPING else DONE)
                )
                self._done.set()

    async def _wait_until(self, target_sim: float) -> None:
        while True:
            if self._state not in (RUNNING, PAUSED):
                return
            if self._state == PAUSED:
                await self._wake_or_end()
                continue
            assert self._simclock is not None
            wait = self._simclock.wall_seconds_until(target_sim)
            if wait <= 0:
                return
            await self._sleep_or_wake(min(wait, WAIT_SLICE_S))

    async def _wake_or_end(self) -> None:
        """Wait while paused; wake on any later control operation.

        The event is cleared *after* each wait returns, never before:
        resume can arrive while the sender is still walking to this
        method, and clearing first would swallow that wake and hang the
        run forever.
        """
        while self._state == PAUSED:
            await self._wake.wait()
            self._wake.clear()

    async def _sleep_or_wake(self, seconds: float) -> None:
        """Sleep `seconds` of wall time, or until a control op wakes us.

        Safe in one thread: the clear and the wait have no await point
        between them, so no operation can set the event in between.
        """
        self._wake.clear()
        sleep_task = asyncio.ensure_future(self._wall.sleep(seconds))
        wake_task = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({sleep_task, wake_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleep_task, wake_task):
                task.cancel()
            await asyncio.gather(sleep_task, wake_task, return_exceptions=True)

    async def _await_or_stop(self, waiter: Awaitable) -> bool:
        """Wait for `waiter`, or give up when the run stops.

        The dispatcher uses this for two waits that a target can hold
        indefinitely: an order's outcome and the restock barrier. A
        control operation that is not a stop (pause, resume, a speed
        change) also wakes the wait, but it does not end it: the loop
        waits again, so a pause keeps today's behavior - the outcome is
        still awaited, the event only goes out after the resume. A stop
        ends the wait immediately, so `stop` never sits behind a slow
        or unanswering target longer than the drain timeout.

        `waiter` may be a future the caller owns (an order's outcome:
        never cancelled here) or a coroutine private to this call (a
        restock barrier: cancelled on the way out).
        """
        task = waiter if isinstance(waiter, asyncio.Future) else asyncio.ensure_future(waiter)
        try:
            while True:
                wake_task = asyncio.ensure_future(self._wake.wait())
                try:
                    done, _ = await asyncio.wait(
                        {task, wake_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    wake_task.cancel()
                    await asyncio.gather(wake_task, return_exceptions=True)
                if self._state == STOPPING:
                    return False
                if task.done():
                    return True
                # a wake that was not a stop: clear it and keep waiting
                self._wake.clear()
        finally:
            if task is not waiter:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def _after_gate(self) -> bool:
        """Honor a pause or stop that arrived while a gate was waiting.

        Every stop-aware gate (the restock barrier, an outcome wait,
        the wait before a restock batch) ends without looking at the
        state a control operation may have changed in the meantime, so
        the dispatcher calls this before the next send goes out: a
        pause parks it, a stop ends it. Returns False when the run is
        stopping."""
        if self._state == PAUSED:
            await self._wake_or_end()
        return self._state != STOPPING

    def _dispatch_order(self, sim_offset: float, request: OrderRequest) -> None:
        seq = self._next_seq()
        self._sent += 1
        handle = self._schedule_events(seq, request, sim_offset)
        task = asyncio.create_task(self._send_one(seq, request, sim_offset, handle))
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)

    def _schedule_events(
        self, order_seq: int, request: OrderRequest, sim_offset: float
    ) -> _OrderHandle | None:
        """Reserve the order's status events, or None if it gets none.

        The deadlines are fixed here, at dispatch: they depend on
        nothing but the order's simulated instant, the seed and the
        settings, never on when the ack arrives. Whether an event is
        actually sent is decided later, by the order's outcome.
        """
        if not self._status_events:
            return None
        if self._event_sample_rate < 1.0 and (
            stable_float(self._seed, request.external_order_id, "events-sample")
            >= self._event_sample_rate
        ):
            return None  # this order is out of the sample: no events
        sim_time = self._from_day * SECONDS_PER_DAY + sim_offset
        handle = _OrderHandle(
            asyncio.get_running_loop().create_future(), request, sim_time, order_seq
        )
        for index, (to_status, offset) in enumerate(EVENT_SCHEDULE):
            jitter = (
                stable_float(self._seed, request.external_order_id, f"events-jitter-{index}")
                * self._event_jitter_s
            )
            # The jitter never reorders the four events of one order
            # (the base gaps are at least 120 s and the jitter is below
            # 60 s) and never moves one before the order itself. The
            # tiebreak (dispatch sequence, event index) keeps two
            # orders whose events land on the same simulated second in
            # dispatch order.
            heapq.heappush(
                self._heap,
                (
                    sim_time + offset + jitter,
                    RANK_EVENT,
                    order_seq,
                    index,
                    "event",
                    (handle, to_status, jitter),
                ),
            )
        return handle

    def _dispatch_event(
        self, sim_offset: float, handle: _OrderHandle, to_status: OrderStatus, jitter: float
    ) -> None:
        outcome = handle.outcome
        assert outcome is not None
        assert self._log is not None
        if not outcome.accepted or outcome.order_id is None:
            # Parked, refused, failed or cancelled orders never move
            # through the kitchen. The scheduled event still gets its
            # line, with the reason, so the log carries one entry per
            # scheduled event and the count is checkable.
            seq = self._next_seq()
            self._events_skipped += 1
            self._log.add(
                seq,
                {
                    "kind": "status_event",
                    "seq": seq,
                    "external_order_id": handle.request.external_order_id,
                    "to_status": to_status.value,
                    "sim_sent_at": round(sim_offset, 3),
                    "skipped": True,
                    "reason": outcome.reason,
                },
            )
            self._log.flush()
            return
        seq = self._next_seq()
        self._events_sent += 1
        # The house style for event ids: deterministic, and without the
        # run id. POST /orders is idempotent by (channel,
        # external_order_id) - WP1's inbox answers a repeat with the
        # original order_id - so a re-run of the same window gets the
        # same order ids and therefore the same event ids, and the
        # region answers duplicate. A run id in the event id (or a
        # uuid4) would send *new* ids for orders that have already
        # moved on: every one of them a conflict and an exception at
        # HQ, which is exactly what this rule prevents. The same id on
        # every retry is what makes retries safe: the region's
        # change_status records the id in processed_events, so a
        # repeat of a committed change answers duplicate and changes
        # nothing, while a new id for the same move would conflict.
        record: dict[str, Any] = {
            "kind": "status_event",
            "seq": seq,
            "external_order_id": handle.request.external_order_id,
            "vendor_id": handle.request.vendor_id,
            "order_id": outcome.order_id,
            "region_id": outcome.region_id,
            "to_status": to_status.value,
            "event_id": str(uuid5(NAMESPACE_URL, f"{outcome.order_id}:{to_status.value}")),
            "sim_sent_at": round(sim_offset, 3),
            "jitter_s": round(jitter, 3),
            "wall_sent_at": None,
        }
        task = asyncio.create_task(self._send_event(seq, record, handle.last_event))
        # the next event of this order waits for this task: the region's
        # state machine is first-committed-change-wins, so preparing must
        # not be able to commit before accepted
        handle.last_event = task
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)

    def _dispatch_restock(self, sim_offset: float, day: int, vendor_index: int) -> None:
        seq = self._next_seq()
        self._restocks_sent += 1
        vendor_id = self._restock_vendors[vendor_index]
        record: dict[str, Any] = {
            "kind": "restock",
            "seq": seq,
            "vendor_id": vendor_id,
            "region_id": self._vendor_regions.get(vendor_id) if self._vendor_regions else None,
            "restock_day": day,
            "sim_sent_at": round(sim_offset, 3),
            "wall_sent_at": None,
        }
        task = asyncio.create_task(self._send_restock(seq, record))
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)
        # the barrier waits for the whole batch before the day's first
        # order: a restock only counts once the region has applied it
        self._restock_tasks.add(task)
        task.add_done_callback(self._restock_tasks.discard)

    async def _restock_barrier(self) -> bool:
        """Wait for the outstanding restocks before anything else goes
        out. Returns False if stop arrives during the wait.

        Dispatch order put the restocks first, but the region applies
        them in answer order: without this barrier the day's first
        orders race up to inflight-1 restocks still in flight, and
        against a real region their refused-versus-accepted outcome
        would depend on timing. The stub never shows this; the oracle
        applies the restock first, and so does the run."""
        while self._restock_tasks:
            if not await self._await_or_stop(asyncio.wait(set(self._restock_tasks))):
                return False
        return True

    async def _send_one(
        self,
        seq: int,
        request: OrderRequest,
        sim_offset: float,
        handle: _OrderHandle | None = None,
    ) -> None:
        assert self._log is not None and self._client is not None and self._slots is not None
        # The record exists before the semaphore: a task cancelled while
        # still queued for a slot was never sent, and the run log must
        # carry one line for every dispatched order either way.
        record: dict[str, Any] = {
            "kind": "order",
            "seq": seq,
            "external_order_id": request.external_order_id,
            "order_day": request.order_day,
            "order_time": request.order_time.isoformat(),
            "sim_sent_at": sim_offset,
            "wall_sent_at": None,
        }
        try:
            async with self._slots:
                record["wall_sent_at"] = round(self._wall.now(), 3)
                try:
                    response = await self._client.post(
                        self._orders_url, json=request.model_dump(mode="json")
                    )
                    record["http_status"] = response.status_code
                    record.update(_response_fields(response))
                    self._count(response)
                except Exception as error:  # noqa: BLE001 - a failed send is a logged outcome
                    # httpx errors, bad URLs, anything: the line is still
                    # written and the failure still counted, or the
                    # reorder buffer would hold every later record hostage
                    record["error"] = type(error).__name__
                    self._failed += 1
        except asyncio.CancelledError:
            record["error"] = "cancelled"
            self._log.add(seq, record)
            self._log.flush()
            self._failed += 1
            if handle is not None:
                # the dispatcher may be waiting on this order's outcome
                handle.resolve(_OrderOutcome(False, None, "cancelled"))
            raise
        if handle is not None:
            handle.resolve(_outcome_of(record))
        record["wall_acked_at"] = round(self._wall.now(), 3)
        self._log.add(seq, record)
        self._log.flush()

    async def _send_event(
        self, seq: int, record: dict[str, Any], previous: asyncio.Task | None
    ) -> None:
        path = f"/vendors/{record['vendor_id']}/orders/{record['order_id']}/status"
        await self._send_aux(
            seq,
            record,
            path,
            {"event_id": record["event_id"], "to_status": record["to_status"]},
            previous,
        )

    async def _send_restock(self, seq: int, record: dict[str, Any]) -> None:
        await self._send_aux(seq, record, f"/vendors/{record['vendor_id']}/restock", None, None)

    async def _send_aux(
        self,
        seq: int,
        record: dict[str, Any],
        path: str,
        body: dict[str, Any] | None,
        previous: asyncio.Task | None = None,
    ) -> None:
        """Send one event or restock: the same slot, log and failure
        discipline as an order send, with counters picked by kind.

        An event waits here for the previous event of its own order
        (the kitchen timeline is first-committed-change-wins), without
        blocking the dispatcher and without holding a slot while it
        waits: the wait sits before the semaphore, inside the cancel
        handling, so a task cancelled by the drain while waiting still
        writes its line. If the previous event failed, conflicted or
        was cancelled, this one still goes out: the region decides,
        and the answer is logged and counted as usual.

        A timeout, a connection error or a 5xx is retried, up to
        `kitchen_attempts` attempts in total, backing off in wall time
        between attempts without holding a slot (review round 3,
        item 1). The edge does not cancel a call that missed its
        deadline, so the first attempt may have committed: every
        attempt sends the same body - for an event, the same event_id,
        which the region's processed_events answers duplicate, and a
        restock has no body, and a top-up twice is safe - so one
        retry too many is a no-op duplicate counted as acked, while
        giving up after one attempt would send the order's next event
        into an impossible move: a conflict and an HQ exception
        caused by playback, not by a real race. A 2xx, 404, 409 or
        422 is never retried: the region answered. The chained next
        event of the order waits for this task, so it waits for the
        final answer, retries included. A stop during a backoff
        cancels the task, and its line says cancelled, as any
        cancelled send does; a restock that still fails after its
        last attempt is counted failed and the run goes on - in a
        fault harness, a call that could not get through is a result
        to report, not a crash.

        The base the path goes to is resolved inside the counted
        path as well: in edge mode a region the routing does not know
        (or a region_id a bad answer made into a non-string) fails
        the call with "unknown region" on its line, sends nothing,
        and never falls back to the gateway - the gateway is exactly
        what does not survive a cut."""
        assert self._log is not None and self._client is not None and self._slots is not None
        is_event = record["kind"] == "status_event"
        response: httpx.Response | None = None
        try:
            if is_event and previous is not None and not previous.done():
                # asyncio.wait returns on completion even when the
                # previous task failed or was cancelled; it does not
                # re-raise
                await asyncio.wait({previous})
            attempt = 1
            while True:
                async with self._slots:
                    try:
                        # The base is resolved here, inside the counted
                        # path, so a bad region_id (or any error in the
                        # URL) is written to the line and counted, not
                        # raised out of the task into a missing
                        # placeholder.
                        base = self._kitchen_base(record.get("region_id"))
                        if base is None:
                            # edge mode, and the routing does not know
                            # this region: send nothing - falling back
                            # to the gateway would quietly undo the
                            # region routing
                            record["error"] = "unknown region"
                            break
                        record["wall_sent_at"] = round(self._wall.now(), 3)
                        response = await self._client.post(base + path, json=body)
                        record["http_status"] = response.status_code
                        record["attempts"] = attempt
                        if response.status_code < 500:
                            # the region answered: never retry a 2xx,
                            # 404, 409 or 422
                            break
                        failure = f"http {response.status_code}"
                    except httpx.TransportError as error:
                        # a timeout or a connection error: the call may
                        # still have committed at the region
                        record["attempts"] = attempt
                        failure = type(error).__name__
                    except Exception as error:  # noqa: BLE001 - a failed send is a logged outcome
                        record["attempts"] = attempt
                        record["error"] = type(error).__name__
                        break
                    if attempt >= self._kitchen_attempts:
                        record["error"] = failure
                        break
                # back off in wall time, without the slot, before the
                # next attempt; a stop cancels the task here and its
                # line says cancelled
                await self._wall.sleep(
                    _KITCHEN_BACKOFF_S[min(attempt - 1, len(_KITCHEN_BACKOFF_S) - 1)]
                )
                attempt += 1
            if is_event:
                if response is not None:
                    record.update(_response_fields(response, EVENT_RESPONSE_FIELDS))
                self._count_event_answer(record, response, attempt)
            elif response is not None and response.status_code == 200:
                # the merged spec: a restock answers 200, and
                # playback reads nothing else - any other code is a
                # failure, 201 included
                self._restocks_acked += 1
            else:
                self._count_aux_failed(record)
        except asyncio.CancelledError:
            record["error"] = "cancelled"
            self._log.add(seq, record)
            self._log.flush()
            self._count_aux_failed(record)
            raise
        record["wall_acked_at"] = round(self._wall.now(), 3)
        self._log.add(seq, record)
        self._log.flush()

    def _count_event_answer(
        self, record: dict[str, Any], response: httpx.Response | None, attempt: int
    ) -> None:
        """Count a status answer by what it means, not by its code
        alone (review round 2, item 5):

        - a 409 is the state machine rejecting the change - a real
          conflict, whatever its body;
        - an answer that never arrived (response is None, the record
          carries the transport error) is a failure;
        - on a 2xx, an answer whose body did not parse (the record
          carries `error` from _response_fields) is a failure;
        - `duplicate` on the first attempt is a replayed event the
          region already knew;
        - `duplicate` on a retry means our own earlier attempt landed
          (every attempt sends the same event_id, and the region's
          processed_events makes a repeat a no-op): the change is
          applied, so it counts as acked, and the line says
          `landed_earlier`;
        - `conflict` on a 2xx is a real conflict too;
        - `applied` is the only other ack;
        - anything else is a failure, below.
        """
        if response is not None and response.status_code == 409:
            self._events_conflicted += 1
            return
        if "error" in record:
            self._events_failed += 1
            return
        if response is None or response.status_code not in (200, 201):
            self._events_failed += 1
            return
        if record.get("duplicate"):
            if attempt > 1:
                # the same event_id went out on an earlier attempt of
                # this same send: it landed, the region just never
                # answered - so the change is applied
                record["landed_earlier"] = True
                self._events_acked += 1
            else:
                self._events_duplicate += 1
        elif record.get("conflict"):
            self._events_conflicted += 1
        elif record.get("applied"):
            self._events_acked += 1
        else:
            # A 2xx that is neither applied, duplicate nor conflict has
            # no meaning in the status state machine (received ->
            # accepted -> preparing -> ready -> picked_up, first
            # committed change wins). Counting it as acked would hide a
            # region or gateway bug behind a success code, so it is
            # counted as failed, on purpose, and the line says why.
            record["error"] = "unexpected status answer"
            self._events_failed += 1

    def _count_aux_failed(self, record: dict[str, Any]) -> None:
        if record["kind"] == "status_event":
            self._events_failed += 1
        else:
            self._restocks_failed += 1

    def _count(self, response: httpx.Response) -> None:
        if response.status_code in (200, 201):
            self._acked += 1
        elif response.status_code == 202:
            self._parked += 1
        elif response.status_code == 409:
            self._refused += 1
        else:
            self._failed += 1

    async def _drain(self) -> None:
        """Wait for every in-flight request, then let the log close."""
        assert self._log is not None
        if self._send_tasks:
            _, pending = await asyncio.wait(set(self._send_tasks), timeout=self._drain_timeout_s)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        self._log.flush()


def _response_fields(
    response: httpx.Response, fields: tuple[str, ...] = RESPONSE_FIELDS
) -> dict[str, Any]:
    """The pass-through fields of an answer, if it sent any."""
    try:
        body = response.json()
    except ValueError:
        return {"error": f"unparsable response body ({len(response.content)} bytes)"}
    if not isinstance(body, dict):
        return {"error": "response body is not a JSON object"}
    return {field: body[field] for field in fields if field in body}


_REASONS = {200: "accepted", 201: "accepted", 202: "parked", 409: "refused"}


def _outcome_of(record: dict[str, Any]) -> _OrderOutcome:
    """What the order's send came to, for its scheduled events."""
    status = record.get("http_status")
    order_id = record.get("order_id")
    accepted = status in (200, 201) and order_id is not None
    reason: str = "failed"
    if isinstance(status, int):
        reason = _REASONS.get(status, "failed")
    if "error" in record:
        reason = record["error"]  # transport error, or "cancelled"
    return _OrderOutcome(accepted, order_id, reason, record.get("region_id"))


def _stream_key(item: tuple[int, OrderRequest]) -> tuple[int, int, int, int]:
    """The sort key of a (stream position, order) pair on the merged
    timeline: (simulated instant, rank, position, 0)."""
    index, request = item
    return (to_sim_seconds(request.order_day, request.order_time), RANK_ORDER, index, 0)


def default_run_id(seed: int) -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"-seed{seed}"

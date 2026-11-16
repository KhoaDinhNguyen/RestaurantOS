"""The playback engine: stream the sorted orders, send each at its
simulated instant, log what came back.

Scheduling follows the brief: an order t simulated seconds after the
start goes out t/speed wall-clock seconds after the start; send times
are never precomputed. `common.simclock.SimClock` holds the
(wall-clock, simulated) anchor pair and re-anchors on every speed
change and on pause/resume, so simulated time never jumps.

Two properties the acceptance test depends on:

- Send order is the stream order, always. Orders in the same second go
  out in the same sequence, and the in-flight bound is a semaphore
  acquired in dispatch order (asyncio semaphores are FIFO), so it
  delays but never reorders.
- The run log is written in send order whatever order the responses
  arrive in: each line carries its send sequence number, and a small
  reorder buffer holds a completed line until every earlier line is
  written. Byte-identical logs across runs therefore do not depend on
  how acks interleave.

Wall time enters through a `WallClock` (protocol) so tests can drive
the scheduler deterministically: the real one wraps `time.monotonic`
and `asyncio.sleep`; a fake one resolves sleeps only when the test
advances it.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import traceback
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from restaurantos.common.models import OrderRequest
from restaurantos.common.simclock import SECONDS_PER_DAY, SimClock, from_sim_seconds, to_sim_seconds

# Re-check the clock at most this often during a long wait, so a speed
# change is picked up within a slice instead of at the far deadline.
WAIT_SLICE_S = 0.2

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
    """`orders.jsonl`, one line per order, written in send order."""

    def __init__(self, path: Path) -> None:
        self._f = path.open("w", encoding="utf-8")
        self._next = 0
        self._pending: dict[int, dict[str, Any]] = {}

    def add(self, seq: int, record: dict[str, Any]) -> None:
        self._pending[seq] = record

    def flush(self) -> None:
        while self._next in self._pending:
            record = self._pending.pop(self._next)
            self._f.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._next += 1

    def close(self) -> None:
        # Drain guarantees every live send task added its record; a gap
        # here is a bug, and the marker makes it visible in the file
        # instead of silently shifting every later line.
        while self._pending:
            seq = min(self._pending)
            record = self._pending.pop(seq)
            record.setdefault("error", "missing: the send task produced no result")
            self._f.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._next = seq + 1
        self._f.close()


class Playback:
    """One run, or the idle slot waiting for one.

    All methods run on the event loop the engine was started on; the
    control routes and the CLI call them from there.
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
    ) -> None:
        self._orders_fn = orders_fn
        self._orders_url = target_url.rstrip("/") + "/orders"
        self._inflight = inflight
        self._http_timeout_s = http_timeout_s
        self._drain_timeout_s = drain_timeout_s
        self._runs_dir = runs_dir
        self.default_speed = default_speed
        self._wall = wall or RealWallClock()
        self._transport = transport

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

        self._run_id = ""
        self._seed = 0
        self._from_day = 0
        self._to_day: int | None = None
        self._sent = 0
        self._acked = 0
        self._parked = 0
        self._refused = 0
        self._failed = 0

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
        # a finished run frees the slot: the control plane may start
        # another run on the same engine without restarting anything
        self._require(FINAL_STATES, "start a run")
        self._simclock = SimClock(
            start_sim_seconds=from_day * SECONDS_PER_DAY,
            speed=speed,
            wall=self._wall.now,
            running=False,
        )
        self._run_id = run_id or default_run_id(seed)
        self._seed = seed
        self._from_day = from_day
        self._to_day = to_day
        self._sent = self._acked = self._parked = self._refused = self._failed = 0
        self._slots = asyncio.Semaphore(self._inflight)
        self._send_tasks.clear()
        self._done.clear()
        self._stream = self._orders_fn(from_day, to_day)
        run_dir = self._runs_dir / self._run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        self._log = RunLog(run_dir / "orders.jsonl")
        meta = {
            "run_id": self._run_id,
            "seed": seed,
            "from_day": from_day,
            "to_day": to_day,
            "speed": speed,
            "inflight": self._inflight,
            "target_url": self._orders_url,
            "state_at_start": RUNNING,
            "wall_start_epoch": time.time(),
            **(meta_extra or {}),
        }
        (run_dir / "meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
        )
        self._client = httpx.AsyncClient(timeout=self._http_timeout_s, transport=self._transport)
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
        assert self._stream is not None and self._log is not None and self._client is not None
        stopped = False
        try:
            sim_start = self._from_day * SECONDS_PER_DAY
            for seq, request in enumerate(self._stream):
                target = to_sim_seconds(request.order_day, request.order_time)
                await self._wait_until(target)
                if self._state == STOPPING:
                    stopped = True
                    break
                self._dispatch(seq, request, target - sim_start)
        except Exception as error:  # noqa: BLE001 - the run must not die silently
            traceback.print_exc()
            print(f"playback run {self._run_id} failed: {error}", file=sys.stderr)
            self._state = FAILED
        finally:
            await self._drain()
            self._log.close()
            await self._client.aclose()
            stream_close = getattr(self._stream, "close", None)
            if stream_close is not None:
                stream_close()
            if self._state != FAILED:
                self._state = STOPPED if stopped else DONE
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

    def _dispatch(self, seq: int, request: OrderRequest, sim_offset: float) -> None:
        self._sent += 1
        task = asyncio.create_task(self._send_one(seq, request, sim_offset))
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)

    async def _send_one(self, seq: int, request: OrderRequest, sim_offset: float) -> None:
        assert self._log is not None and self._client is not None and self._slots is not None
        # The record exists before the semaphore: a task cancelled while
        # still queued for a slot was never sent, and the run log must
        # carry one line for every dispatched order either way.
        record: dict[str, Any] = {
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
                except httpx.HTTPError as error:
                    record["error"] = type(error).__name__
                else:
                    record["http_status"] = response.status_code
                    record.update(_response_fields(response))
                    self._count(response)
        except asyncio.CancelledError:
            record["error"] = "cancelled"
            self._log.add(seq, record)
            self._log.flush()
            self._failed += 1
            raise
        record["wall_acked_at"] = round(self._wall.now(), 3)
        self._log.add(seq, record)
        self._log.flush()

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


def _response_fields(response: httpx.Response) -> dict[str, Any]:
    """The pass-through fields of a POST /orders answer, if it sent any."""
    try:
        body = response.json()
    except ValueError:
        return {"error": f"unparsable response body ({len(response.content)} bytes)"}
    if not isinstance(body, dict):
        return {"error": "response body is not a JSON object"}
    return {field: body[field] for field in RESPONSE_FIELDS if field in body}


def default_run_id(seed: int) -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"-seed{seed}"

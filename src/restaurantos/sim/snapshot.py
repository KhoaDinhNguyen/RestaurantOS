"""Snapshot and restore (WP6b, architecture Section 7.4).

**Save** pauses playback (once its dispatch frontier - the last item it
sent, not the clock - is past 00:30 of its day), records the run, that
point, Toxiproxy's proxies and toxics, the cluster
file and the run log, stops ONLY the PostgreSQL services (a bare
`docker compose stop` would stop Toxiproxy too, which reloads its config
on start and silently heals every fault), archives every data volume
with the image the databases run (never pulled), starts the databases
again and resumes playback. It builds into `snapshots/.<name>.partial/`
and renames at the end, so a failed snapshot leaves nothing behind.

**Restore** puts it all back - the cluster file first, then the
generated Compose file regenerated from it, BEFORE any container starts -
restarts every service (they cache epochs and primaries that the restore
makes stale), and resumes playback by **replaying the snapshot's whole
day** as a new run (option C): orders and status events sent before the
snapshot point are answered as duplicates and change nothing, and the
day's midnight restock is skipped if it already went out (restock tops
up to par, so a second one would refill what was sold since midnight).
The limitations of this choice are in architecture Section 7.4.

Never `docker compose down -v` here: it destroys the cluster.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import subprocess
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from restaurantos.common.cluster import BASELINE, HQ_PRIMARY, Cluster, load_cluster, proxies
from restaurantos.common.simclock import SECONDS_PER_DAY
from restaurantos.sim.checker import RESUMED_FROM, run_chain
from restaurantos.sim.config import SimSettings
from restaurantos.sim.timeline import EVENT_SCHEDULE
from restaurantos.sim.toxiproxy import Toxiproxy

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SNAPSHOT_FILE = "snapshot.json"
LIVE_STATES = ("running", "paused")


class SnapshotError(RuntimeError):
    status = 400
    code = "snapshot_error"


class SnapshotExistsError(SnapshotError):
    status = 409
    code = "snapshot_exists"


class UnknownSnapshotError(SnapshotError):
    status = 404
    code = "unknown_snapshot"


def check_name(name: str) -> str:
    if not _NAME.fullmatch(name):
        raise SnapshotError(
            f"snapshot name {name!r} must be one path component: letters, digits, "
            "'.', '_' or '-', 1 to 64 characters, not starting with '.'"
        )
    return name


def sim_seconds(clock: dict[str, Any]) -> int | None:
    """Absolute simulated seconds of a `GET /sim/clock` answer."""
    day, moment = clock.get("sim_day"), clock.get("sim_time")
    if day is None or not moment:
        return None
    hours, minutes, seconds = (int(part) for part in str(moment).split(":"))
    return int(day) * SECONDS_PER_DAY + hours * 3600 + minutes * 60 + seconds


def frontier_seconds(status: dict[str, Any]) -> int | None:
    """The dispatch frontier of a `GET /sim/playback` answer, in absolute
    simulated seconds: the last item the engine actually sent (None
    before the run's first dispatch)."""
    return sim_seconds(
        {"sim_day": status.get("dispatched_sim_day"), "sim_time": status.get("dispatched_sim_time")}
    )


def held_seconds(status: dict[str, Any]) -> int | None:
    """The earliest order the engine pulled but has not sent (shuffle
    holds up to a window of them), absolute simulated seconds."""
    return sim_seconds(
        {"sim_day": status.get("held_sim_day"), "sim_time": status.get("held_sim_time")}
    )


def _hhmm(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}"


def point_problem(status: dict[str, Any], minimum: int) -> str | None:
    """Why the run cannot be snapshotted where it stands, or None.

    Restore replays the frontier's day from midnight, so the point must
    stand `minimum` into that day (the previous day's events are out by
    then), and no order of an EARLIER day may still be held back unsent:
    with shuffle on, a held order can trail the frontier, and at night a
    window of orders can span 00:30 - the replay of the next day would
    never send it."""
    frontier = frontier_seconds(status)
    if frontier is None:
        return "nothing dispatched yet"
    if frontier % SECONDS_PER_DAY < minimum:
        return f"dispatch stands at {_at(frontier)}, before {_hhmm(minimum)} of its day"
    held = held_seconds(status)
    midnight = frontier - frontier % SECONDS_PER_DAY
    if held is not None and held < midnight:
        return (
            f"an order of {_at(held)} is still held back unsent (shuffle) while dispatch "
            f"stands at {_at(frontier)}"
        )
    return None


def next_due_seconds(status: dict[str, Any]) -> int | None:
    """The earliest item of any kind the engine still has to send."""
    return sim_seconds(
        {"sim_day": status.get("next_due_sim_day"), "sim_time": status.get("next_due_sim_time")}
    )


def offset_floor(engine: dict[str, Any]) -> int:
    """The least a snapshot's point may stand into its day, for the engine
    that runs the run (its `engine` block in `GET /sim/playback`).

    Restore replays the day from midnight, so everything of the PREVIOUS
    day must be dispatched by the snapshot point: a late order's last
    status event is due up to the schedule's last offset plus the jitter
    after midnight. With events off only the midnight restock batch
    remains, which a point one second past midnight is already beyond.
    Read from the live engine, not this process's settings: the playback
    host may run with another jitter than the shell that saves."""
    events = engine.get("events") or {}
    if not events.get("enabled") or not events.get("sample_rate"):
        return 1
    last = max(offset for _status, offset in EVENT_SCHEDULE)
    return max(1, last + math.ceil(float(events.get("jitter_s") or 0.0)))


def _at(seconds: int) -> str:
    day, rest = divmod(seconds, SECONDS_PER_DAY)
    return f"day {day} {rest // 3600:02d}:{rest % 3600 // 60:02d}:{rest % 60:02d}"


# --- Docker -------------------------------------------------------------------

Compose = Callable[..., subprocess.CompletedProcess]
Runner = Callable[[list[str]], subprocess.CompletedProcess]


def _bootstrap_compose(*args: str) -> subprocess.CompletedProcess:
    from restaurantos.sim.bootstrap import _compose

    return _compose(*args)


def _run(command: list[str]) -> subprocess.CompletedProcess:
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise SnapshotError(
            f"{' '.join(command)} failed ({result.returncode}):\n{result.stdout}{result.stderr}"
        )
    return result


def volume_name(node: str) -> str:
    """Compose names a service's volume <project>_<volume>."""
    from restaurantos.sim.bootstrap import PROJECT

    return f"{PROJECT}_{node}-data"


def database_services(cluster: Cluster) -> list[str]:
    """Every PostgreSQL service of the generated Compose file, baseline included."""
    return [*cluster.nodes, BASELINE]


@dataclass
class Docker:
    """The Docker side of a snapshot, as blocking calls (run them in a
    thread). Tests pass fakes."""

    compose: Compose = _bootstrap_compose
    run: Runner = _run

    def stop(self, services: list[str]) -> None:
        self.compose("stop", *services)

    def start(self, services: list[str]) -> None:
        # `up --wait` on the named services: starts them in dependency
        # order (a standby waits for its primary) and returns when healthy
        self.compose("up", "-d", "--wait", *services)

    def create_missing(self, services: list[str]) -> None:
        # containers and volumes for the named services, labelled by
        # Compose, without starting anything
        self.compose("up", "--no-start", *services)

    def container_image(self, node: str) -> tuple[str, str] | None:
        """(image id, image name) the node's container was created from,
        or None when it has no container. The id is what the database
        actually runs - exact, and local by definition, so a `docker run`
        on it never needs to pull. Not the Compose file's image: that
        follows ROS_POSTGRES_IMAGE as it is NOW, and once the service
        image is in the generated file too, "the first image that is not
        toxiproxy" can be the Python one."""
        ids = self.compose("ps", "-a", "-q", node).stdout.split()
        if not ids:
            return None
        found = self.run(
            ["docker", "inspect", "-f", "{{.Image}} {{.Config.Image}}", ids[0]]
        ).stdout.split()
        return found[0], found[1] if len(found) > 1 else found[0]

    def archive(self, volume: str, out_dir: Path, image: str) -> None:
        owner = f"{os.getuid()}:{os.getgid()}" if hasattr(os, "getuid") else "0:0"
        self.run(
            [
                "docker",
                "run",
                "--rm",
                # never pull mid-snapshot: the databases are stopped
                "--pull=never",
                "--entrypoint",
                "sh",
                "-v",
                f"{volume}:/data",
                "-v",
                f"{out_dir.resolve()}:/out",
                image,
                "-c",
                f"tar --numeric-owner -czf /out/{volume}.tar.gz -C /data . "
                f"&& chown {owner} /out/{volume}.tar.gz",
            ]
        )

    def unpack(self, volume: str, in_dir: Path, image: str) -> None:
        self.run(
            [
                "docker",
                "run",
                "--rm",
                # never pull mid-snapshot: the databases are stopped
                "--pull=never",
                "--entrypoint",
                "sh",
                "-v",
                f"{volume}:/data",
                "-v",
                f"{in_dir.resolve()}:/in:ro",
                image,
                "-c",
                "find /data -mindepth 1 -delete && "
                f"tar --numeric-owner -xzf /in/{volume}.tar.gz -C /data",
            ]
        )


# --- playback, over the control port -------------------------------------------


class PlaybackClient:
    """The process that hosts playback (`rosctl control`, or a standalone
    `rosctl playback start`), over HTTP. `status()` answers None when no
    such process listens: there is no run to pause or resume."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 10.0,
        stop_timeout_s: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url
        self._timeout_s = timeout_s
        self._stop_timeout_s = stop_timeout_s
        self._transport = transport

    async def _call(
        self, method: str, path: str, body: dict | None = None, timeout_s: float | None = None
    ) -> httpx.Response:
        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout_s or self._timeout_s,
            transport=self._transport,
        ) as client:
            return await client.request(method, path, json=body)

    async def status(self) -> dict[str, Any] | None:
        try:
            response = await self._call("GET", "/sim/playback")
        except httpx.ConnectError:
            return None
        response.raise_for_status()
        return response.json()

    async def clock(self) -> dict[str, Any]:
        response = await self._call("GET", "/sim/clock")
        response.raise_for_status()
        return response.json()

    async def command(self, body: dict[str, Any]) -> httpx.Response:
        stop = body.get("action") == "stop"
        return await self._call(
            "POST", "/sim/playback", body, timeout_s=self._stop_timeout_s if stop else None
        )

    async def speed(self, value: float) -> httpx.Response:
        return await self._call("PATCH", "/sim/playback", {"speed": value})

    async def heal_all(self) -> bool:
        """Heal-all through the control process (so its fault map is
        cleared too); False when the host is a standalone playback."""
        try:
            response = await self._call("POST", "/sim/faults/heal-all")
        except httpx.ConnectError:
            return False
        return response.status_code == 200


def _message(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text
    return str(body.get("message") or body.get("detail") or body)


# --- save -------------------------------------------------------------------------


@dataclass
class Context:
    """Everything save and restore touch, injectable for tests."""

    cluster_file: Path
    sim: SimSettings
    playback: PlaybackClient
    toxiproxy_factory: Callable[[], Toxiproxy] = Toxiproxy
    docker: Docker | None = None
    runs_dir: Path = Path("runs")
    generated_dir: Path = Path("infra/generated")
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    # wall time for the waits' deadlines (monotonic; tests pass a fake
    # that their sleep advances)
    clock: Callable[[], float] = time.monotonic
    services_down: Callable[[], None] | None = None
    # (cluster, cluster file, ports the precheck must skip: the listeners
    # of the proxies a restore cut again)
    services_up: Callable[[Cluster, Path, Sequence[int]], None] | None = None

    def __post_init__(self) -> None:
        if self.docker is None:
            self.docker = Docker()

    @property
    def snapshot_dir(self) -> Path:
        return self.sim.snapshot_dir


async def _pause_for_snapshot(ctx: Context) -> tuple[dict[str, Any] | None, bool]:
    """Pause a live run at a safe point. Returns (status, was_running).

    The point is the DISPATCH FRONTIER (the last item the engine sent),
    never the clock: the clock runs on wall time and runs ahead of
    dispatch whenever an event waits for its order's answer, and a
    snapshot whose day came from the clock could name day N+1 while
    dispatch was still in day N - restore would replay N+1 and the rest
    of N would never be sent. The wait and the recorded point both use
    the frontier: it must stand at least `minimum` into its own day."""
    status = await ctx.playback.status()
    if status is None or status.get("state") not in LIVE_STATES:
        return None, False
    was_running = status["state"] == "running"
    minimum = ctx.sim.snapshot_min_day_offset_s
    engine = status.get("engine")
    if not isinstance(engine, dict):
        raise SnapshotError(
            "the playback host does not report its engine settings (GET /sim/playback "
            "has no 'engine'): restart it from this checkout"
        )
    floor = offset_floor(engine)
    if minimum < floor:
        events = engine.get("events") or {}
        raise SnapshotError(
            f"ROS_SIM_SNAPSHOT_MIN_DAY_OFFSET_S is {minimum} s, but this run's last status "
            f"event of a day can be due {floor} s after midnight (the schedule's last "
            f"offset plus up to {events.get('jitter_s')} s jitter), and restore replays "
            f"only the snapshot's own day: raise it to at least {floor}"
        )
    # Dispatch can stall (a target that stopped answering holds the events
    # behind their orders), so the wait has a limit - but counted only
    # while something is DUE (the clock is past the next item still to go
    # out) and nothing moves. Waiting for an item that is not due yet is
    # not a stall however long it takes: at 1x a quiet night has twenty
    # real minutes between orders. Once something is due, sending it is
    # HTTP work, so the limit is wall time.
    limit = ctx.sim.snapshot_drain_timeout_s
    stalled_since: float | None = None
    while True:
        problem = point_problem(status, minimum)
        if problem is None:
            break
        if not was_running or status.get("state") != "running":
            raise SnapshotError(
                f"cannot snapshot here: {problem}. The restored day is replayed from "
                "midnight - resume the run until that clears first"
            )
        progress = (frontier_seconds(status), next_due_seconds(status))
        await ctx.sleep(0.1)
        status = await ctx.playback.status() or {}
        now = sim_seconds(await ctx.playback.clock())
        due = next_due_seconds(status)
        moved = (frontier_seconds(status), due) != progress
        if moved or due is None or now is None or now < due:
            stalled_since = None
        elif stalled_since is None:
            stalled_since = ctx.clock()
        elif ctx.clock() - stalled_since >= limit:
            raise SnapshotError(
                f"the run's dispatch has not moved for {limit:g} s with {_at(due)} due "
                f"and {problem}: no snapshot - is the target answering?"
            )
    if was_running:
        response = await ctx.playback.command({"action": "pause"})
        if response.status_code != 200:
            raise SnapshotError(f"cannot pause playback: {_message(response)}")
    # From here this call owns the pause: whatever fails before save takes
    # over (a status call that errors, the drain timeout, the frontier
    # check) resumes a run this call paused. A run that was already
    # paused is left as it was.
    try:
        # a real deadline, set before the first poll: each status call is
        # bounded by the time left, so a slow host cannot stretch it
        deadline = ctx.clock() + limit
        while True:
            left = deadline - ctx.clock()
            if left <= 0:
                raise SnapshotError(
                    f"{status.get('inflight')} requests still in flight after {limit:g} s"
                )
            try:
                status = await asyncio.wait_for(ctx.playback.status(), timeout=left) or {}
            except TimeoutError:
                raise SnapshotError(
                    f"the playback host did not answer within the {limit:g} s in-flight wait"
                ) from None
            if not status.get("inflight"):
                break
            await ctx.sleep(min(0.2, max(left, 0.0)))
        # dispatch is frozen now; it may have moved since the read above -
        # across midnight, or past a held order's day - so check again
        problem = point_problem(status, minimum)
        if problem is not None:
            raise SnapshotError(f"dispatch moved before the pause took: {problem} - try again")
        return status, was_running
    except BaseException as error:
        if not was_running:
            raise
        problem = await _resume_quietly(ctx)
        if problem is not None:
            # both failures, the first one first: the run is left paused
            raise SnapshotError(
                f"{_describe(error)}; no snapshot, and playback did NOT resume "
                f"(it is left paused): {problem}"
            ) from error
        if isinstance(error, SnapshotError):
            raise SnapshotError(f"{error}; playback resumed, no snapshot") from error
        raise


def _describe(error: BaseException) -> str:
    return str(error) if isinstance(error, SnapshotError) else f"{type(error).__name__}: {error}"


async def _resume_quietly(ctx: Context) -> str | None:
    """Resume the run; None when it resumed, else what went wrong."""
    try:
        response = await ctx.playback.command({"action": "resume"})
    except Exception as error:  # noqa: BLE001 - reported next to the first failure
        return _describe(error)
    if response.status_code != 200:
        return _message(response)
    return None


async def save(name: str, ctx: Context) -> dict[str, Any]:
    check_name(name)
    final = ctx.snapshot_dir / name
    if final.exists():
        raise SnapshotExistsError(f"snapshot {name!r} exists; choose another name")
    cluster = load_cluster(ctx.cluster_file)
    partial = ctx.snapshot_dir / f".{name}.partial"
    if partial.exists():
        shutil.rmtree(partial)  # left by a crashed snapshot
    status, was_running = await _pause_for_snapshot(ctx)
    failed = True
    # Playback resumes only when the databases are confirmed back - or
    # were never stopped: a run resumed against stopped databases fails
    # every send. `published` is the rename: the snapshot exists from then.
    stop_begun = databases_back = published = False
    try:
        partial.mkdir(parents=True)
        record: dict[str, Any] = {
            "name": name,
            "created_at": datetime.now(UTC).isoformat(),
            "sim_day": None,
            "sim_time": None,
            "clock_sim_day": None,
            "clock_sim_time": None,
            "regions": list(cluster.region_ids),
            "cluster_file": str(ctx.cluster_file),
            "run": None,
            "volumes": [volume_name(node) for node in database_services(cluster)],
        }
        if status is not None:
            clock = await ctx.playback.clock()
            run_id = status["run_id"]
            run_dir = ctx.runs_dir / run_id
            meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
            # the replay boundary is where dispatch stopped (what the run
            # log and the databases hold), not where the clock stands
            record["sim_day"] = status.get("dispatched_sim_day")
            record["sim_time"] = status.get("dispatched_sim_time")
            # the clock at the pause, for information only
            record["clock_sim_day"] = clock.get("sim_day")
            record["clock_sim_time"] = clock.get("sim_time")
            record["run"] = {
                "run_id": run_id,
                "seed": status.get("seed"),
                "speed": status.get("speed"),
                "from_day": status.get("from_day"),
                "to_day": status.get("to_day"),
                "duplicate_rate": meta.get("duplicate_rate", 0.0),
                "shuffle": meta.get("shuffle", False),
                # engine-wide facts: the host that resumes must match them
                "sequential": meta.get("sequential", False),
                "region": meta.get("region"),
                "target_url": meta.get("target_url"),
                # every engine fact, from the live host (GET /sim/playback)
                "engine": status.get("engine"),
            }
            shutil.copytree(run_dir, partial / "run" / run_id)
        shutil.copy2(ctx.cluster_file, partial / "cluster.toml")
        toxiproxy = ctx.toxiproxy_factory()
        try:
            proxies = await toxiproxy.list_proxies()
        finally:
            await toxiproxy.aclose()
        (partial / "toxiproxy.json").write_text(
            json.dumps(
                [
                    {
                        "name": proxy.get("name"),
                        "enabled": proxy.get("enabled", True),
                        "toxics": proxy.get("toxics") or [],
                    }
                    for proxy in proxies
                ],
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        services = database_services(cluster)
        docker = ctx.docker
        assert docker is not None
        found = await asyncio.to_thread(docker.container_image, HQ_PRIMARY)
        if found is None:
            raise SnapshotError(f"no {HQ_PRIMARY} container: there is no cluster to snapshot")
        image = found[0]
        # restore refuses to unpack these volumes under another PostgreSQL
        (partial / "image.json").write_text(
            json.dumps({"id": found[0], "name": found[1]}) + "\n", encoding="utf-8"
        )
        stop_begun = True
        first_error: BaseException | None = None
        try:
            # the stop is inside: one that fails half way has still
            # stopped some databases, and they come back below
            await asyncio.to_thread(docker.stop, services)
            for node in services:
                await asyncio.to_thread(docker.archive, volume_name(node), partial, image)
            (partial / SNAPSHOT_FILE).write_text(
                json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            partial.rename(final)
            published = True
        except BaseException as error:  # noqa: BLE001 - re-raised once the databases are back
            first_error = error
        # whatever happened after the stop began, the databases come back
        # (`up --wait`: healthy) before anything else
        try:
            await asyncio.to_thread(docker.start, services)
        except Exception as start_error:
            before = f" after {_describe(first_error)}" if first_error is not None else ""
            saved = "was saved" if published else "was not saved"
            raise SnapshotError(
                f"the databases did not come back{before}: {_describe(start_error)}. "
                f"Playback is left paused; snapshot {name!r} {saved}. Bring them back "
                "with: docker compose -p restaurantos -f infra/generated/compose.yaml "
                f"up -d --wait {' '.join(services)}"
            ) from start_error
        databases_back = True
        if first_error is not None:
            raise first_error
        failed = False
    finally:
        if failed and not published:
            shutil.rmtree(partial, ignore_errors=True)
        if was_running and (databases_back or not stop_begun):
            response = await ctx.playback.command({"action": "resume"})
            # a failed snapshot keeps its own error; a saved one reports
            # a resume that did not happen
            if response.status_code != 200 and not failed:
                raise SnapshotError(
                    f"snapshot {name!r} saved, but playback did not resume: {_message(response)}"
                )
    return record


def list_snapshots(snapshot_dir: Path) -> list[dict[str, Any]]:
    """Section 6.3: {name, created_at, sim_day, sim_time, regions} each."""
    if not snapshot_dir.is_dir():
        return []
    answer = []
    for entry in sorted(snapshot_dir.iterdir()):
        record_file = entry / SNAPSHOT_FILE
        if entry.name.startswith(".") or not record_file.is_file():
            continue
        record = json.loads(record_file.read_text(encoding="utf-8"))
        answer.append(
            {
                key: record.get(key)
                for key in ("name", "created_at", "sim_day", "sim_time", "regions")
            }
        )
    return answer


# --- restore ------------------------------------------------------------------------


def _restock_done(run_dir: Path, day: int) -> bool:
    """Whether `day`'s midnight restock already went out for this run,
    decided from its meta.json - the run's spec, written once at start -
    never from the run log.

    The snapshot point is past midnight of its day (save's minimum is at
    least 1 s, and the engine dispatches in one time order with in-flight
    at 0), so a restock day's batch has been sent and answered when:
    - the day is in the run's own restock days, or
    - the run is itself a restored replay of that day that skipped it
      (`from_day == day` and `restock.skipped_first`): the restock went
      out in the run it continues. Without this, saving a resumed run
      again on the same day and restoring THAT would restock twice."""
    try:
        meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    except OSError as error:
        raise SnapshotError(f"the snapshot's run has no readable meta.json: {error}") from error
    restock = meta.get("restock") or {}
    if day in (restock.get("days") or []):
        return True
    return bool(restock.get("skipped_first")) and meta.get("from_day") == day


FAILED_VENDORS_SHOWN = 10


def _failed_restock_vendors(runs_dir: Path, run_id: str, day: int) -> list[str] | None:
    """The vendors whose `day` restock failed (anything but a 200, or a
    transport error), from the logs of the run and the runs it continues.
    Informational only - it decides nothing: the replay skips the whole
    batch either way, as the original run went on without the failed
    ones. None when a log cannot be read."""
    failed: set[str] = set()
    acked: set[str] = set()
    try:
        for chain_run in run_chain(runs_dir, [run_id]):
            log = runs_dir / chain_run / "orders.jsonl"
            if not log.is_file():
                continue
            with log.open(encoding="utf-8") as lines:
                for text in lines:
                    if '"restock"' not in text:
                        continue
                    line = json.loads(text)
                    if line.get("kind") != "restock" or line.get("restock_day") != day:
                        continue
                    ok = "error" not in line and line.get("http_status") == 200
                    (acked if ok else failed).add(str(line.get("vendor_id")))
    except OSError, ValueError:
        return None
    return sorted(failed - acked)


def next_run_id(runs_dir: Path, run_id: str) -> str:
    base = run_id[:58]
    n = 1
    while (runs_dir / f"{base}-r{n}").exists():
        n += 1
    return f"{base}-r{n}"


def _check_complete(name: str, snap: Path, record: dict[str, Any], cluster: Cluster) -> None:
    """Everything restore will read must be there before it touches the
    cluster: every volume archive it unpacks (not empty), Toxiproxy's
    state, and the saved run. Restore replaces the cluster on purpose,
    and it does not stage the volumes (Docker cannot rename a volume, so
    staging would copy every volume twice on every restore): after this
    check only an I/O error mid-unpack can stop it half way, and the
    snapshot itself is untouched, so running the restore again is the
    recovery."""
    missing: list[str] = []
    for node in database_services(cluster):
        archive = snap / f"{volume_name(node)}.tar.gz"
        if not archive.is_file() or archive.stat().st_size == 0:
            missing.append(archive.name)
    if not (snap / "toxiproxy.json").is_file():
        missing.append("toxiproxy.json")
    run = record.get("run")
    if run and not (snap / "run" / run["run_id"] / "meta.json").is_file():
        missing.append(f"run/{run['run_id']}/meta.json")
    if missing:
        raise SnapshotError(
            f"snapshot {name!r} is incomplete, nothing was changed: missing or empty "
            + ", ".join(missing)
        )


def _saved_image(snap: Path) -> dict[str, str] | None:
    image_file = snap / "image.json"
    if not image_file.is_file():
        return None  # a snapshot from before the image was recorded
    return json.loads(image_file.read_text(encoding="utf-8"))


def _check_image(saved: dict[str, str] | None, found: tuple[str, str], state: str) -> None:
    if saved is not None and saved.get("id") != found[0]:
        raise SnapshotError(
            f"the snapshot was taken with PostgreSQL image {saved.get('name')} "
            f"({saved.get('id')}), but the databases now use {found[1]} ({found[0]}); "
            "a data directory only starts under the PostgreSQL it was written by. "
            f"Set ROS_POSTGRES_IMAGE back and recreate the containers ({state})"
        )


async def _stop_live_run(ctx: Context) -> None:
    status = await ctx.playback.status()
    if status is None or status.get("state") not in (*LIVE_STATES, "stopping"):
        return
    response = await ctx.playback.command({"action": "stop"})
    if response.status_code not in (200, 409):
        raise SnapshotError(f"cannot stop the live run: {_message(response)}")


async def _restore_toxiproxy(
    ctx: Context, cluster: Cluster, saved: list[dict]
) -> tuple[list[str], list[str]]:
    """Put the saved proxy state back; returns (the toxics it could not,
    the proxies it cut again)."""
    skipped: list[str] = []
    cut: list[str] = []
    toxiproxy = ctx.toxiproxy_factory()
    try:
        await toxiproxy.sync(cluster)
        # through the control process when it runs, so its fault map is
        # cleared too; the restored toxics then show as unexplained there
        if not await ctx.playback.heal_all():
            await toxiproxy.reset()
        existing = {str(proxy.get("name")) for proxy in await toxiproxy.list_proxies()}
        for proxy in saved:
            name = str(proxy.get("name"))
            if name not in existing:
                continue
            if not proxy.get("enabled", True):
                await toxiproxy.set_enabled(name, False)
                cut.append(name)
            # TODO(wp6b/faults-control): re-add the saved toxics with
            # Toxiproxy.add_toxic once that PR is on main; until then a
            # restore puts back cut proxies only and reports the toxics.
            for toxic in proxy.get("toxics") or []:
                skipped.append(f"{name}/{toxic.get('name')}")
    finally:
        await toxiproxy.aclose()
    return skipped, cut


# the engine facts a replay must share with the run it continues; inflight
# changes how fast the target is driven, not what is sent
ENGINE_KEYS = (
    "sequential",
    "region",
    "target_url",
    "events",
    "restock_every",
    "duplicate_offset_s",
    "shuffle_window",
    "kitchen",
)


def engine_mismatches(run: dict[str, Any], host: dict[str, Any] | None) -> list[str]:
    """One line per engine fact the playback host does not share with the
    snapshot's run; empty when the replay can start there."""
    if not isinstance(host, dict):
        return ["the playback host does not report its engine settings: restart it"]
    saved = run.get("engine")
    keys = ENGINE_KEYS if saved else ("sequential", "region", "target_url")
    saved = saved or run
    lines = []
    for key in keys:
        want, have = saved.get(key), host.get(key)
        if key == "sequential":
            want, have = bool(want), bool(have)
        elif key == "target_url":
            want = _api_base(str(want)) if want else want
            have = _api_base(str(have)) if have else have
        if want != have:
            lines.append(f"the playback host's {key} is {have!r}, the snapshot's run had {want!r}")
    return lines


def _num(value: Any) -> str:
    return f"{float(value):g}"


def _api_base(url: str) -> str:
    return url.rstrip("/").removesuffix("/orders")


def replay_command(
    run: dict[str, Any],
    *,
    day: int,
    run_id: str,
    speed: float,
    skip_first_restock: bool,
    sim: SimSettings,
    cluster: Cluster,
) -> tuple[str, list[str]]:
    """The `rosctl playback start` that resumes a restored run by hand,
    and notes for what no flag can set.

    Minimal but exact: the flags with no meaningful default for a replay
    are always there (the day, the window, the seed, the run id, the
    speed, the restock skip); every other setting gets a flag only where
    the saved run differs from what a start would pick up from the
    current settings (ROS_SIM_*, .env) - the settings this restore just
    loaded, the same ones the command will see when run from this
    checkout and shell."""
    flags = [f"--from-day {day}", f"--to-day {run['to_day']}", f"--seed {run['seed']}"]
    flags += [f"--run-id {run_id}", f"--speed {_num(speed)}"]
    if skip_first_restock:
        flags.append("--skip-first-restock")
    notes: list[str] = []
    rate = float(run.get("duplicate_rate", 0.0))
    if rate != sim.playback_duplicate_rate:
        flags.append(f"--duplicate-rate {_num(rate)}")
    shuffle = bool(run.get("shuffle", False))
    if shuffle != sim.playback_shuffle:
        flags.append("--shuffle" if shuffle else "--no-shuffle")
    # a snapshot from before the engine block: the three facts meta had
    engine = run.get("engine") or {
        key: run.get(key) for key in ("sequential", "region", "target_url")
    }
    sequential = bool(engine.get("sequential"))
    region = engine.get("region")
    if sequential:
        flags.append("--sequential")
    if region is not None:
        flags.append(f"--region {region}")
    target = engine.get("target_url")
    if target:
        base = _api_base(str(target))
        edge = None
        if region is not None and region in cluster.region_ids:
            from restaurantos.sim.regions import edge_target_url

            edge = _api_base(edge_target_url(cluster, region))
        if base == edge:
            flags.append("--target edge")
        elif base != _api_base(sim.playback_target_url):
            flags.append(f"--target {base}")
    inflight = engine.get("inflight")
    if inflight is not None and not sequential and inflight != sim.playback_inflight:
        flags.append(f"--inflight {inflight}")
    events = engine.get("events")
    if events:
        if bool(events.get("enabled")) != sim.playback_status_events:
            flags.append("--events" if events.get("enabled") else "--no-events")
        if events.get("enabled") and events.get("sample_rate") != sim.playback_event_sample_rate:
            flags.append(f"--event-sample-rate {_num(events.get('sample_rate'))}")
        if events.get("enabled") and events.get("jitter_s") != sim.playback_event_jitter_s:
            notes.append(
                f"set ROS_SIM_PLAYBACK_EVENT_JITTER_S={_num(events.get('jitter_s'))} first "
                "(it has no flag)"
            )
    if "restock_every" in engine:
        every = engine.get("restock_every")
        if every is None:
            notes.append("the saved run had no restock, which rosctl playback start cannot set")
        elif every != sim.stock_restock_days:
            flags.append(f"--restock-days {every}")
    offset = engine.get("duplicate_offset_s")
    if offset is not None and offset != sim.playback_duplicate_offset_s:
        flags.append(f"--duplicate-offset-s {_num(offset)}")
    window = engine.get("shuffle_window")
    if window is not None and window != sim.shuffle_window:
        flags.append(f"--shuffle-window {window}")
    kitchen = engine.get("kitchen") or {}
    if kitchen.get("mode") == "override":
        flags.append(f"--kitchen-target {kitchen.get('url')}")
    elif kitchen.get("mode") == "edge":
        from restaurantos.sim.playback_cli import kitchen_urls

        if kitchen.get("urls") != kitchen_urls(cluster, sim.playback_edge_host):
            notes.append(
                f"the saved run sent kitchen calls to {kitchen.get('urls')}: set "
                "ROS_SIM_PLAYBACK_EDGE_HOST to match first (it has no flag)"
            )
    elif kitchen.get("mode") == "target":
        notes.append(
            "the saved run sent kitchen calls to its order target, which "
            "rosctl playback start cannot set"
        )
    if engine is not run.get("engine"):
        notes.append(
            "this snapshot predates the recorded engine settings: add any other "
            "start flag the original run used"
        )
    else:
        notes.append(
            "flags left out match your current settings (ROS_SIM_*, .env); if you "
            "change those first, add the flags back"
        )
    return "rosctl playback start " + " ".join(flags), notes


async def _resume(
    ctx: Context, record: dict[str, Any], snap: Path, cluster: Cluster
) -> dict[str, Any]:
    run = record["run"]
    day = int(record["sim_day"])
    point = sim_seconds({"sim_day": record["sim_day"], "sim_time": record["sim_time"]})
    assert point is not None
    new_run = next_run_id(ctx.runs_dir, run["run_id"])
    saved_speed = float(run["speed"])
    catchup = max(saved_speed, ctx.sim.snapshot_catchup_speed)
    skip = _restock_done(ctx.runs_dir / run["run_id"], day)
    body: dict[str, Any] = {
        "action": "start",
        "from_day": day,
        "to_day": run["to_day"],
        "seed": run["seed"],
        "speed": catchup,
        "run_id": new_run,
        "duplicate_rate": run.get("duplicate_rate", 0.0),
        "shuffle": bool(run.get("shuffle", False)),
        "skip_first_restock": skip,
    }
    resume: dict[str, Any] = {
        "run_id": new_run,
        "parent_run": run["run_id"],
        "from_day": day,
        "skip_first_restock": skip,
        "restock_failed_vendors": [],
        "speed": saved_speed,
        "catchup_speed": catchup,
        "warnings": [],
    }
    if skip:
        failed_vendors = _failed_restock_vendors(ctx.runs_dir, run["run_id"], day)
        if failed_vendors is None:
            resume["warnings"].append(
                f"could not read the run log to list day {day}'s failed restocks"
            )
        elif failed_vendors:
            resume["restock_failed_vendors"] = failed_vendors
            shown = ", ".join(failed_vendors[:FAILED_VENDORS_SHOWN])
            more = len(failed_vendors) - FAILED_VENDORS_SHOWN
            resume["warnings"].append(
                f"day {day}'s restock failed in the original run for {len(failed_vendors)} "
                f"vendor(s): {shown}{f' and {more} more' if more > 0 else ''}. The replay "
                "skips the whole batch, as the original run went on without them "
                "(restock_failed_vendors lists every one)"
            )
    manual, notes = replay_command(
        run,
        day=day,
        run_id=new_run,
        speed=saved_speed,
        skip_first_restock=skip,
        sim=ctx.sim,
        cluster=cluster,
    )
    resume["manual_command"] = manual
    host = await ctx.playback.status()
    if host is None:
        resume["started"] = False
        resume["warnings"].append(
            f"no playback host at {ctx.playback.base_url}; resume with: {manual}"
        )
        resume["warnings"] += notes
        return resume
    # The host's engine settings are fixed when it starts; a start body
    # cannot change them. Compare BEFORE the replay starts: a gateway
    # snapshot replayed on a single-region host would send with that
    # host's filter from its first order on.
    mismatches = engine_mismatches(run, host.get("engine"))
    if mismatches:
        resume["started"] = False
        resume["warnings"] += mismatches
        resume["warnings"].append(
            "the replay was not started: stop that playback host and resume with: " + manual
        )
        resume["warnings"] += notes
        return resume
    response = await ctx.playback.command(body)
    if response.status_code != 200:
        resume["started"] = False
        resume["warnings"].append(f"the resumed run did not start: {_message(response)}")
        return resume
    resume["started"] = True
    new_dir = ctx.runs_dir / new_run
    if new_dir.is_dir():
        (new_dir / RESUMED_FROM).write_text(
            json.dumps(
                {
                    "parent_run": run["run_id"],
                    "snapshot": record["name"],
                    "sim_day": record["sim_day"],
                    "sim_time": record["sim_time"],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    if catchup > saved_speed:
        switch_at = point - ctx.sim.snapshot_catchup_margin_s
        while True:
            clock = await ctx.playback.clock()
            now = sim_seconds(clock)
            if clock.get("state") != "running" or now is None or now >= switch_at:
                break
            await ctx.sleep(0.05)
        if clock.get("state") == "running":
            changed = await ctx.playback.speed(saved_speed)
            if changed.status_code != 200:
                resume["warnings"].append(
                    f"could not switch back to {saved_speed:g}x: {_message(changed)}"
                )
    return resume


async def restore(name: str, ctx: Context, *, replace_cluster_file: bool = False) -> dict[str, Any]:
    check_name(name)
    snap = ctx.snapshot_dir / name
    record_file = snap / SNAPSHOT_FILE
    if not record_file.is_file():
        raise UnknownSnapshotError(f"no snapshot {name!r} in {ctx.snapshot_dir}")
    record = json.loads(record_file.read_text(encoding="utf-8"))
    if not (snap / "cluster.toml").is_file():
        raise SnapshotError(
            f"snapshot {name!r} is incomplete (no cluster.toml); nothing was changed"
        )
    snapshot_toml = (snap / "cluster.toml").read_text(encoding="utf-8")
    current_toml = (
        ctx.cluster_file.read_text(encoding="utf-8") if ctx.cluster_file.is_file() else None
    )
    differs = current_toml != snapshot_toml
    if differs and not replace_cluster_file:
        raise SnapshotError(
            f"the snapshot's cluster.toml differs from {ctx.cluster_file}; restore would "
            "change the cluster's shape - pass --replace-cluster-file to write the "
            "snapshot's copy over it"
        )
    snapshot_cluster = load_cluster(snap / "cluster.toml")
    _check_complete(name, snap, record, snapshot_cluster)
    current_cluster = load_cluster(ctx.cluster_file) if current_toml is not None else None
    docker = ctx.docker
    assert docker is not None

    # the volumes only start under the PostgreSQL they were saved with:
    # refuse a mismatch before anything is stopped (and again after
    # create_missing, for a cluster that had no containers yet)
    saved_image = _saved_image(snap)
    running = await asyncio.to_thread(docker.container_image, HQ_PRIMARY)
    if running is not None:
        _check_image(saved_image, running, "nothing was changed")
    await _stop_live_run(ctx)
    if ctx.services_down is not None:
        await asyncio.to_thread(ctx.services_down)
    if current_cluster is not None:
        # the databases of the shape running now, from today's generated file
        await asyncio.to_thread(docker.stop, database_services(current_cluster))
    if differs:
        shutil.copy2(snap / "cluster.toml", ctx.cluster_file)
    # the binding rule: regenerate from the snapshot's cluster file before
    # any container starts
    from restaurantos.sim.generator import generate

    generate(snapshot_cluster, ctx.generated_dir, snapshot_toml)
    services = database_services(snapshot_cluster)
    await asyncio.to_thread(docker.create_missing, services)
    found = await asyncio.to_thread(docker.container_image, HQ_PRIMARY)
    if found is None:
        raise SnapshotError(f"no {HQ_PRIMARY} container after compose up --no-start")
    _check_image(saved_image, found, "nothing is unpacked yet; the databases are stopped")
    image = found[0]
    for node in services:
        await asyncio.to_thread(docker.unpack, volume_name(node), snap, image)
    if record.get("run"):
        run_id = record["run"]["run_id"]
        target = ctx.runs_dir / run_id
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(snap / "run" / run_id, target)
    await asyncio.to_thread(docker.start, services)
    saved = json.loads((snap / "toxiproxy.json").read_text(encoding="utf-8"))
    skipped_toxics, cut = await _restore_toxiproxy(ctx, snapshot_cluster, saved)
    if ctx.services_up is not None:
        # A cut proxy does not listen, and rosctl up's precheck dials every
        # proxy the services use: name the cut ones so it skips them. The
        # cut stays in place - starting healed and cutting afterwards would
        # let the relay drain the backlog the snapshot held behind it.
        ports = {proxy.name: proxy.port for proxy in proxies(snapshot_cluster)}
        expected_down = sorted(ports[name] for name in cut if name in ports)
        await asyncio.to_thread(ctx.services_up, snapshot_cluster, ctx.cluster_file, expected_down)
    result: dict[str, Any] = {
        "name": name,
        "sim_day": record.get("sim_day"),
        "sim_time": record.get("sim_time"),
        "cluster_file_replaced": differs,
        "resume": None,
        "warnings": [],
    }
    if skipped_toxics:
        result["warnings"].append(
            f"not restored (needs the faults-control PR): toxics {', '.join(skipped_toxics)}"
        )
    if differs:
        result["warnings"].append(
            f"{ctx.cluster_file} now holds the snapshot's cluster shape; restart any "
            "process that read the old one"
        )
    if record.get("run"):
        result["resume"] = await _resume(ctx, record, snap, snapshot_cluster)
    return result


# --- the services, through rosctl up/down's own code ---------------------------------


def services_down() -> None:
    from restaurantos.sim.services import RUNS_DIR, down_exit_code, down_services, require_proc

    require_proc()
    steps = down_services(runs_dir=RUNS_DIR)
    if down_exit_code(steps):
        failed = ", ".join(f"{step.name}: {step.outcome}" for step in steps)
        raise SnapshotError(f"rosctl down left services behind ({failed})")


def services_up(cluster: Cluster, cluster_file: Path, expected_down: Sequence[int] = ()) -> None:
    from restaurantos.sim.services import RUNS_DIR, require_proc, up_exit_code, up_services

    require_proc()
    steps = up_services(
        cluster, runs_dir=RUNS_DIR, cluster_file=cluster_file, expected_down=expected_down
    )
    if up_exit_code(steps):
        failed = ", ".join(f"{step.name}: {step.outcome}" for step in steps)
        raise SnapshotError(f"rosctl up did not bring every service back ({failed})")


def default_context(cluster_file: Path, sim: SimSettings, *, port: int | None = None) -> Context:
    base = f"http://{sim.control_host}:{port or sim.control_port}"
    return Context(
        cluster_file=cluster_file,
        sim=sim,
        playback=PlaybackClient(
            base,
            timeout_s=sim.http_timeout_s,
            stop_timeout_s=sim.playback_drain_timeout_s + sim.http_timeout_s + 5.0,
        ),
        services_down=services_down,
        services_up=services_up,
    )

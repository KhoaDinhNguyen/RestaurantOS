"""The fault scenarios T1, T1b and T2 to T6 (WP6b, Phase 2 Table 9).

Every scenario runs the same script inside the control process, which
holds the playback engine, the fault layer and the nodes:

1. start a playback run (one or more whole days, see below);
2. wait until the simulated clock reaches the injection moment, by
   default 11:00 of the first day (the lunch peak), so the fault lands
   under load;
3. break something, hold it for `fault_s` wall seconds, repair it;
4. let the run finish its window, wait for the queues to drain and run
   the invariant checker;
5. write `runs/<run_id>/scenario.json`, which is also the job's result.

What each one breaks, and when it passes (the table is in architecture
Section 7.3):

- T1: `cut` on `node:<the region's current primary>`. Passes when the
  standby is promoted, orders commit again and the checker passes.
- T1b: `docker kill` of the current primary, started again at the heal.
  Passes as T1.
- T2: `cut` on `link:<region><->hq`. Passes when there is no failover,
  the holding queue drains after the heal and the checker passes.
- T3: T2, then kill -9 of `relay-<region>` and `hq-worker` during the
  drain, both started again after `down_s`. Passes as T2, and both
  come back.
- T4: no fault; the run resends `duplicate_rate` of its orders and
  shuffles delivery. Passes when the checker passes strictly (one order
  per external id).
- T5: `latency` on `node:<the region's current primary>`. Passes when
  there is no failover and the checker passes.
- T6: `reset` on `link:<region><->hq`, then HQ publishes a new menu
  version. Passes when the region never shows half a version, ends on
  the new one, and the checker passes.

**Why a run covers whole days.** The checker compares every order stored
in the run's days with the run log, so a run must send every order of
its window: a run stopped halfway would leave orders an earlier run
stored on those days that this run never sent. The window is sized to
outlast the script (injection moment plus fault, settle and T3's kill
time, at the run's speed) and the run ends by itself. By default it
starts on the first day after every earlier run's window, so each
scenario writes new orders; when the data has no such days left it
replays from the first day after the warm-up, and orders already stored
are answered as duplicates.

**T1 and T1b leave the old primary fenced.** It is no longer a standby,
so a second T1 or T1b on the region is refused until it is rejoined
(`next_steps` in the result names the command).

The database reads go straight to the nodes from the host
(`host_conninfo`, never through Toxiproxy): a scenario has to read a
region it has just cut off.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster, host_conninfo
from restaurantos.common.simclock import SECONDS_PER_DAY
from restaurantos.sim.toxiproxy import ToxiproxyError

LIVE_STATES = ("running", "paused", "stopping")
RESULT_FILE = "scenario.json"
# simulated seconds the window keeps past the script's end: a run ends at
# its last order, which can come well before the midnight of its last day
TAIL_MARGIN_S = 3 * 3600
REJOIN = "uv run python -m restaurantos.coordinator.rejoin {rid} --compose-file {compose}"
COMPOSE_FILE = "infra/generated/compose.yaml"


class ScenarioError(Exception):
    """A scenario request the control process refuses, or a scenario
    that could not go on. `status` and `code` are the control API's
    answer (architecture Section 6.3)."""

    status = 409
    code = "scenario_refused"


class UnknownScenarioError(ScenarioError):
    status = 404
    code = "unknown_scenario"


class ScenarioParamsError(ScenarioError, ValueError):
    status = 422
    code = "invalid_params"


class ScenarioNotBuiltError(ScenarioError):
    """The checker module is not on this branch, so no scenario can run
    (its verdict needs the checker). Answers `501 not_built`, like the
    snapshot and check routes."""

    status = 501
    code = "not_built"


# --- the scenarios and their parameters --------------------------------------


@dataclass(frozen=True)
class Spec:
    name: str
    summary: str
    fault_s: float  # default hold, in wall seconds
    extras: tuple[str, ...] = ()  # parameters beyond the common ones
    # strict: the checker fails orders that are stored though every send
    # of them errored; fault scenarios allow them (a timeout that still
    # landed is normal under a fault)
    strict: bool = False


SPECS: dict[str, Spec] = {
    spec.name: spec
    for spec in (
        Spec("T1", "isolate the region's primary (cut node:<primary>); the standby takes over", 30),
        Spec(
            "T1b", "kill the region's primary container (a real crash); the standby takes over", 30
        ),
        Spec("T2", "cut the region off from HQ; the region keeps working, HQ parks its orders", 60),
        Spec(
            "T3",
            "T2, then kill -9 the region's relay and the HQ worker during the drain",
            60,
            ("kill_after_s", "down_s"),
        ),
        Spec(
            "T4",
            "no fault: resend a share of the orders and shuffle delivery",
            60,
            ("duplicate_rate", "shuffle"),
            strict=True,
        ),
        Spec(
            "T5",
            "slow the region's primary (latency on node:<primary>); no failover",
            30,
            ("latency_ms", "jitter_ms"),
        ),
        Spec(
            "T6",
            "reset the region's links to HQ while HQ publishes a new menu version",
            20,
            ("reset_timeout_ms",),
        ),
    )
}

COMMON = ("region_id", "from_day", "speed", "seed", "inject_at", "fault_s", "settle_s")


@dataclass(frozen=True)
class Params:
    region_id: str = "r1"
    from_day: int | None = None  # None: the first day no earlier run covered
    speed: float | None = None  # None: the playback default
    seed: int = 0
    inject_at: str = "11:00:00"  # simulated time of day on the first day
    fault_s: float = 0.0  # filled from the scenario's default
    settle_s: float = 30.0  # wall seconds of load after the heal
    kill_after_s: float = 1.0  # T3: from the heal to the kill
    down_s: float = 5.0  # T3: how long the killed services stay down
    duplicate_rate: float = 0.08  # T4
    shuffle: bool = True  # T4
    latency_ms: int = 100  # T5, each direction; measured: 1000 promotes the standby
    jitter_ms: int = 0  # T5
    reset_timeout_ms: int = 200  # T6: a connection lives this long, then is reset

    def shown(self, spec: Spec) -> dict[str, Any]:
        """The parameters that apply to `spec`, for the result file."""
        return {key: getattr(self, key) for key in (*COMMON, *spec.extras)}


def spec_for(name: str) -> Spec:
    for key, spec in SPECS.items():
        if key.lower() == name.lower():
            return spec
    raise UnknownScenarioError(f"no scenario {name!r}; one of {', '.join(SPECS)}")


def seconds_of_day(text: str) -> int:
    """`HH:MM:SS` as seconds since midnight."""
    try:
        hours, minutes, seconds = (int(part) for part in text.split(":"))
    except AttributeError, ValueError:
        raise ScenarioParamsError(f"inject_at must be HH:MM:SS, not {text!r}") from None
    if not (0 <= hours < 24 and 0 <= minutes < 60 and 0 <= seconds < 60):
        raise ScenarioParamsError(f"inject_at must be a time of day, not {text!r}")
    return hours * 3600 + minutes * 60 + seconds


def _number(key: str, value: Any, *, minimum: float, above: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ScenarioParamsError(f"{key} must be a number")
    try:
        finite = math.isfinite(value)
    except OverflowError:  # an int too large for a float
        raise ScenarioParamsError(f"{key} is too large") from None
    if not finite:
        raise ScenarioParamsError(f"{key} must be a number")
    if value < minimum or (above and value == minimum):
        relation = "above" if above else "at least"
        raise ScenarioParamsError(f"{key} must be {relation} {minimum:g}")
    return float(value)


def _whole(key: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ScenarioParamsError(f"{key} must be a whole number, 0 or more")
    return value


def parse_params(
    name: str, body: Mapping[str, Any] | None, cluster: Cluster
) -> tuple[Spec, Params]:
    """Check a scenario request against Section 6.3. An unknown key is
    refused rather than ignored, so a typo never runs a scenario with a
    default nobody asked for."""
    spec = spec_for(name)
    given = dict(body or {})
    allowed = (*COMMON, *spec.extras)
    unknown = sorted(set(given) - set(allowed))
    if unknown:
        raise ScenarioParamsError(
            f"{spec.name} does not take {', '.join(unknown)} (it takes: {', '.join(allowed)})"
        )
    values: dict[str, Any] = {}
    for key, value in given.items():
        if value is None and key in ("from_day", "speed"):
            continue
        if key == "region_id":
            if value not in cluster.region_ids:
                raise ScenarioParamsError(
                    f"no region {value!r}; this cluster has {', '.join(cluster.region_ids)}"
                )
            values[key] = value
        elif key in ("from_day", "seed", "latency_ms", "jitter_ms", "reset_timeout_ms"):
            values[key] = _whole(key, value)
        elif key == "inject_at":
            seconds_of_day(value)
            values[key] = value
        elif key in ("speed", "fault_s"):
            values[key] = _number(key, value, minimum=0, above=True)
        elif key == "duplicate_rate":
            rate = _number(key, value, minimum=0)
            if rate > 1:
                raise ScenarioParamsError("duplicate_rate must be a share in [0, 1]")
            values[key] = rate
        elif key == "shuffle":
            if not isinstance(value, bool):
                raise ScenarioParamsError("shuffle must be true or false")
            values[key] = value
        else:  # settle_s, kill_after_s, down_s
            values[key] = _number(key, value, minimum=0)
    values.setdefault("fault_s", float(spec.fault_s))
    return spec, Params(**values)


def describe() -> list[dict[str, Any]]:
    """`GET /sim/scenarios`: every scenario with its defaults."""
    defaults = Params()
    return [
        {
            "name": spec.name,
            "summary": spec.summary,
            "params": {
                key: (spec.fault_s if key == "fault_s" else getattr(defaults, key))
                for key in (*COMMON, *spec.extras)
            },
        }
        for spec in SPECS.values()
    ]


# --- what the scenarios read from the databases ------------------------------


class ClusterProbe:
    """Plain psycopg connections from the host straight to each node.
    Blocking: the scenarios call it through `asyncio.to_thread`."""

    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster

    def _rows(self, node: str, sql: str, params: Any = None) -> list[tuple]:
        import psycopg

        with psycopg.connect(
            host_conninfo(self.cluster, node),
            connect_timeout=5,
            options="-c statement_timeout=5000",
        ) as conn:
            return conn.execute(sql, params).fetchall()

    def hq(self) -> str:
        """The HQ node that is not in recovery (HQ may have been promoted)."""
        candidates = [HQ_PRIMARY, HQ_STANDBY] if self.cluster.standbys else [HQ_PRIMARY]
        found = []
        for node in candidates:
            with contextlib.suppress(Exception):  # a down node is a fact here
                if not self._rows(node, "SELECT pg_is_in_recovery()")[0][0]:
                    found.append(node)
        if len(found) != 1:
            raise ScenarioError(f"expected exactly one HQ primary, found {found or 'none'}")
        return found[0]

    def region(self, region_id: str) -> dict[str, Any]:
        rows = self._rows(
            self.hq(),
            "SELECT primary_node, standby_node, epoch FROM region_state WHERE region_id = %s",
            (region_id,),
        )
        if not rows:
            raise ScenarioError(f"region {region_id} is not in HQ's region_state")
        primary, standby, epoch = rows[0]
        return {"primary": primary, "standby": standby, "epoch": epoch}

    def streaming(self, node: str) -> int:
        """How many standbys stream from `node` right now."""
        return self._rows(
            node, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"
        )[0][0]

    def last_event_id(self) -> int:
        return self._rows(
            self.hq(), "SELECT coalesce(max(cluster_event_id), 0) FROM cluster_events"
        )[0][0]

    def failovers_since(self, after_id: int, region_id: str) -> list[dict[str, Any]]:
        rows = self._rows(
            self.hq(),
            "SELECT cluster_event_id, occurred_at, node_id FROM cluster_events "
            "WHERE kind = 'failover' AND region_id = %s AND cluster_event_id > %s "
            "ORDER BY cluster_event_id",
            (region_id, after_id),
        )
        return [{"event_id": eid, "occurred_at": at, "node_id": node} for eid, at, node in rows]

    def first_commit_after(
        self, node: str, after: datetime, days: tuple[int, int]
    ) -> datetime | None:
        return self._rows(
            node,
            "SELECT min(committed_at) FROM orders "
            "WHERE order_day BETWEEN %s AND %s AND committed_at > %s",
            (days[0], days[1], after),
        )[0][0]

    def holding_left(self, region_id: str) -> int:
        return self._rows(
            self.hq(),
            "SELECT count(*) FROM holding_queue WHERE region_id = %s AND delivered_at IS NULL",
            (region_id,),
        )[0][0]

    def outbox_left(self, node: str) -> int:
        """Undelivered rows of the region's outbox on its current primary
        (the relay's own queue; T3 asks whether work was pending)."""
        return self._rows(node, "SELECT count(*) FROM outbox WHERE relayed_at IS NULL")[0][0]

    def probe_source(self, node: str) -> dict[str, Any]:
        """A product of the region to copy the dish link and price from."""
        rows = self._rows(
            node,
            "SELECT p.vendor_id, pd.dish_id, pd.multiplier, p.price_units "
            "FROM products AS p JOIN product_dish AS pd USING (vendor_id, product_id) "
            "WHERE p.origin = 'hq' ORDER BY p.vendor_id, p.product_id LIMIT 1",
        )
        if not rows:
            raise ScenarioError(f"{node} has no product to copy a dish link from")
        vendor_id, dish_id, multiplier, price_units = rows[0]
        return {
            "vendor_id": vendor_id,
            "dish_id": dish_id,
            "multiplier": multiplier,
            "price_units": price_units,
        }

    def menu_state(self, node: str, vendor_id: str, product_id: str) -> dict[str, Any]:
        """The region's menu version, how many products claim a newer one
        (a half-applied version would show here), and the probe product's
        version (None while the region does not have it)."""
        version, ahead, probe = self._rows(
            node,
            "SELECT n.menu_version, "
            "(SELECT count(*) FROM products AS p "
            " WHERE p.origin = 'hq' AND p.menu_version > n.menu_version), "
            "(SELECT p.menu_version FROM products AS p "
            " WHERE p.vendor_id = %s AND p.product_id = %s) "
            "FROM node_state AS n",
            (vendor_id, product_id),
        )[0]
        return {"version": version, "ahead": ahead, "probe": probe}


# --- killing and restarting a Python service (T3) ----------------------------


class Services:
    """kill -9 and restart one service, in whichever mode it runs: host
    mode (`rosctl up`, a pid file under runs/services/) or container
    mode (the `services` profile of the generated Compose file)."""

    def __init__(
        self,
        cluster: Cluster,
        cluster_file: Path,
        *,
        runs_dir: Path = Path("runs"),
        compose: Callable[..., Any] | None = None,
        container_start_wait_s: float = 15.0,
        container_stable_s: float = 3.0,
    ) -> None:
        self._cluster = cluster
        self._cluster_file = cluster_file
        self._runs_dir = runs_dir
        self._compose = compose
        self._container_start_wait_s = container_start_wait_s
        self._container_stable_s = container_stable_s

    def _compose_call(self, *args: str) -> Any:
        if self._compose is not None:
            return self._compose(*args)
        from restaurantos.sim.bootstrap import _compose

        return _compose(*args)

    def _compose_running(self, name: str) -> bool:
        from restaurantos.sim.nodes import parse_running

        result = self._compose_call("--profile", "services", "ps", "--format", "json", name)
        return parse_running(result.stdout)

    def _mode(self, name: str) -> str:
        from restaurantos.sim.services import identity, pid_path, read_pid_record, service_specs

        record = read_pid_record(self._runs_dir, name)
        host_live = False
        unknown: str | None = None
        if isinstance(record, tuple):
            pid, ticks = record
            spec = {row.name: row for row in service_specs(self._cluster)}.get(name)
            if spec is None:
                unknown = f"no service {name} in this cluster ({pid_path(self._runs_dir, name)})"
            else:
                who = identity(pid, ticks, spec)
                if who == "match":
                    host_live = True
                elif who == "unknown":
                    unknown = str(pid_path(self._runs_dir, name))
        elif isinstance(record, str):
            unknown = f"{record} ({pid_path(self._runs_dir, name)})"
        container_live = self._compose_running(name)
        if host_live and container_live:
            raise ScenarioError(f"{name} runs both from rosctl up and as a container; stop one")
        if host_live:
            return "host"
        if container_live:
            return "container"
        if unknown is not None:
            raise ScenarioError(
                f"cannot tell whether {name} runs from rosctl up - inspect {unknown}"
            )
        raise ScenarioError(
            f"{name} is not running: no pid file from `rosctl up` and no running container"
        )

    def _kill(self, name: str) -> dict[str, Any]:
        mode = self._mode(name)
        if mode == "host":
            from restaurantos.sim.services import down_exit_code, down_services

            # no grace: SIGKILL follows SIGINT at once, so the service
            # stops wherever it was, mid-batch included
            step = down_services(
                self._cluster, runs_dir=self._runs_dir, names=[name], stop_timeout_s=0.0
            )[0]
            ok = down_exit_code([step]) == 0
            return {
                "service": name,
                "mode": mode,
                "ok": ok,
                "detail": f"{step.outcome}: {step.detail}",
            }
        self._compose_call("--profile", "services", "kill", name)
        return {"service": name, "mode": mode, "ok": True, "detail": "docker compose kill"}

    def _start(self, name: str, mode: str) -> dict[str, Any]:
        if mode == "host":
            from restaurantos.sim.services import up_exit_code, up_services

            step = up_services(
                self._cluster,
                runs_dir=self._runs_dir,
                names=[name],
                cluster_file=self._cluster_file,
            )[0]
            ok = up_exit_code([step]) == 0
            return {
                "service": name,
                "mode": mode,
                "ok": ok,
                "detail": f"{step.outcome}: {step.detail}",
            }
        self._compose_call("--profile", "services", "start", name)
        # compose start returns before a worker that crashes on start has
        # exited: wait until it runs, then check it is still running
        deadline = time.monotonic() + self._container_start_wait_s
        while not self._compose_running(name):
            if time.monotonic() >= deadline:
                return {
                    "service": name,
                    "mode": mode,
                    "ok": False,
                    "detail": (
                        f"docker compose start: not running within "
                        f"{self._container_start_wait_s:g} s"
                    ),
                }
            time.sleep(0.5)
        time.sleep(self._container_stable_s)
        if not self._compose_running(name):
            return {
                "service": name,
                "mode": mode,
                "ok": False,
                "detail": (
                    f"docker compose start: not still running {self._container_stable_s:g} s later"
                ),
            }
        return {"service": name, "mode": mode, "ok": True, "detail": "docker compose start"}

    async def kill(self, name: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._kill, name)

    async def start(self, name: str, mode: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._start, name, mode)


# --- the environment a scenario runs in --------------------------------------


@dataclass
class ScenarioEnv:
    """Everything a scenario touches, injectable for tests. `playback`,
    `faults` and `nodes` are the control process's own objects, so a
    scenario's faults show in `GET /sim/links` while it runs."""

    cluster: Cluster
    playback: Any  # restaurantos.sim.playback.Playback
    faults: Any  # restaurantos.sim.faults.FaultLayer
    nodes: Any  # restaurantos.sim.nodes.Nodes
    services: Any  # Services
    probe: Any  # ClusterProbe
    # (run_id, allow_unacked) -> the checker's result; blocking
    check: Callable[[str, bool], dict[str, Any]]
    # the POST /menu/products body -> its answer (T6)
    publish_menu: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    # why no scenario may run at all (the checker module missing), or None
    ready: Callable[[], str | None] = lambda: None
    runs_dir: Path = Path("runs")
    default_from_day: int = 0
    last_day: int | None = None
    default_speed: float = 1000.0
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    monotonic: Callable[[], float] = time.monotonic
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    tick_s: float = 0.25
    # how long after the run a drain or a menu publish is still waited for
    wait_after_s: float = 300.0


def default_env(
    cluster: Cluster,
    cluster_file: Path,
    *,
    playback: Any,
    faults: Any,
    nodes: Any,
    api_base: str,
    default_from_day: int,
    last_day: int | None,
    default_speed: float,
    http_timeout_s: float = 10.0,
    runs_dir: Path = Path("runs"),
) -> ScenarioEnv:
    """The control process's scenario environment: its own engine, fault
    layer and nodes, the real databases, the checker and the gateway at
    `api_base` (the engine's target, for T6's menu publish)."""

    def check(run_id: str, allow_unacked: bool) -> dict[str, Any]:
        from restaurantos.sim.checker import run_check
        from restaurantos.sim.config import get_sim_settings
        from restaurantos.sim.replaydata import CHANNEL

        sim = get_sim_settings()
        return run_check(
            cluster,
            [run_id],
            runs_dir=runs_dir,
            channel=CHANNEL,
            allow_unacked=allow_unacked,
            drain_timeout_s=getattr(sim, "check_drain_timeout_s", 300.0),
            poll_s=getattr(sim, "check_poll_s", 1.0),
        )

    async def publish_menu(body: dict[str, Any]) -> dict[str, Any]:
        import httpx

        url = f"{api_base.rstrip('/')}/menu/products"
        async with httpx.AsyncClient(timeout=http_timeout_s) as client:
            response = await client.post(url, json=body)
        if response.status_code != 202:
            raise ScenarioError(f"POST {url} answered {response.status_code}: {response.text}")
        return response.json()

    import importlib.util

    def ready() -> str | None:
        if importlib.util.find_spec("restaurantos.sim.checker") is None:
            return "the checker is not on this branch (sim/checker.py, PR #33)"
        return None

    return ScenarioEnv(
        cluster=cluster,
        playback=playback,
        faults=faults,
        nodes=nodes,
        services=Services(cluster, cluster_file, runs_dir=runs_dir),
        probe=ClusterProbe(cluster),
        check=check,
        publish_menu=publish_menu,
        ready=ready,
        runs_dir=runs_dir,
        default_from_day=default_from_day,
        last_day=last_day,
        default_speed=default_speed,
    )


async def refuse_reason(env: ScenarioEnv) -> tuple[int, str, str] | None:
    """Why no scenario may start now, as `(status, code, message)`, or None.

    `ready` (the checker module missing on this branch) answers `501
    not_built`, like the snapshot and check routes; everything else is a
    refusal. The link state is read live last: a cut a restart left
    behind belongs to no live fault, so a scenario would neither know
    about it nor heal it."""
    why = env.ready()
    if why is not None:
        return (501, "not_built", why)
    state = env.playback.state
    if state in LIVE_STATES:
        return (
            409,
            "scenario_refused",
            f"a playback run is {state}; stop it first (rosctl playback stop)",
        )
    if env.faults.faults():
        ids = ", ".join(fault.fault_id for fault in env.faults.faults())
        return (
            409,
            "scenario_refused",
            f"faults are live ({ids}); heal them first (rosctl fault heal-all)",
        )
    try:
        links = await env.faults.links()
    except ToxiproxyError as error:
        return (
            getattr(error, "status", 502),
            "toxiproxy_error",
            f"Toxiproxy cannot be read: {error}",
        )
    stray: dict[str, dict[str, list[Any]]] = {}
    for entry in links:
        kinds = {
            kind: things for kind, things in (entry.get("unexplained") or {}).items() if things
        }
        if kinds:
            stray[str(entry.get("group"))] = kinds
    if stray:
        named = "; ".join(
            f"{group}: "
            + ", ".join(f"{kind} {_plain(things)}" for kind, things in sorted(kinds.items()))
            for group, kinds in stray.items()
        )
        return (
            409,
            "scenario_refused",
            f"Toxiproxy holds link state no live fault explains ({named}); "
            "clear it with rosctl fault heal-all",
        )
    return None


def _plain(things: Sequence[Any]) -> str:
    """Name the stray entries of one kind: a toxic by its name, a proxy
    by its own string."""
    return ", ".join(
        str(thing.get("name") or thing.get("type")) if isinstance(thing, dict) else str(thing)
        for thing in things
    )


def next_free_day(runs_dir: Path, default: int) -> int:
    """The first day after every earlier run's window (meta.json only,
    never a run log), or `default` when no run is newer."""
    last = default - 1
    if runs_dir.is_dir():
        for meta_file in runs_dir.glob("*/meta.json"):
            with contextlib.suppress(OSError, ValueError, TypeError):
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                if not isinstance(meta, dict):
                    continue  # not a run's meta.json (a list, a number, ...)
                to_day = meta.get("to_day")
                if isinstance(to_day, int):
                    last = max(last, to_day)
    return last + 1


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds(), 3)


def _start_hint(mode: str, name: str) -> str:
    """The command a person runs to start one service by hand."""
    if mode == "host":
        return f"uv run rosctl up {name}"
    return f"docker compose -p restaurantos -f {COMPOSE_FILE} --profile services start {name}"


# --- one scenario run ----------------------------------------------------------


class Session:
    """One scenario run: the shared script and what it recorded."""

    def __init__(
        self,
        env: ScenarioEnv,
        spec: Spec,
        params: Params,
        progress: Callable[[str], None],
    ) -> None:
        self.env = env
        self.spec = spec
        self.params = params
        self.progress = progress
        self.run_id: str | None = None
        self.window: dict[str, Any] = {}
        self.timeline: dict[str, datetime | None] = {}
        self.measurements: dict[str, Any] = {}
        self.conditions: list[dict[str, Any]] = []
        self.warnings: list[str] = []
        self.next_steps: list[str] = []
        self.state: dict[str, Any] = {}  # what a script keeps between its phases
        self.check: dict[str, Any] | None = None
        self.playback: dict[str, Any] | None = None
        # what the script broke and has not repaired yet: cleaned up on error
        self.live_faults: list[str] = []
        self.killed_nodes: list[str] = []
        self.killed_services: dict[str, str] = {}  # name -> mode
        self.watchers: list[asyncio.Task] = []

    # --- helpers ---------------------------------------------------------

    async def db(self, fn: Callable[..., Any], *args: Any) -> Any:
        return await asyncio.to_thread(fn, *args)

    def condition(self, name: str, passed: bool, detail: str = "") -> None:
        self.conditions.append({"name": name, "passed": bool(passed), "detail": detail})

    def mark(self, key: str) -> datetime:
        at = self.env.now()
        self.timeline[key] = at
        return at

    async def hold(
        self,
        seconds: float,
        label: str,
        tick: Callable[[], Awaitable[None]] | None = None,
        *,
        under_load: bool = True,
    ) -> None:
        """Wait `seconds` wall seconds, calling `tick` on every step. With
        `under_load` the run must stay live: a fault held after the run
        ended would not be under load."""
        env = self.env
        end = env.monotonic() + seconds
        while True:
            left = end - env.monotonic()
            self.progress(f"{label}: {max(left, 0):.0f} s left")
            if tick is not None:
                # a probe read must not hold the fault past its deadline:
                # the read may still finish in its thread, the hold does not wait
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(tick(), timeout=max(min(left, 2.0), 0.1))
            if under_load and env.playback.state not in LIVE_STATES:
                raise ScenarioError(
                    f"the run ended ({env.playback.state}) during '{label}'; "
                    "its window is too short - pick a lower speed"
                )
            if left <= 0:
                return
            await env.sleep(min(env.tick_s, left))

    def watch(self, coro: Awaitable[None]) -> None:
        """A measurement that runs beside the rest of the script."""
        self.watchers.append(asyncio.ensure_future(coro))

    async def add_fault(self, target: str, kind: str, params: dict[str, Any] | None = None) -> None:
        fault = await self.env.faults.create(target, kind, params or {})
        self.live_faults.append(fault.fault_id)
        self.state.setdefault("faults", []).append(fault.as_dict())

    async def heal_faults(self) -> None:
        while self.live_faults:
            await self.env.faults.heal(self.live_faults.pop())

    async def kill_node(self, node: str) -> None:
        answer = await self.env.nodes.kill(node)
        self.killed_nodes.append(node)
        if answer.get("running"):
            raise ScenarioError(f"{node} is still running after the kill")

    async def start_nodes(self) -> list[str]:
        """Start the killed nodes again; returns the ones that did not
        come back (T1b fails its own verdict on that list)."""
        failed: list[str] = []
        while self.killed_nodes:
            node = self.killed_nodes.pop()
            answer = await self.env.nodes.start(node)
            if not answer.get("running"):
                self.warnings.append(f"{node} did not start again")
                failed.append(node)
        return failed

    # --- the shared script ------------------------------------------------

    async def start_run(self) -> None:
        env, params, spec = self.env, self.params, self.spec
        speed = params.speed or env.default_speed
        hold_s = params.fault_s + params.settle_s
        if spec.name == "T3":
            hold_s += params.kill_after_s + params.down_s
        needed = seconds_of_day(params.inject_at) + hold_s * speed + TAIL_MARGIN_S
        if not math.isfinite(needed):
            raise ScenarioParamsError(
                f"the script's window is not a finite number of days at {speed:g}x "
                "(the speed or the hold is too large)"
            )
        days = max(1, math.ceil(needed / SECONDS_PER_DAY))
        if params.from_day is not None:
            from_day = params.from_day
        else:
            from_day = await asyncio.to_thread(next_free_day, env.runs_dir, env.default_from_day)
            if env.last_day is not None and from_day + days - 1 > env.last_day:
                self.warnings.append(
                    f"every day up to {env.last_day} has been replayed; replaying from day "
                    f"{env.default_from_day} again (stored orders are answered as duplicates)"
                )
                from_day = env.default_from_day
        to_day = from_day + days - 1
        if env.last_day is not None and to_day > env.last_day:
            raise ScenarioParamsError(
                f"the script needs days {from_day} to {to_day} at {speed:g}x, but the data ends "
                f"on day {env.last_day}; pick an earlier from_day or a lower speed"
            )
        stamp = env.now().strftime("%Y%m%d-%H%M%S")
        run_id = f"{spec.name.lower()}-{params.region_id}-{stamp}"
        self.window = {"from_day": from_day, "to_day": to_day, "speed": speed}
        extra: dict[str, Any] = {}
        if spec.name == "T4":
            extra = {"duplicate_rate": params.duplicate_rate, "shuffle": params.shuffle}
        self.progress(f"starting run {run_id} (days {from_day}-{to_day}, {speed:g}x)")
        await env.playback.start(
            seed=params.seed,
            from_day=from_day,
            to_day=to_day,
            speed=speed,
            run_id=run_id,
            meta_extra={"scenario": spec.name, "scenario_region": params.region_id},
            **extra,
        )
        # set once the run exists: a start the engine refused leaves no
        # result file in a run directory it never made
        self.run_id = run_id
        self.mark("run_started_at")

    async def wait_for_injection(self) -> None:
        env = self.env
        target = self.window["from_day"] * SECONDS_PER_DAY + seconds_of_day(self.params.inject_at)
        while True:
            clock = env.playback.clock()
            if clock.get("state") not in LIVE_STATES:
                raise ScenarioError(f"the run ended ({clock.get('state')}) before the fault")
            day, moment = clock.get("sim_day"), clock.get("sim_time")
            if day is not None and moment:
                now = int(day) * SECONDS_PER_DAY + seconds_of_day(str(moment))
                if now >= target:
                    return
                self.progress(
                    f"run {self.run_id}: waiting for day {self.window['from_day']} "
                    f"{self.params.inject_at} (now day {day} {moment})"
                )
            await env.sleep(env.tick_s)

    async def finish_run(self) -> None:
        self.progress(f"run {self.run_id}: waiting for the run to finish its window")
        await self.env.playback.wait_done()
        self.mark("run_ended_at")
        self.playback = self.env.playback.status()
        state = self.playback.get("state")
        if state != "done":
            raise ScenarioError(f"the run ended {state}, not done; see runs/{self.run_id}/")

    async def finish_watchers(self) -> None:
        if not self.watchers:
            return
        self.progress("waiting for the measurements after the heal")
        done, pending = await asyncio.wait(self.watchers, timeout=self.env.wait_after_s)
        for task in pending:
            task.cancel()
        for task in done:
            if task.exception() is not None:
                self.warnings.append(f"a measurement failed: {task.exception()}")
        self.watchers = []

    async def run_check(self) -> None:
        self.progress("checking: waiting for the queues to drain, then comparing")
        try:
            self.check = await asyncio.to_thread(self.env.check, self.run_id, not self.spec.strict)
        except Exception as error:  # noqa: BLE001 - a failed check is a verdict, not a crash
            self.check = {"passed": False, "error": f"{type(error).__name__}: {error}"}
        self.mark("checked_at")
        check = self.check
        detail = check.get("error") or (
            f"acked {check.get('acked')}, stored {check.get('stored')}, hq {check.get('hq')}, "
            f"lost at failover {(check.get('lost_at_failover') or {}).get('count', 0)}"
        )
        self.condition("checker passed", bool(self.check.get("passed")), detail)

    async def cleanup(self) -> None:
        """Repair what the script broke, after an error."""
        for task in self.watchers:
            task.cancel()
        try:
            await self.heal_faults()
        except Exception as error:  # noqa: BLE001 - a last resort exists for this one
            # the preflight refused every other live fault, so everything
            # Toxiproxy still holds here is this scenario's own
            self.warnings.append(f"could not heal the scenario's faults: {error}")
            try:
                await self.env.faults.heal_all()
            except Exception as heal_error:  # noqa: BLE001
                self.warnings.append(f"heal-all also failed: {heal_error}")
            else:
                self.warnings.append("rosctl fault heal-all ran as a last resort")
        steps = (
            ("start the killed nodes", self.start_nodes),
            ("start the killed services", self._restart_services),
            ("stop the run", self._stop_run),
        )
        for what, step in steps:
            try:
                await step()
            except Exception as error:  # noqa: BLE001 - report every repair that failed
                self.warnings.append(f"could not {what}: {error}")

    async def _restart_services(self) -> None:
        for name, mode in list(self.killed_services.items()):
            try:
                answer = await self.env.services.start(name, mode)
            except Exception as error:  # noqa: BLE001 - the other services still get their turn
                self.warnings.append(
                    f"could not start {name} again: {error}; "
                    f"start it by hand: {_start_hint(mode, name)}"
                )
                continue
            if answer.get("ok"):
                self.killed_services.pop(name, None)
            else:
                self.warnings.append(
                    f"{name} did not start again: {answer.get('detail')}; "
                    f"start it by hand: {_start_hint(mode, name)}"
                )

    async def _stop_run(self) -> None:
        # only the scenario's own run: someone else's run is not ours to stop
        status = self.env.playback.status()
        if status.get("run_id") == self.run_id and status.get("state") in ("running", "paused"):
            await self.env.playback.stop()

    def result(self, error: str | None = None) -> dict[str, Any]:
        passed = error is None and all(item["passed"] for item in self.conditions)
        return {
            "scenario": self.spec.name,
            "summary": self.spec.summary,
            "passed": passed,
            "error": error,
            "region_id": self.params.region_id,
            "params": self.params.shown(self.spec),
            "run_id": self.run_id,
            "window": self.window,
            "timeline": {key: _iso(value) for key, value in self.timeline.items()},
            "measurements": self.measurements,
            "conditions": self.conditions,
            "faults": self.state.get("faults", []),
            "check": self.check,
            "playback": self.playback,
            "warnings": self.warnings,
            "next_steps": self.next_steps,
        }

    def write(self, result: dict[str, Any]) -> None:
        if self.run_id is None:
            return
        run_dir = self.env.runs_dir / self.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / RESULT_FILE).write_text(
            json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
        )


# --- the scripts ----------------------------------------------------------------


class Script:
    """One scenario's own part. `prepare` runs before the run starts (a
    refusal there costs nothing), `recheck` at the injection moment (a
    failover during the warm-up would break the wrong node, so the
    scripts that break a primary refuse there), `act` from the injection
    moment to the heal, `conclude` after the run and before the check."""

    async def prepare(self, s: Session) -> None:
        pass

    async def recheck(self, s: Session) -> None:
        pass

    async def _same_primary(self, s: Session, key: str) -> None:
        """Refuse when the region's primary moved between `prepare` and
        the injection moment: the script would break the wrong node, and
        an earlier failover event would then count as this run's."""
        view = await s.db(s.env.probe.region, s.params.region_id)
        if view["primary"] != s.state[key] or view["epoch"] != s.state["epoch"]:
            raise ScenarioError(
                f"{s.params.region_id} failed over before the fault "
                f"({s.state[key]} epoch {s.state['epoch']} -> "
                f"{view['primary']} epoch {view['epoch']}); run the scenario again"
            )

    async def act(self, s: Session) -> None:
        await s.hold(s.params.fault_s, "load")

    async def conclude(self, s: Session) -> None:
        pass

    async def no_failover(self, s: Session) -> None:
        rid = s.params.region_id
        failovers = await s.db(s.env.probe.failovers_since, s.state["event_mark"], rid)
        s.measurements["failovers"] = len(failovers)
        s.condition(
            f"no failover in {rid}",
            not failovers,
            ", ".join(f"event {f['event_id']} at {f['occurred_at']}" for f in failovers),
        )


class PrimaryLoss(Script):
    """T1 (cut) and T1b (crash)."""

    def __init__(self, crash: bool) -> None:
        self.crash = crash

    async def prepare(self, s: Session) -> None:
        await super().prepare(s)
        rid = s.params.region_id
        if not s.env.cluster.standbys:
            raise ScenarioError("this cluster has no standbys: there is nothing to fail over to")
        view = await s.db(s.env.probe.region, rid)
        old = view["primary"]
        if not await s.db(s.env.probe.streaming, old):
            raise ScenarioError(
                f"{old}, {rid}'s primary, has no streaming standby, so nothing can take over. "
                f"After a T1 or T1b, rejoin the old primary first: {_rejoin(rid)}"
            )
        s.state.update(old=old, epoch=view["epoch"])
        s.measurements.update(old_primary=old, epoch_before=view["epoch"])

    async def recheck(self, s: Session) -> None:
        await self._same_primary(s, "old")

    async def act(self, s: Session) -> None:
        old, rid = s.state["old"], s.params.region_id
        injected = s.mark("injected_at")
        if self.crash:
            await s.kill_node(old)
        else:
            await s.add_fault(f"node:{old}", "cut")

        async def tick() -> None:
            if "promoted_seen_at" in s.state:
                return
            with contextlib.suppress(Exception):  # HQ may answer slowly mid-failover
                view = await s.db(s.env.probe.region, rid)
                if view["primary"] != old:
                    s.state["promoted_seen_at"] = s.env.now()

        await s.hold(s.params.fault_s, f"{old} {'killed' if self.crash else 'cut off'}", tick)
        failed: list[str] = []
        if self.crash:
            failed = await s.start_nodes()
        else:
            await s.heal_faults()
        s.mark("healed_at")
        if self.crash:
            s.condition(
                f"{old} started again",
                old not in failed,
                "started again" if old not in failed else f"{old} did not start again",
            )
        s.measurements["promotion_seen_s"] = _seconds(injected, s.state.get("promoted_seen_at"))

    async def conclude(self, s: Session) -> None:
        old, rid = s.state["old"], s.params.region_id
        injected = s.timeline["injected_at"]
        view = await s.db(s.env.probe.region, rid)
        new = view["primary"]
        failovers = await s.db(s.env.probe.failovers_since, s.state["event_mark"], rid)
        s.measurements.update(new_primary=new, epoch_after=view["epoch"], failovers=len(failovers))
        if failovers:
            s.measurements["failover_s"] = _seconds(injected, failovers[0]["occurred_at"])
        s.condition(f"{rid} failed over", new != old, f"{old} -> {new}")
        first = None
        if new != old:
            days = (s.window["from_day"], s.window["to_day"])
            first = await s.db(s.env.probe.first_commit_after, new, injected, days)
        s.measurements["writes_resumed_s"] = _seconds(injected, first)
        s.condition(
            f"orders commit on {new} again",
            first is not None,
            f"first commit {first.isoformat()}" if first is not None else "none after the fault",
        )
        if new != old:
            s.next_steps.append(
                f"{old} is fenced and no longer a standby; rejoin it before another T1 or "
                f"T1b on {rid}: {_rejoin(rid)}"
            )


class Partition(Script):
    """T2, and T3 (T2 plus killed workers during the drain)."""

    def __init__(self, kill_workers: bool) -> None:
        self.kill_workers = kill_workers

    async def act(self, s: Session) -> None:
        rid = s.params.region_id
        s.mark("injected_at")
        await s.add_fault(f"link:{rid}<->hq", "cut")
        s.measurements["held_peak"] = 0

        async def tick() -> None:
            with contextlib.suppress(Exception):
                held = await s.db(s.env.probe.holding_left, rid)
                s.measurements["held_peak"] = max(s.measurements["held_peak"], held)

        await s.hold(s.params.fault_s, f"{rid} cut off from HQ", tick)
        await s.heal_faults()
        healed = s.mark("healed_at")
        s.watch(self._drain(s, healed))
        if self.kill_workers:
            await self._kill_workers(s)

    async def _drain(self, s: Session, healed: datetime) -> None:
        rid = s.params.region_id
        while True:
            with contextlib.suppress(Exception):
                if await s.db(s.env.probe.holding_left, rid) == 0:
                    s.measurements["drain_s"] = _seconds(healed, s.env.now())
                    return
            await s.env.sleep(1.0)

    async def _kill_workers(self, s: Session) -> None:
        names = (f"relay-{s.params.region_id}", "hq-worker")
        await s.env.sleep(s.params.kill_after_s)
        # was there work to interrupt? A T3 that kills an idle pair proves
        # nothing about the drain; both numbers go into the result
        holding: int | None = None
        outbox: int | None = None
        with contextlib.suppress(Exception):
            holding = await s.db(s.env.probe.holding_left, s.params.region_id)
        with contextlib.suppress(Exception):
            view = await s.db(s.env.probe.region, s.params.region_id)
            outbox = await s.db(s.env.probe.outbox_left, view["primary"])
        s.measurements["pending_at_kill"] = {"holding_queue": holding, "outbox": outbox}
        kills = []
        for name in names:
            answer = await s.env.services.kill(name)
            kills.append(answer)
            s.killed_services[name] = answer["mode"]
        s.mark("workers_killed_at")
        await s.hold(s.params.down_s, "relay and HQ worker down", under_load=False)
        starts = []
        for name in names:
            answer = await s.env.services.start(name, s.killed_services[name])
            starts.append(answer)
            if answer.get("ok"):
                s.killed_services.pop(name, None)
            else:
                s.warnings.append(
                    f"{name} did not start again: {answer.get('detail')}; "
                    f"start it by hand: {_start_hint(s.killed_services[name], name)}"
                )
        s.mark("workers_started_at")
        s.measurements["workers"] = {"killed": kills, "started": starts}
        pending = s.measurements["pending_at_kill"]
        s.condition(
            "work was pending when the workers were killed",
            (pending["holding_queue"] or 0) + (pending["outbox"] or 0) > 0,
            f"holding_queue {pending['holding_queue']}, outbox {pending['outbox']}",
        )
        s.condition(
            "relay and HQ worker killed and started again",
            all(item["ok"] for item in kills + starts),
            "; ".join(f"{item['service']}: {item['detail']}" for item in kills + starts),
        )

    async def conclude(self, s: Session) -> None:
        await self.no_failover(s)
        drained = s.measurements.get("drain_s")
        s.condition(
            "holding queue drained after the heal",
            drained is not None,
            f"{drained} s" if drained is not None else "not drained",
        )


class Duplicates(Script):
    """T4: the run itself resends and shuffles; the checker is strict."""

    async def act(self, s: Session) -> None:
        s.mark("injected_at")
        await s.hold(s.params.fault_s, "duplicates and shuffle under load")
        s.mark("healed_at")

    async def conclude(self, s: Session) -> None:
        status = s.playback or {}
        s.measurements["orders_resent"] = status.get("orders_resent")
        s.condition(
            "orders were resent",
            s.params.duplicate_rate == 0 or bool(status.get("orders_resent")),
            f"{status.get('orders_resent')} resent",
        )


class SlowNode(Script):
    """T5: latency on the current primary; no failover."""

    async def prepare(self, s: Session) -> None:
        await super().prepare(s)
        view = await s.db(s.env.probe.region, s.params.region_id)
        s.state.update(primary=view["primary"], epoch=view["epoch"])

    async def recheck(self, s: Session) -> None:
        await self._same_primary(s, "primary")

    async def act(self, s: Session) -> None:
        primary = s.state["primary"]
        s.mark("injected_at")
        await s.add_fault(
            f"node:{primary}",
            "latency",
            {"latency_ms": s.params.latency_ms, "jitter_ms": s.params.jitter_ms},
        )
        await s.hold(s.params.fault_s, f"{primary} slowed by {s.params.latency_ms} ms")
        await s.heal_faults()
        s.mark("healed_at")

    async def conclude(self, s: Session) -> None:
        await self.no_failover(s)
        view = await s.db(s.env.probe.region, s.params.region_id)
        s.condition(
            "the primary did not change",
            view["primary"] == s.state["primary"],
            f"{s.state['primary']} -> {view['primary']}",
        )


class MenuPublish(Script):
    """T6: a menu version published while the region's links to HQ reset
    every connection after `reset_timeout_ms`. The apply is one
    transaction, so every sample must show the old version or the whole
    new one: no product newer than the region's version, and the probe
    product present exactly when the version reached the new number."""

    async def prepare(self, s: Session) -> None:
        await super().prepare(s)
        view = await s.db(s.env.probe.region, s.params.region_id)
        s.state.update(primary=view["primary"], epoch=view["epoch"])
        s.state["source"] = await s.db(s.env.probe.probe_source, view["primary"])
        s.measurements.update(samples=0, mixed=[])

    async def recheck(self, s: Session) -> None:
        await self._same_primary(s, "primary")

    async def _sample(self, s: Session) -> dict[str, Any] | None:
        product = s.state.get("product")
        if product is None:
            return None
        try:
            state = await s.db(
                s.env.probe.menu_state,
                s.state["primary"],
                product["vendor_id"],
                product["product_id"],
            )
        except Exception:  # noqa: BLE001 - a sample that cannot be read is not a mix
            return None
        version = s.state["version"]
        s.measurements["samples"] += 1
        has_new = state["version"] >= version
        mixed = state["ahead"] > 0 or has_new != (state["probe"] is not None)
        if mixed and len(s.measurements["mixed"]) < 10:
            s.measurements["mixed"].append({"at": _iso(s.env.now()), **state})
        return state

    async def act(self, s: Session) -> None:
        rid, source = s.params.region_id, s.state["source"]
        s.mark("injected_at")
        await s.add_fault(f"link:{rid}<->hq", "reset", {"timeout_ms": s.params.reset_timeout_ms})
        name = f"{s.run_id} probe"
        answer = await s.env.publish_menu(
            {
                "note": f"scenario {s.run_id}",
                "products": [
                    {
                        "vendor_id": source["vendor_id"],
                        "name": name,
                        "price_units": source["price_units"],
                        "dish_id": source["dish_id"],
                        "multiplier": source["multiplier"],
                    }
                ],
            }
        )
        s.mark("published_at")
        product = answer["products"][0]
        s.state.update(version=answer["menu_version"], product=product)
        s.measurements.update(menu_version=answer["menu_version"], probe_product=product)

        async def tick() -> None:
            await self._sample(s)

        await s.hold(s.params.fault_s, f"{rid}'s links reset during the publish", tick)
        await s.heal_faults()
        healed = s.mark("healed_at")
        s.watch(self._arrival(s, healed))

    async def _arrival(self, s: Session, healed: datetime) -> None:
        while True:
            state = await self._sample(s)
            if state is not None and state["version"] >= s.state["version"] and state["probe"]:
                s.measurements["final_version"] = state["version"]
                s.measurements["arrived_s"] = _seconds(healed, s.env.now())
                return
            await s.env.sleep(s.env.tick_s)

    async def conclude(self, s: Session) -> None:
        mixed = s.measurements["mixed"]
        s.condition(
            "the region never showed half a version",
            not mixed,
            f"{s.measurements['samples']} samples, {len(mixed)} mixed",
        )
        arrived = s.measurements.get("arrived_s")
        s.condition(
            f"the region reached menu version {s.state['version']}",
            arrived is not None,
            f"{arrived} s after the heal" if arrived is not None else "not reached",
        )


SCRIPTS: dict[str, Callable[[], Script]] = {
    "T1": lambda: PrimaryLoss(crash=False),
    "T1b": lambda: PrimaryLoss(crash=True),
    "T2": lambda: Partition(kill_workers=False),
    "T3": lambda: Partition(kill_workers=True),
    "T4": Duplicates,
    "T5": SlowNode,
    "T6": MenuPublish,
}


def _rejoin(region_id: str) -> str:
    return REJOIN.format(rid=region_id, compose=COMPOSE_FILE)


async def run_scenario(
    name: str,
    body: Mapping[str, Any] | None,
    env: ScenarioEnv,
    *,
    progress: Callable[[str], None] = lambda text: None,
) -> dict[str, Any]:
    """Run one scenario to its verdict. Refusals (bad params, a live run,
    live faults, no standby to promote) raise before anything changes;
    an error later repairs what the script broke, writes the partial
    result and raises."""
    spec, params = parse_params(name, body, env.cluster)
    reason = await refuse_reason(env)
    if reason is not None:
        status, code, message = reason
        if code == "not_built":
            raise ScenarioNotBuiltError(message)
        raise ScenarioError(message)
    script = SCRIPTS[spec.name]()
    s = Session(env, spec, params, progress)
    await script.prepare(s)
    try:
        await s.start_run()
        await s.wait_for_injection()
        # the mark and the recheck sit at the injection moment: a failover
        # during the warm-up would break the wrong node, and an event from
        # before it would then count as this run's
        await script.recheck(s)
        s.state["event_mark"] = await s.db(s.env.probe.last_event_id)
        await script.act(s)
        await s.hold(params.settle_s, "settling after the heal", under_load=False)
        await s.finish_run()
        await s.finish_watchers()
        await script.conclude(s)
        await s.run_check()
    except BaseException as error:
        await s.cleanup()
        await asyncio.to_thread(s.write, s.result(error=f"{type(error).__name__}: {error}"))
        raise
    result = s.result()
    await asyncio.to_thread(s.write, result)
    progress(f"{spec.name} {'passed' if result['passed'] else 'FAILED'}")
    return result


def format_result(result: dict[str, Any]) -> str:
    """The `rosctl scenario` summary."""
    verdict = "PASSED" if result.get("passed") else "FAILED"
    window = result.get("window") or {}
    lines = [
        f"{result.get('scenario')} {verdict} (run {result.get('run_id')}, region "
        f"{result.get('region_id')}, days {window.get('from_day')}-{window.get('to_day')})"
    ]
    if result.get("error"):
        lines.append(f"error: {result['error']}")
    for item in result.get("conditions") or []:
        mark = "ok" if item["passed"] else "FAIL"
        detail = f" ({item['detail']})" if item.get("detail") else ""
        lines.append(f"  [{mark}] {item['name']}{detail}")
    measurements = {
        key: value
        for key, value in (result.get("measurements") or {}).items()
        if not isinstance(value, dict | list)
    }
    if measurements:
        lines.append(
            "measurements: "
            + ", ".join(f"{key} {value}" for key, value in sorted(measurements.items()))
        )
    for warning in result.get("warnings") or []:
        lines.append(f"warning: {warning}")
    for step in result.get("next_steps") or []:
        lines.append(f"next: {step}")
    if result.get("run_id"):
        lines.append(f"result: runs/{result['run_id']}/{RESULT_FILE}")
    return "\n".join(lines)

"""The fault scenarios (T1, T1b, T2 to T6) against fakes: a playback
engine whose simulated clock follows a fake wall clock, a fault layer,
nodes, services and database reads that answer what each test sets.
Nothing sleeps for real and nothing touches Docker or PostgreSQL."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.sim.scenarios import (
    RESULT_FILE,
    ScenarioEnv,
    ScenarioError,
    ScenarioParamsError,
    UnknownScenarioError,
    describe,
    format_result,
    next_free_day,
    parse_params,
    refuse_reason,
    run_scenario,
)

CLUSTER = Cluster(regions=2)
DAY = 86_400
EPOCH = datetime(2026, 11, 24, 20, 0, tzinfo=UTC)


class Clock:
    """Fake wall time: `sleep` moves it, nothing waits."""

    def __init__(self) -> None:
        self.t = 0.0

    def monotonic(self) -> float:
        return self.t

    def now(self) -> datetime:
        return EPOCH + timedelta(seconds=self.t)

    async def sleep(self, seconds: float) -> None:
        self.t += max(seconds, 0.001)
        await asyncio.sleep(0)


class FakePlayback:
    """Simulated time = the window's start + wall time x speed; the run is
    done once it passes its last day's last order (23:00)."""

    def __init__(self, clock: Clock, runs_dir: Path) -> None:
        self.clock_ = clock
        self.runs_dir = runs_dir
        self.started: dict[str, Any] | None = None
        self._state = "idle"
        self.stopped = False

    def _sim(self) -> float:
        assert self.started is not None
        return (
            self.started["from_day"] * DAY
            + (self.clock_.t - self.started["t0"]) * self.started["speed"]
        )

    @property
    def state(self) -> str:
        if self._state == "running" and self._sim() >= self.started["to_day"] * DAY + 23 * 3600:
            self._state = "done"
        return self._state

    async def start(self, **kwargs: Any) -> str:
        self.started = {**kwargs, "t0": self.clock_.t}
        self._state = "running"
        # like the engine: the run's window goes to its meta.json
        run_dir = self.runs_dir / kwargs["run_id"]
        run_dir.mkdir(parents=True)
        window = {"from_day": kwargs["from_day"], "to_day": kwargs["to_day"]}
        (run_dir / "meta.json").write_text(json.dumps(window), encoding="utf-8")
        return kwargs["run_id"]

    def clock(self) -> dict[str, Any]:
        state = self.state
        if self.started is None:
            return {"state": state, "sim_day": None, "sim_time": None, "speed": None}
        sim = int(self._sim())
        day, rest = divmod(sim, DAY)
        moment = f"{rest // 3600:02d}:{rest % 3600 // 60:02d}:{rest % 60:02d}"
        return {"state": state, "sim_day": day, "sim_time": moment, "speed": 1.0}

    async def wait_done(self) -> None:
        while self.state == "running":
            await self.clock_.sleep(1.0)

    async def stop(self) -> None:
        self.stopped = True
        self._state = "stopped"

    def status(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "run_id": (self.started or {}).get("run_id"),
            "orders_resent": 7,
        }


@dataclass
class FakeFault:
    fault_id: str
    target: str
    fault: str
    params: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "fault_id": self.fault_id,
            "target": self.target,
            "fault": self.fault,
            "params": self.params,
        }


class FakeFaults:
    def __init__(self) -> None:
        self.live: dict[str, FakeFault] = {}
        self.log: list[tuple[str, ...]] = []
        self.links_answer: list[dict[str, Any]] = []
        self.heal_all_calls = 0

    def faults(self) -> list[FakeFault]:
        return list(self.live.values())

    async def create(self, target: str, fault: str, params: dict[str, Any]) -> FakeFault:
        record = FakeFault(f"f{len(self.log) + 1}", target, fault, dict(params))
        self.live[record.fault_id] = record
        self.log.append(("create", target, fault))
        return record

    async def heal(self, fault_id: str) -> None:
        record = self.live.pop(fault_id)
        self.log.append(("heal", record.target, record.fault))

    async def links(self) -> list[dict[str, Any]]:
        return self.links_answer

    async def heal_all(self) -> int:
        self.heal_all_calls += 1
        healed = len(self.live)
        self.live.clear()
        return healed


class FakeNodes:
    def __init__(self) -> None:
        self.running = {node: True for node in CLUSTER.nodes}
        self.fail_start = False
        self.log: list[tuple[str, str]] = []

    async def kill(self, node: str) -> dict[str, Any]:
        self.running[node] = False
        self.log.append(("kill", node))
        return {"node_id": node, "running": False}

    async def start(self, node: str) -> dict[str, Any]:
        running = not self.fail_start
        self.running[node] = running
        self.log.append(("start", node))
        return {"node_id": node, "running": running}


class FakeServices:
    def __init__(self) -> None:
        self.log: list[tuple[str, str]] = []
        self.fail_start: set[str] = set()

    async def kill(self, name: str) -> dict[str, Any]:
        self.log.append(("kill", name))
        return {"service": name, "mode": "host", "ok": True, "detail": "killed"}

    async def start(self, name: str, mode: str) -> dict[str, Any]:
        self.log.append(("start", name))
        ok = name not in self.fail_start
        return {
            "service": name,
            "mode": mode,
            "ok": ok,
            "detail": "started" if ok else "it crashed on start",
        }


class FakeProbe:
    """Answers what the test sets; `primary_after` switches the region's
    primary once the fault has been on for a while (a failover)."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.primary = "r1-primary"
        self.promote_at: float | None = None  # wall time of a failover
        self.streaming_count = 1
        self.failovers: list[dict[str, Any]] = []
        self.holding = 0
        self.outbox = 0
        self.menu: dict[str, Any] = {"version": 1, "ahead": 0, "probe": None}
        self.menu_after_heal: dict[str, Any] | None = None
        self.healed = False

    def region(self, region_id: str) -> dict[str, Any]:
        if self.promote_at is not None and self.clock.t >= self.promote_at:
            return {"primary": "r1-standby", "standby": "r1-primary", "epoch": 2}
        return {"primary": self.primary, "standby": "r1-standby", "epoch": 1}

    def streaming(self, node: str) -> int:
        return self.streaming_count

    def last_event_id(self) -> int:
        return 40

    def failovers_since(self, after_id: int, region_id: str) -> list[dict[str, Any]]:
        return [event for event in self.failovers if event["event_id"] > after_id]

    def first_commit_after(
        self, node: str, after: datetime, days: tuple[int, int]
    ) -> datetime | None:
        return after + timedelta(seconds=9) if node == "r1-standby" else None

    def holding_left(self, region_id: str) -> int:
        return self.holding

    def outbox_left(self, node: str) -> int:
        return self.outbox

    def probe_source(self, node: str) -> dict[str, Any]:
        return {"vendor_id": "v1", "dish_id": "d1", "multiplier": 2, "price_units": 30}

    def menu_state(self, node: str, vendor_id: str, product_id: str) -> dict[str, Any]:
        if self.healed and self.menu_after_heal is not None:
            return self.menu_after_heal
        return self.menu


class World:
    def __init__(self, tmp_path: Path) -> None:
        self.clock = Clock()
        self.playback = FakePlayback(self.clock, tmp_path / "runs")
        self.faults = FakeFaults()
        self.nodes = FakeNodes()
        self.services = FakeServices()
        self.probe = FakeProbe(self.clock)
        self.checks: list[tuple[str, bool]] = []
        self.check_result: dict[str, Any] = {
            "passed": True,
            "acked": 10,
            "stored": 10,
            "hq": 10,
            "lost_at_failover": {"count": 0},
        }
        self.published: list[dict[str, Any]] = []
        self.env = ScenarioEnv(
            cluster=CLUSTER,
            playback=self.playback,
            faults=self.faults,
            nodes=self.nodes,
            services=self.services,
            probe=self.probe,
            check=self._check,
            publish_menu=self._publish,
            runs_dir=tmp_path / "runs",
            default_from_day=14,
            last_day=90,
            default_speed=1000.0,
            sleep=self.clock.sleep,
            monotonic=self.clock.monotonic,
            now=self.clock.now,
            wait_after_s=5.0,
        )
        # heal the menu in the probe when the fault layer heals
        original_heal = self.faults.heal

        async def heal(fault_id: str) -> None:
            await original_heal(fault_id)
            self.probe.healed = True

        self.faults.heal = heal  # type: ignore[method-assign]

    def _check(self, run_id: str, allow_unacked: bool) -> dict[str, Any]:
        self.checks.append((run_id, allow_unacked))
        return self.check_result

    async def _publish(self, body: dict[str, Any]) -> dict[str, Any]:
        self.published.append(body)
        return {
            "menu_version": 5,
            "products": [
                {"vendor_id": "v1", "product_id": "p9", "region_id": "r1", "created": True}
            ],
        }

    def result_file(self, result: dict[str, Any]) -> dict[str, Any]:
        path = self.env.runs_dir / result["run_id"] / RESULT_FILE
        return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path)


# --- parameters ------------------------------------------------------------------


def test_params_defaults_and_names_ignore_case() -> None:
    spec, params = parse_params("t1b", None, CLUSTER)
    assert spec.name == "T1b"
    assert params.region_id == "r1" and params.fault_s == 30 and params.inject_at == "11:00:00"
    _, params = parse_params("T2", {"fault_s": 300, "region_id": "r2"}, CLUSTER)
    assert params.fault_s == 300 and params.region_id == "r2"


@pytest.mark.parametrize(
    ("name", "body", "match"),
    [
        ("T2", {"latency_ms": 5}, "does not take latency_ms"),
        ("T4", {"duplicate_rate": 1.5}, r"\[0, 1\]"),
        ("T2", {"region_id": "r9"}, "no region 'r9'"),
        ("T2", {"inject_at": "25:00:00"}, "time of day"),
        ("T2", {"inject_at": "noon"}, "HH:MM:SS"),
        ("T2", {"fault_s": 0}, "above 0"),
        ("T5", {"latency_ms": -1}, "whole number"),
        ("T4", {"shuffle": "yes"}, "true or false"),
        ("T2", {"settle_s": True}, "must be a number"),
        ("T2", {"fault_s": 10**400}, "too large"),
    ],
)
def test_bad_params_are_refused(name: str, body: dict, match: str) -> None:
    with pytest.raises(ScenarioParamsError, match=match):
        parse_params(name, body, CLUSTER)


def test_unknown_scenario() -> None:
    with pytest.raises(UnknownScenarioError) as caught:
        parse_params("T7", None, CLUSTER)
    assert caught.value.status == 404


def test_describe_lists_every_scenario_with_its_own_params() -> None:
    listed = {entry["name"]: entry["params"] for entry in describe()}
    assert list(listed) == ["T1", "T1b", "T2", "T3", "T4", "T5", "T6"]
    assert listed["T5"]["latency_ms"] == 100 and "latency_ms" not in listed["T2"]
    assert listed["T2"]["fault_s"] == 60


def test_next_free_day_reads_meta_files_only(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    assert next_free_day(runs, 14) == 14
    for run_id, to_day in (("a", 20), ("b", 31), ("c", None)):
        (runs / run_id).mkdir(parents=True)
        (runs / run_id / "meta.json").write_text(json.dumps({"to_day": to_day}))
    (runs / "services").mkdir()  # rosctl up's pid files: no meta.json
    assert next_free_day(runs, 14) == 32


# --- refusals ----------------------------------------------------------------------


async def test_refused_while_a_run_is_live_or_faults_are_on(world: World) -> None:
    world.playback._state = "running"
    world.playback.started = {"from_day": 14, "to_day": 99, "speed": 1, "t0": 0}
    reason = await refuse_reason(world.env)
    assert reason is not None and "stop it first" in reason[2]
    with pytest.raises(ScenarioError, match="stop it first"):
        await run_scenario("T2", None, world.env)
    world.playback._state = "done"
    await world.faults.create("repl:r1", "cut", {})
    reason = await refuse_reason(world.env)
    assert reason is not None and "heal them first" in reason[2]


async def test_refused_over_link_state_no_live_fault_explains(world: World) -> None:
    world.faults.links_answer = [
        {
            "group": "link:r1<->hq",
            "unexplained": {"disabled": ["gateway__r1-primary"], "toxics": [], "missing": []},
        }
    ]
    reason = await refuse_reason(world.env)
    assert reason is not None
    status, code, message = reason
    assert (status, code) == (409, "scenario_refused")
    assert "gateway__r1-primary" in message and "heal-all" in message


async def test_refused_while_the_checker_is_missing(world: World) -> None:
    world.env.ready = lambda: "the checker is not on this branch (sim/checker.py, PR #33)"
    reason = await refuse_reason(world.env)
    assert reason is not None and reason[:2] == (501, "not_built")


async def test_t1_refused_without_a_streaming_standby(world: World) -> None:
    world.probe.streaming_count = 0
    with pytest.raises(ScenarioError, match="rejoin"):
        await run_scenario("T1", None, world.env)
    assert world.playback.started is None and world.faults.log == []


async def test_a_window_past_the_data_is_refused(world: World) -> None:
    with pytest.raises(ScenarioParamsError, match="data ends on day 90"):
        await run_scenario("T2", {"from_day": 90, "fault_s": 900}, world.env)
    assert world.playback.started is None


async def test_an_impossible_window_is_refused_before_any_run(world: World) -> None:
    with pytest.raises(ScenarioParamsError, match="not a finite number of days"):
        await run_scenario("T2", {"speed": 1e308}, world.env)
    assert world.playback.started is None


# --- the scripts ---------------------------------------------------------------------


async def test_t2_cuts_heals_measures_and_checks(world: World) -> None:
    world.probe.holding = 3

    async def drain_later() -> None:
        while not world.probe.healed:
            await asyncio.sleep(0)
        world.clock.t += 4
        world.probe.holding = 0

    drainer = asyncio.create_task(drain_later())
    result = await run_scenario("T2", {"fault_s": 60}, world.env)
    await drainer
    assert world.faults.log == [("create", "link:r1<->hq", "cut"), ("heal", "link:r1<->hq", "cut")]
    started = world.playback.started
    assert started["from_day"] == 14 and started["speed"] == 1000.0
    # 11:00 + 90 s of script at 1000x + the tail margin: two days
    assert started["to_day"] == 15
    assert started["meta_extra"] == {"scenario": "T2", "scenario_region": "r1"}
    assert world.checks == [(result["run_id"], True)]
    assert result["passed"] is True
    assert result["measurements"]["held_peak"] == 3
    assert result["measurements"]["drain_s"] is not None
    names = [item["name"] for item in result["conditions"]]
    assert names == ["no failover in r1", "holding queue drained after the heal", "checker passed"]
    assert world.result_file(result)["passed"] is True
    injected = datetime.fromisoformat(result["timeline"]["injected_at"])
    started_at = datetime.fromisoformat(result["timeline"]["run_started_at"])
    # the fault starts at 11:00 simulated: 39.6 s of wall time at 1000x
    assert 39 <= (injected - started_at).total_seconds() <= 40


async def test_t2_fails_on_a_failover(world: World) -> None:
    world.probe.failovers = [{"event_id": 41, "occurred_at": EPOCH, "node_id": "r1-standby"}]
    result = await run_scenario("T2", None, world.env)
    assert result["passed"] is False
    assert result["conditions"][0] == {
        "name": "no failover in r1",
        "passed": False,
        "detail": f"event 41 at {EPOCH}",
    }


async def test_the_next_scenario_starts_after_the_last_window(world: World) -> None:
    first = await run_scenario("T2", None, world.env)
    world.playback._state = "done"
    second = await run_scenario("T5", None, world.env)
    assert second["window"]["from_day"] == first["window"]["to_day"] + 1


async def test_t1_measures_the_failover(world: World) -> None:
    world.probe.promote_at = 45.0  # wall seconds: a few after the cut at ~39.6

    def failovers(after_id: int, region_id: str) -> list[dict[str, Any]]:
        return [
            {"event_id": 41, "occurred_at": EPOCH + timedelta(seconds=44), "node_id": "r1-standby"}
        ]

    world.probe.failovers_since = failovers  # type: ignore[method-assign]
    result = await run_scenario("T1", None, world.env)
    assert world.faults.log == [
        ("create", "node:r1-primary", "cut"),
        ("heal", "node:r1-primary", "cut"),
    ]
    m = result["measurements"]
    assert m["old_primary"] == "r1-primary" and m["new_primary"] == "r1-standby"
    assert m["epoch_before"] == 1 and m["epoch_after"] == 2
    assert 4 <= m["failover_s"] <= 5
    assert m["writes_resumed_s"] == 9.0
    assert 5 <= m["promotion_seen_s"] <= 6
    assert result["passed"] is True
    assert "restaurantos.coordinator.rejoin r1" in result["next_steps"][0]


async def test_t1_fails_when_nobody_takes_over(world: World) -> None:
    result = await run_scenario("T1", None, world.env)
    assert result["passed"] is False
    failed = [item["name"] for item in result["conditions"] if not item["passed"]]
    assert failed == ["r1 failed over", "orders commit on r1-primary again"]
    assert result["next_steps"] == []


async def test_t1b_kills_the_primary_and_starts_it_at_the_heal(world: World) -> None:
    world.probe.promote_at = 45.0
    await run_scenario("T1b", None, world.env)
    assert world.nodes.log == [("kill", "r1-primary"), ("start", "r1-primary")]
    assert world.faults.log == []


async def test_t1b_fails_when_the_old_primary_does_not_come_back(world: World) -> None:
    world.nodes.fail_start = True
    result = await run_scenario("T1b", None, world.env)
    assert result["passed"] is False
    failed = {item["name"] for item in result["conditions"] if not item["passed"]}
    assert "r1-primary started again" in failed


async def test_a_failover_during_the_warm_up_is_refused_at_injection(world: World) -> None:
    # the primary moves at wall-second 10, long before the 11:00 injection
    world.probe.promote_at = 10.0
    with pytest.raises(ScenarioError, match="failed over before the fault"):
        await run_scenario("T1", None, world.env)
    # nothing was broken, and the run was stopped
    assert world.faults.log == [] and world.nodes.log == []
    assert world.playback.stopped is True


async def test_t3_kills_and_restarts_the_relay_and_the_hq_worker(world: World) -> None:
    world.probe.holding = 3  # work is pending at the kill, so the drain is real

    # the holding queue empties once the workers are back
    original_start = world.services.start

    async def start_then_drain(name: str, mode: str) -> dict[str, Any]:
        answer = await original_start(name, mode)
        world.probe.holding = 0
        return answer

    world.services.start = start_then_drain  # type: ignore[method-assign]
    result = await run_scenario("T3", {"region_id": "r2", "fault_s": 10}, world.env)
    assert world.faults.log[0] == ("create", "link:r2<->hq", "cut")
    assert world.services.log == [
        ("kill", "relay-r2"),
        ("kill", "hq-worker"),
        ("start", "relay-r2"),
        ("start", "hq-worker"),
    ]
    assert result["passed"] is True
    assert "relay and HQ worker killed and started again" in [
        item["name"] for item in result["conditions"]
    ]
    assert result["timeline"]["workers_killed_at"] < result["timeline"]["workers_started_at"]


async def test_t3_with_nothing_pending_fails_saying_why(world: World) -> None:
    result = await run_scenario("T3", {"region_id": "r2", "fault_s": 10}, world.env)
    assert result["passed"] is False
    assert result["measurements"]["pending_at_kill"] == {"holding_queue": 0, "outbox": 0}
    failed = {item["name"] for item in result["conditions"] if not item["passed"]}
    assert "work was pending when the workers were killed" in failed


async def test_a_failed_service_restart_warns_and_keeps_the_condition(world: World) -> None:
    world.probe.holding = 3
    world.services.fail_start = {"relay-r2"}

    # the drain completes only when a worker actually comes back
    original_start = world.services.start

    async def start_then_drain(name: str, mode: str) -> dict[str, Any]:
        answer = await original_start(name, mode)
        if answer.get("ok"):
            world.probe.holding = 0
        return answer

    world.services.start = start_then_drain  # type: ignore[method-assign]
    result = await run_scenario("T3", {"region_id": "r2", "fault_s": 10}, world.env)
    # the other service still got its start
    assert ("start", "hq-worker") in world.services.log
    assert result["passed"] is False
    failed = {item["name"] for item in result["conditions"] if not item["passed"]}
    assert "relay and HQ worker killed and started again" in failed
    assert any("uv run rosctl up relay-r2" in item for item in result["warnings"])


async def test_t4_resends_and_shuffles_and_checks_strictly(world: World) -> None:
    result = await run_scenario("T4", {"duplicate_rate": 0.1}, world.env)
    started = world.playback.started
    assert started["duplicate_rate"] == 0.1 and started["shuffle"] is True
    assert world.checks == [(result["run_id"], False)]  # strict: no unacked orders allowed
    assert world.faults.log == []
    assert result["measurements"]["orders_resent"] == 7
    assert result["passed"] is True


async def test_t5_slows_the_current_primary(world: World) -> None:
    world.probe.primary = "r1-standby"  # after an earlier failover
    result = await run_scenario("T5", {"latency_ms": 400, "jitter_ms": 50}, world.env)
    assert world.faults.log[0] == ("create", "node:r1-standby", "latency")
    assert result["faults"][0]["params"] == {"latency_ms": 400, "jitter_ms": 50}
    assert result["passed"] is True


async def test_t6_publishes_and_samples_the_region(world: World) -> None:
    world.probe.menu_after_heal = {"version": 5, "ahead": 0, "probe": 5}
    result = await run_scenario("T6", None, world.env)
    assert world.faults.log[0] == ("create", "link:r1<->hq", "reset")
    assert result["faults"][0]["params"] == {"timeout_ms": 200}
    product = world.published[0]["products"][0]
    assert product == {
        "vendor_id": "v1",
        "name": f"{result['run_id']} probe",
        "price_units": 30,
        "dish_id": "d1",
        "multiplier": 2,
    }
    m = result["measurements"]
    assert m["menu_version"] == 5 and m["final_version"] == 5 and m["samples"] > 0
    assert m["mixed"] == []
    assert result["passed"] is True


async def test_t6_fails_on_half_a_version(world: World) -> None:
    # the probe product is there while the region still claims version 1
    world.probe.menu = {"version": 1, "ahead": 1, "probe": 5}
    world.probe.menu_after_heal = {"version": 5, "ahead": 0, "probe": 5}
    result = await run_scenario("T6", None, world.env)
    assert result["passed"] is False
    assert result["measurements"]["mixed"][0]["ahead"] == 1


async def test_a_failed_check_is_a_verdict(world: World) -> None:
    def broken(run_id: str, allow_unacked: bool) -> dict[str, Any]:
        raise RuntimeError("the queues did not drain in 300 s")

    world.env.check = broken
    result = await run_scenario("T5", None, world.env)
    assert result["passed"] is False
    assert result["check"]["error"] == "RuntimeError: the queues did not drain in 300 s"


async def test_an_error_repairs_what_the_script_broke(world: World) -> None:
    async def boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("the gateway is down")

    world.env.publish_menu = boom
    with pytest.raises(RuntimeError, match="the gateway is down"):
        await run_scenario("T6", None, world.env)
    # the reset fault was healed and the run stopped
    assert world.faults.live == {}
    assert world.playback.stopped is True
    runs = list((world.env.runs_dir).glob(f"*/{RESULT_FILE}"))
    written = json.loads(runs[0].read_text())
    assert written["passed"] is False and "the gateway is down" in written["error"]


async def test_cleanup_never_stops_someone_elses_run(world: World) -> None:
    def foreign_run_is_live(after_id: int, region_id: str) -> list[dict[str, Any]]:
        # between this scenario's finish and its cleanup, run B went live
        world.playback.started = {
            "run_id": "run-b",
            "from_day": 20,
            "to_day": 21,
            "speed": 1,
            "t0": world.clock.t,
        }
        world.playback._state = "running"
        raise RuntimeError("HQ is gone")

    world.probe.failovers_since = foreign_run_is_live  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="HQ is gone"):
        await run_scenario("T2", {"fault_s": 5}, world.env)
    assert world.playback.stopped is False  # run B was left alone


async def test_a_failed_heal_runs_heal_all_once(world: World) -> None:
    async def boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("the gateway is down")

    async def unhealable(fault_id: str) -> None:
        raise RuntimeError("toxiproxy is gone")

    world.env.publish_menu = boom
    world.faults.heal = unhealable  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="the gateway is down"):
        await run_scenario("T6", None, world.env)
    assert world.faults.heal_all_calls == 1
    written = json.loads(next(world.env.runs_dir.glob(f"*/{RESULT_FILE}")).read_text())
    assert any("heal-all ran as a last resort" in item for item in written["warnings"])


async def test_progress_is_reported(world: World) -> None:
    seen: list[str] = []
    await run_scenario("T5", {"fault_s": 5}, world.env, progress=seen.append)
    assert seen[0].startswith("starting run t5-r1-")
    assert any("slowed by 100 ms" in line for line in seen)
    assert seen[-1] == "T5 passed"


def test_format_result() -> None:
    text = format_result(
        {
            "scenario": "T2",
            "passed": False,
            "run_id": "t2-r1-x",
            "region_id": "r1",
            "window": {"from_day": 14, "to_day": 15},
            "conditions": [
                {"name": "no failover in r1", "passed": True, "detail": ""},
                {"name": "checker passed", "passed": False, "detail": "acked 3"},
            ],
            "measurements": {"drain_s": 4.0, "workers": {"killed": []}},
            "warnings": ["w"],
            "next_steps": ["n"],
        }
    )
    assert text.splitlines() == [
        "T2 FAILED (run t2-r1-x, region r1, days 14-15)",
        "  [ok] no failover in r1",
        "  [FAIL] checker passed (acked 3)",
        "measurements: drain_s 4.0",
        "warning: w",
        "next: n",
        "result: runs/t2-r1-x/scenario.json",
    ]


# --- T3's service kills, container mode ------------------------------------------------


async def test_services_in_container_mode_go_through_compose(tmp_path: Path) -> None:
    import subprocess

    from restaurantos.sim.scenarios import Services

    calls: list[tuple[str, ...]] = []
    running = {"relay-r1"}

    def compose(*args: str) -> subprocess.CompletedProcess:
        calls.append(args)
        command, name = args[2], args[-1]
        stdout = ""
        if command == "ps" and name in running:
            stdout = json.dumps({"Service": name, "State": "running"}) + "\n"
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")

    services = Services(
        CLUSTER,
        tmp_path / "cluster.toml",
        runs_dir=tmp_path,
        compose=compose,
        container_start_wait_s=0,
        container_stable_s=0,
    )
    killed = await services.kill("relay-r1")
    assert killed["mode"] == "container" and killed["ok"] is True
    assert ("--profile", "services", "kill", "relay-r1") in calls
    started = await services.start("relay-r1", killed["mode"])
    assert started["ok"] is True
    assert ("--profile", "services", "start", "relay-r1") in calls
    # a malformed pid file plus a running container: container mode wins
    (tmp_path / "services").mkdir(exist_ok=True)
    (tmp_path / "services" / "relay-r1.pid").write_text("garbage\n", encoding="utf-8")
    killed = await services.kill("relay-r1")
    assert killed["mode"] == "container"
    # neither a pid file nor a running container: refused, nothing killed
    with pytest.raises(ScenarioError, match="hq-worker is not running"):
        await services.kill("hq-worker")
    assert not any(call[2] == "kill" and call[-1] == "hq-worker" for call in calls)


async def test_an_unjudgeable_pid_file_names_itself(tmp_path: Path) -> None:
    import subprocess

    from restaurantos.sim.scenarios import Services

    def compose(*args: str) -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    services = Services(
        CLUSTER,
        tmp_path / "cluster.toml",
        runs_dir=tmp_path,
        compose=compose,
        container_start_wait_s=0,
        container_stable_s=0,
    )
    # a pid record for a process that no longer exists cannot be judged:
    # the refusal names the file so a person can look at it
    (tmp_path / "services").mkdir()
    (tmp_path / "services" / "relay-r1.pid").write_text("99999999 12345\n", encoding="utf-8")
    with pytest.raises(ScenarioError, match="relay-r1.pid"):
        await services.kill("relay-r1")


async def test_a_container_that_dies_right_after_start_reports_it(tmp_path: Path) -> None:
    import subprocess

    from restaurantos.sim.scenarios import Services

    running = {"relay-r1"}

    def compose(*args: str) -> subprocess.CompletedProcess:
        command, name = args[2], args[-1]
        stdout = ""
        if command == "ps" and name in running:
            stdout = json.dumps({"Service": name, "State": "running"}) + "\n"
            running.discard(name)  # seen running once, then gone
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")

    services = Services(
        CLUSTER,
        tmp_path / "cluster.toml",
        runs_dir=tmp_path,
        compose=compose,
        container_start_wait_s=0,
        container_stable_s=0,
    )
    started = await services.start("relay-r1", "container")
    assert started["ok"] is False
    assert "not still running" in started["detail"]

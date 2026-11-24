"""Snapshot save/restore with fake Docker, a fake playback host and a fake
Toxiproxy: which services stop, cleanup on failure, the 00:30 wait, the
cluster-file rule, the run log, and the option-C resume (replay the day,
skip a restock that already went out, catch up, switch speed back)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import httpx
import pytest

from restaurantos.common.cluster import BASELINE, Cluster, Proxy, proxies, proxy_name
from restaurantos.sim.checker import RESUMED_FROM
from restaurantos.sim.config import SimSettings
from restaurantos.sim.snapshot import (
    Context,
    SnapshotError,
    SnapshotExistsError,
    UnknownSnapshotError,
    list_snapshots,
    next_run_id,
    restore,
    save,
    sim_seconds,
    volume_name,
)
from restaurantos.sim.toxiproxy import Toxiproxy

CLUSTER = Cluster(regions=1)
TOML = "[cluster]\nregions = 1\nstandbys = true\n"
SHARED = proxy_name("gateway", "r1-primary")


class FakeToxiproxy:
    """A stateful Toxiproxy server over a MockTransport, with the listen and
    upstream addresses `Toxiproxy.sync` compares."""

    def __init__(self, proxies: Iterable[str | Proxy]) -> None:
        self.proxies: dict[str, dict] = {}
        for proxy in proxies:
            if isinstance(proxy, Proxy):
                self.proxies[proxy.name] = {**proxy.as_toxiproxy(), "toxics": []}
            else:
                self.proxies[proxy] = {"name": proxy, "enabled": True, "toxics": []}
        self.calls: list[tuple[str, str]] = []
        # (method, path) pairs that answer 500, to test rollbacks
        self.fail: set[tuple[str, str]] = set()

    def client(self) -> Toxiproxy:
        return Toxiproxy("http://toxiproxy.test", transport=httpx.MockTransport(self.handler))

    def toxic_names(self, proxy: str) -> list[str]:
        return sorted(toxic["name"] for toxic in self.proxies[proxy]["toxics"])

    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        self.calls.append((method, path))
        if (method, path) in self.fail:
            return httpx.Response(500, text="injected failure")
        body = json.loads(request.content) if request.content else None
        if method == "GET" and path == "/proxies":
            return httpx.Response(200, json=self.proxies)
        if method == "POST" and path == "/proxies":
            if body["name"] in self.proxies:
                return httpx.Response(409, text="proxy already exists")
            self.proxies[body["name"]] = {**body, "toxics": []}
            return httpx.Response(201, json=self.proxies[body["name"]])
        if method == "POST" and path == "/reset":
            for proxy in self.proxies.values():
                proxy["enabled"] = True
                proxy["toxics"] = []
            return httpx.Response(204)
        parts = path.strip("/").split("/")
        if parts[0] != "proxies" or len(parts) < 2 or parts[1] not in self.proxies:
            return httpx.Response(404, text="proxy not found")
        proxy = self.proxies[parts[1]]
        if len(parts) == 2 and method == "DELETE":
            del self.proxies[parts[1]]
            return httpx.Response(204)
        if len(parts) == 2 and method == "POST":
            proxy["enabled"] = body["enabled"]
            return httpx.Response(200, json=proxy)
        if len(parts) == 3 and parts[2] == "toxics":
            if method == "GET":
                return httpx.Response(200, json=proxy["toxics"])
            if method == "POST":
                name = body.get("name") or f"{body['type']}_{body['stream']}"
                if any(toxic["name"] == name for toxic in proxy["toxics"]):
                    return httpx.Response(409, text="toxic already exists")
                toxic = {**body, "name": name}
                proxy["toxics"].append(toxic)
                return httpx.Response(200, json=toxic)
        if len(parts) == 4 and parts[2] == "toxics" and method == "DELETE":
            before = len(proxy["toxics"])
            proxy["toxics"] = [toxic for toxic in proxy["toxics"] if toxic["name"] != parts[3]]
            if len(proxy["toxics"]) == before:
                return httpx.Response(404, text="toxic not found")
            return httpx.Response(204)
        return httpx.Response(404, text="unknown route")


class FakeDocker:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.fail_archive: str | None = None

    def stop(self, services: list[str]) -> None:
        self.calls.append(("stop", tuple(services)))

    def start(self, services: list[str]) -> None:
        self.calls.append(("start", tuple(services)))

    def create_missing(self, services: list[str]) -> None:
        self.calls.append(("create_missing", tuple(services)))

    def postgres_image(self) -> str:
        return "postgres:18-test"

    def archive(self, volume: str, out_dir: Path, image: str) -> None:
        if volume == self.fail_archive:
            raise SnapshotError("disk full")
        (out_dir / f"{volume}.tar.gz").write_text("archive", encoding="utf-8")
        self.calls.append(("archive", volume))

    def unpack(self, volume: str, in_dir: Path, image: str) -> None:
        assert (in_dir / f"{volume}.tar.gz").is_file()
        self.calls.append(("unpack", volume))


class FakeHost:
    """A playback host (rosctl control) reduced to what snapshots use."""

    base_url = "http://control.test"

    def __init__(self, runs_dir: Path, *, state: str = "running", day: int = 23, at: int = 3600):
        self.runs_dir = runs_dir
        self.alive = True
        self.state = state
        self.day = day
        self.offset = at
        self.speed_value = 1000.0
        self.inflight = 0
        self.run_id = "run-a"
        self.commands: list[dict] = []
        self.speeds: list[float] = []
        self.healed = False
        self.engine_meta: dict[str, Any] = {"target_url": "http://127.0.0.1:8000/api/v1"}
        # how far dispatch trails the clock, in simulated seconds (the
        # clock runs on wall time; an event waits for its order's answer)
        self.lag = 0
        # once set, the frontier reported after the pause (to cross midnight)
        self.frontier_after_pause: int | None = None
        self.dispatched = True
        # GET /sim/playback's engine block (what every run on it shares)
        self.engine: dict[str, Any] = {
            "sequential": False,
            "region": None,
            "target_url": "http://127.0.0.1:8000/api/v1",
            "inflight": 64,
            "events": {"enabled": True, "sample_rate": 1.0, "jitter_s": 60.0},
            "restock_every": 7,
            "duplicate_offset_s": 1.0,
            "shuffle_window": 8,
            "kitchen": {"mode": "edge", "urls": {"r1": "http://127.0.0.1:8011/api/v1"}},
        }

    def advance(self, wall_s: float) -> None:
        if self.state == "running":
            self.offset += wall_s * self.speed_value

    def frontier(self) -> int:
        """Absolute simulated seconds of the last dispatched item."""
        if self.state == "paused" and self.frontier_after_pause is not None:
            return self.frontier_after_pause
        return self.day * 86400 + int(self.offset) - self.lag

    async def status(self) -> dict | None:
        if not self.alive:
            return None
        day, rest = divmod(self.frontier(), 86400)
        answer = {
            "state": self.state,
            "run_id": self.run_id,
            "seed": 7,
            "speed": self.speed_value,
            "from_day": 21,
            "to_day": 28,
            "inflight": self.inflight,
            "dispatched_sim_day": day if self.dispatched else None,
            "dispatched_sim_time": (
                f"{rest // 3600:02d}:{rest % 3600 // 60:02d}:{rest % 60:02d}"
                if self.dispatched
                else None
            ),
            "engine": self.engine,
        }
        if self.state == "paused" and self.inflight:
            self.inflight -= 1
        return answer

    async def clock(self) -> dict:
        day, rest = self.day + int(self.offset) // 86400, int(self.offset) % 86400
        moment = f"{rest // 3600:02d}:{rest % 3600 // 60:02d}:{rest % 60:02d}"
        return {"state": self.state, "sim_day": day, "sim_time": moment, "speed": self.speed_value}

    async def command(self, body: dict) -> httpx.Response:
        self.commands.append(body)
        action = body["action"]
        if action == "pause":
            self.state = "paused"
        elif action == "resume":
            self.state = "running"
        elif action == "stop":
            self.state = "stopped"
        elif action == "start":
            self.run_id = body["run_id"]
            run_dir = self.runs_dir / self.run_id
            run_dir.mkdir(parents=True)
            (run_dir / "meta.json").write_text(json.dumps(self.engine_meta), encoding="utf-8")
            self.state, self.day, self.offset = "running", body["from_day"], 0
            self.speed_value = body["speed"]
        return httpx.Response(200, json={"state": self.state})

    async def speed(self, value: float) -> httpx.Response:
        self.speeds.append(value)
        self.speed_value = value
        return httpx.Response(200, json={})

    async def heal_all(self) -> bool:
        self.healed = True
        return True


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(tmp_path)
    runs = tmp_path / "runs"
    run_dir = runs / "run-a"
    run_dir.mkdir(parents=True)
    (run_dir / "meta.json").write_text(
        json.dumps(
            {
                "run_id": "run-a",
                "from_day": 21,
                "to_day": 28,
                "duplicate_rate": 0.05,
                "shuffle": False,
                "target_url": "http://127.0.0.1:8000/api/v1",
            }
        ),
        encoding="utf-8",
    )
    lines = [
        {"kind": "restock", "restock_day": 21, "vendor_id": "v1"},
        {"kind": "order", "external_order_id": "1", "http_status": 200, "order_id": "u1"},
    ]
    (run_dir / "orders.jsonl").write_text(
        "".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8"
    )
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text(TOML, encoding="utf-8")
    server = FakeToxiproxy(proxies(CLUSTER))
    host = FakeHost(runs)
    docker = FakeDocker()
    services: list[str] = []

    async def sleep(wall_s: float) -> None:
        host.advance(wall_s)

    ctx = Context(
        cluster_file=cluster_file,
        sim=SimSettings(_env_file=None, snapshot_dir=tmp_path / "snapshots"),
        playback=host,  # type: ignore[arg-type]
        toxiproxy_factory=server.client,
        docker=docker,  # type: ignore[arg-type]
        runs_dir=runs,
        generated_dir=tmp_path / "generated",
        sleep=sleep,
        services_down=lambda: services.append("down"),
        services_up=lambda cluster, path: services.append("up"),
    )
    return ctx, host, docker, server, services


DATABASES = (*CLUSTER.nodes, BASELINE)


def test_helpers() -> None:
    assert sim_seconds({"sim_day": 2, "sim_time": "00:30:05"}) == 2 * 86400 + 1805
    assert sim_seconds({"sim_day": None, "sim_time": None}) is None
    assert volume_name("r1-primary") == "restaurantos_r1-primary-data"


def test_next_run_id_skips_taken_ids(tmp_path: Path) -> None:
    (tmp_path / "a-r1").mkdir()
    assert next_run_id(tmp_path, "a") == "a-r2"


async def test_save_pauses_archives_every_database_and_resumes(world) -> None:
    ctx, host, docker, server, _ = world
    host.inflight = 2
    server.proxies[SHARED]["enabled"] = False
    server.proxies[SHARED]["toxics"].append(
        {"name": "f1-latency-up", "type": "latency", "stream": "upstream", "attributes": {}}
    )
    record = await save("before-t2", ctx)
    snap = ctx.snapshot_dir / "before-t2"
    assert snap.is_dir() and not (ctx.snapshot_dir / ".before-t2.partial").exists()
    assert [body["action"] for body in host.commands] == ["pause", "resume"]
    # only the PostgreSQL services stop: Toxiproxy keeps its faults
    assert docker.calls[0] == ("stop", DATABASES)
    assert docker.calls[-1] == ("start", DATABASES)
    assert {call[1] for call in docker.calls if call[0] == "archive"} == {
        volume_name(node) for node in DATABASES
    }
    assert record["run"]["run_id"] == "run-a" and record["run"]["duplicate_rate"] == 0.05
    assert (record["sim_day"], record["sim_time"]) == (23, "01:00:00")
    assert record["regions"] == ["r1"]
    saved = json.loads((snap / "toxiproxy.json").read_text())
    entry = next(proxy for proxy in saved if proxy["name"] == SHARED)
    assert entry["enabled"] is False and entry["toxics"][0]["name"] == "f1-latency-up"
    assert (snap / "run" / "run-a" / "orders.jsonl").is_file()
    assert (snap / "cluster.toml").read_text() == TOML
    assert list_snapshots(ctx.snapshot_dir) == [
        {
            "name": "before-t2",
            "created_at": record["created_at"],
            "sim_day": 23,
            "sim_time": "01:00:00",
            "regions": ["r1"],
        }
    ]


async def test_save_waits_until_half_past_midnight(world) -> None:
    ctx, host, _, _, _ = world
    host.offset = 600  # 00:10
    record = await save("early", ctx)
    assert host.commands[0] == {"action": "pause"}
    assert sim_seconds(record) % 86400 >= 1800


async def test_the_snapshot_point_is_the_dispatch_frontier_not_the_clock(world) -> None:
    """The clock is at day 23 01:00, dispatch is still at day 22 23:00:
    the snapshot is a day-22 snapshot - the run log and the databases end
    there, and restore must replay day 22, not day 23."""
    ctx, host, _, _, _ = world
    host.lag = 2 * 3600
    record = await save("lagging", ctx)
    assert (record["sim_day"], record["sim_time"]) == (22, "23:00:00")
    assert (record["clock_sim_day"], record["clock_sim_time"]) == (23, "01:00:00")
    assert list_snapshots(ctx.snapshot_dir)[0]["sim_day"] == 22


async def test_save_waits_for_dispatch_not_the_clock_to_pass_the_minimum(world) -> None:
    ctx, host, _, _, _ = world
    host.offset, host.lag = 3600, 3000  # clock 01:00, dispatch 00:10
    record = await save("trailing", ctx)
    assert record["sim_day"] == 23 and sim_seconds(record) % 86400 >= 1800
    clock = sim_seconds({"sim_day": record["clock_sim_day"], "sim_time": record["clock_sim_time"]})
    assert clock == sim_seconds(record) + 3000  # the clock stays ahead; the record is dispatch


async def test_dispatch_crossing_midnight_at_the_pause_refuses_and_resumes(world) -> None:
    ctx, host, docker, _, _ = world
    host.frontier_after_pause = 24 * 86400 + 5  # day 24 00:00:05
    with pytest.raises(SnapshotError, match="try again"):
        await save("crossed", ctx)
    assert [body["action"] for body in host.commands] == ["pause", "resume"]
    assert docker.calls == []
    assert not (ctx.snapshot_dir / "crossed").exists()


async def test_save_refuses_a_minimum_below_the_engines_event_floor(world) -> None:
    ctx, host, docker, _, _ = world
    ctx.sim = SimSettings(
        _env_file=None, snapshot_dir=ctx.snapshot_dir, snapshot_min_day_offset_s=600
    )
    with pytest.raises(SnapshotError, match="at least 1560"):
        await save("low", ctx)
    assert host.commands == [] and docker.calls == []
    # the host's own jitter decides, not this process's settings
    ctx.sim = SimSettings(
        _env_file=None, snapshot_dir=ctx.snapshot_dir, snapshot_min_day_offset_s=1600
    )
    host.engine["events"]["jitter_s"] = 120.0
    with pytest.raises(SnapshotError, match="at least 1620"):
        await save("low", ctx)
    # with events off only the midnight restock is left: any point past it
    host.engine["events"]["enabled"] = False
    ctx.sim = SimSettings(
        _env_file=None, snapshot_dir=ctx.snapshot_dir, snapshot_min_day_offset_s=600
    )
    host.offset = 700
    record = await save("events-off", ctx)
    assert sim_seconds(record) % 86400 >= 600


async def test_save_refuses_a_paused_run_that_dispatched_nothing(world) -> None:
    ctx, host, docker, _, _ = world
    host.state, host.dispatched = "paused", False
    with pytest.raises(SnapshotError, match="nothing dispatched yet"):
        await save("empty", ctx)
    assert docker.calls == []


async def test_save_refuses_a_run_paused_before_half_past_midnight(world) -> None:
    ctx, host, docker, _, _ = world
    host.state, host.offset = "paused", 600
    with pytest.raises(SnapshotError, match="resume the run past that time"):
        await save("early", ctx)
    assert docker.calls == []
    assert not (ctx.snapshot_dir / "early").exists()


async def test_save_refuses_a_taken_name(world) -> None:
    ctx, *_ = world
    (ctx.snapshot_dir / "taken").mkdir(parents=True)
    with pytest.raises(SnapshotExistsError):
        await save("taken", ctx)


async def test_a_failed_snapshot_cleans_up_and_restarts_everything(world) -> None:
    ctx, host, docker, _, _ = world
    docker.fail_archive = volume_name("r1-standby")
    with pytest.raises(SnapshotError, match="disk full"):
        await save("broken", ctx)
    assert not (ctx.snapshot_dir / "broken").exists()
    assert not (ctx.snapshot_dir / ".broken.partial").exists()
    assert docker.calls[-1] == ("start", DATABASES)
    assert host.state == "running"


async def test_save_without_a_run_records_no_run(world) -> None:
    ctx, host, _, _, _ = world
    host.alive = False
    record = await save("idle", ctx)
    assert record["run"] is None and record["sim_day"] is None


async def test_restore_refuses_a_different_cluster_file_without_the_flag(world) -> None:
    ctx, host, docker, _, _ = world
    await save("snap", ctx)
    ctx.cluster_file.write_text("[cluster]\nregions = 2\n", encoding="utf-8")
    docker.calls.clear()
    with pytest.raises(SnapshotError, match="--replace-cluster-file"):
        await restore("snap", ctx)
    assert docker.calls == []


async def test_restore_unknown_snapshot(world) -> None:
    ctx, *_ = world
    with pytest.raises(UnknownSnapshotError):
        await restore("nope", ctx)


async def test_restore_puts_everything_back_and_replays_the_day(world) -> None:
    ctx, host, docker, server, services = world
    server.proxies[SHARED]["enabled"] = False
    server.proxies[SHARED]["toxics"].append(
        {
            "name": "f1-latency-up",
            "type": "latency",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": {"latency": 100, "jitter": 0},
        }
    )
    await save("snap", ctx)
    # the run goes on after the snapshot, then a fault is healed
    log = ctx.runs_dir / "run-a" / "orders.jsonl"
    log.write_text(log.read_text() + json.dumps({"kind": "order", "external_order_id": "2"}) + "\n")
    server.proxies[SHARED]["toxics"].clear()
    server.proxies[SHARED]["enabled"] = True
    docker.calls.clear()
    host.commands.clear()

    result = await restore("snap", ctx)

    assert services == ["down", "up"]
    assert host.commands[0] == {"action": "stop"}
    assert docker.calls[0] == ("stop", DATABASES)
    assert ("create_missing", DATABASES) in docker.calls
    assert {call[1] for call in docker.calls if call[0] == "unpack"} == {
        volume_name(node) for node in DATABASES
    }
    assert ("start", DATABASES) in docker.calls
    assert (ctx.generated_dir / "compose.yaml").is_file()
    # the run log is the snapshot's again: order 2 came after it
    assert '"external_order_id": "2"' not in log.read_text()
    # Toxiproxy: healed, then the cut proxy is cut again; the toxic is
    # reported until the faults-control PR brings add_toxic
    assert host.healed
    assert server.proxies[SHARED]["enabled"] is False
    assert server.proxies[SHARED]["toxics"] == []
    assert any(f"{SHARED}/f1-latency-up" in warning for warning in result["warnings"])
    # the resume: the snapshot's day from midnight, as a new run
    start = next(body for body in host.commands if body["action"] == "start")
    assert start["from_day"] == 23 and start["to_day"] == 28 and start["seed"] == 7
    assert start["run_id"] == "run-a-r1"
    assert start["duplicate_rate"] == 0.05
    assert start["skip_first_restock"] is False  # day 23 had no restock
    assert start["speed"] == ctx.sim.snapshot_catchup_speed
    assert host.speeds == [1000.0]  # switched back before the snapshot point
    pointer = json.loads((ctx.runs_dir / "run-a-r1" / RESUMED_FROM).read_text())
    assert pointer["parent_run"] == "run-a" and pointer["snapshot"] == "snap"
    assert result["resume"]["started"] is True and result["resume"]["warnings"] == []
    assert result["cluster_file_replaced"] is False


async def test_restore_skips_the_restock_of_a_day_it_already_had(world) -> None:
    ctx, host, _, _, _ = world
    host.day = 21  # day 21's restock line is in the run log
    await save("restocked", ctx)
    await restore("restocked", ctx)
    start = next(body for body in host.commands if body["action"] == "start")
    assert start["from_day"] == 21 and start["skip_first_restock"] is True


async def test_restore_with_the_flag_replaces_the_cluster_file(world) -> None:
    ctx, *_ = world
    await save("snap", ctx)
    ctx.cluster_file.write_text("[cluster]\nregions = 1\nstandbys = false\n", encoding="utf-8")
    result = await restore("snap", ctx, replace_cluster_file=True)
    assert ctx.cluster_file.read_text() == TOML
    assert result["cluster_file_replaced"] is True
    assert "restart any process" in result["warnings"][0]


async def test_restore_without_a_playback_host_says_how_to_resume(world) -> None:
    ctx, host, _, _, _ = world
    await save("snap", ctx)
    host.alive = False
    result = await restore("snap", ctx)
    resume = result["resume"]
    assert resume["started"] is False
    assert "rosctl playback start --from-day 23" in resume["warnings"][0]


async def test_restore_warns_when_the_host_runs_other_engine_flags(world) -> None:
    ctx, host, _, _, _ = world
    await save("snap", ctx)
    host.engine_meta = {"target_url": "http://127.0.0.1:8000/api/v1", "sequential": True}
    result = await restore("snap", ctx)
    assert any("sequential" in warning for warning in result["resume"]["warnings"])

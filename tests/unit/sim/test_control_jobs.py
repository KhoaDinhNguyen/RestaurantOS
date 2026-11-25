"""The job routes: scenarios, snapshots, restore and the checker over
HTTP (architecture Section 6.3), on the fake world of test_scenarios."""

from __future__ import annotations

import sys

import httpx
import pytest
from fastapi import FastAPI
from test_scenarios import World

from restaurantos.sim.control_jobs import add_job_routes
from restaurantos.sim.jobs import JobRunner


@pytest.fixture
async def setup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    world = World(tmp_path)
    jobs = JobRunner()
    app = FastAPI()
    add_job_routes(
        app,
        jobs=jobs,
        scenarios=world.env,
        cluster_file=tmp_path / "cluster.toml",
        control_port=8002,
        runs_dir=world.env.runs_dir,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://control.test"
    ) as client:
        yield world, jobs, client
    await jobs.aclose()


async def test_scenarios_are_listed(setup) -> None:
    _world, _jobs, client = setup
    listed = (await client.get("/sim/scenarios")).json()
    assert [entry["name"] for entry in listed] == ["T1", "T1b", "T2", "T3", "T4", "T5", "T6"]


async def test_a_scenario_runs_as_a_job(setup) -> None:
    world, jobs, client = setup
    response = await client.post("/sim/scenarios/t5", json={"fault_s": 5})
    assert response.status_code == 202
    job = response.json()
    assert job["kind"] == "scenario" and job["name"] == "T5" and job["state"] == "running"
    assert "progress" in job
    done = await jobs.wait(job["job_id"])
    assert done.state == "done", done.error
    assert done.result["passed"] is True
    assert done.progress == "T5 passed"
    assert world.faults.log[0] == ("create", "node:r1-primary", "latency")


async def test_scenario_refusals(setup) -> None:
    world, _jobs, client = setup
    unknown = await client.post("/sim/scenarios/T9")
    assert unknown.status_code == 404 and unknown.json()["error"] == "unknown_scenario"
    bad = await client.post("/sim/scenarios/T2", json={"latency_ms": 3})
    assert bad.status_code == 422 and bad.json()["error"] == "invalid_params"
    await world.faults.create("repl:r1", "cut", {})
    live = await client.post("/sim/scenarios/T2")
    assert live.status_code == 409 and live.json()["error"] == "scenario_refused"
    assert "heal them first" in live.json()["message"]


async def test_refused_over_unexplained_link_state(setup) -> None:
    world, _jobs, client = setup
    world.faults.links_answer = [
        {
            "group": "link:r1<->hq",
            "unexplained": {"disabled": ["gateway__r1-primary"], "toxics": [], "missing": []},
        }
    ]
    answer = await client.post("/sim/scenarios/T2")
    assert answer.status_code == 409 and answer.json()["error"] == "scenario_refused"
    assert "gateway__r1-primary" in answer.json()["message"]
    assert "heal-all" in answer.json()["message"]


async def test_refused_with_501_while_the_checker_is_missing(setup) -> None:
    world, _jobs, client = setup
    world.env.ready = lambda: "the checker is not on this branch (sim/checker.py, PR #33)"
    answer = await client.post("/sim/scenarios/T2")
    assert answer.status_code == 501 and answer.json()["error"] == "not_built"


async def test_one_job_at_a_time(setup) -> None:
    _world, jobs, client = setup
    first = (await client.post("/sim/scenarios/T5", json={"fault_s": 5})).json()
    busy = await client.post("/sim/scenarios/T2")
    assert busy.status_code == 409
    assert busy.json()["error"] == "busy" and busy.json()["job_id"] == first["job_id"]
    await jobs.wait(first["job_id"])


async def test_check_and_snapshots_answer_501_without_their_modules(setup, monkeypatch) -> None:
    _world, _jobs, client = setup
    monkeypatch.setitem(sys.modules, "restaurantos.sim.checker", None)
    monkeypatch.setitem(sys.modules, "restaurantos.sim.snapshot", None)
    check = await client.post("/sim/check", json={"runs": ["r"]})
    assert check.status_code == 501 and check.json()["error"] == "not_built"
    assert (await client.get("/sim/snapshots")).status_code == 501
    assert (await client.post("/sim/snapshots", json={"name": "a"})).status_code == 501
    assert (await client.post("/sim/snapshots/a/restore")).status_code == 501


async def test_check_validates_its_runs(setup) -> None:
    pytest.importorskip("restaurantos.sim.checker")
    world, jobs, client = setup
    missing = await client.post("/sim/check", json={"runs": ["nope"]})
    assert missing.status_code == 404 and missing.json()["error"] == "unknown_run"
    empty = await client.post("/sim/check", json={"runs": []})
    assert empty.status_code == 422
    # a run that is still live is refused
    await world.playback.start(
        seed=0, from_day=14, to_day=99, speed=1.0, run_id="live-1", meta_extra={}
    )
    live = await client.post("/sim/check", json={"runs": ["live-1"]})
    assert live.status_code == 409 and live.json()["error"] == "run_live"


async def test_snapshot_names_are_checked(setup, monkeypatch, tmp_path) -> None:
    pytest.importorskip("restaurantos.sim.snapshot")
    from restaurantos.sim.config import get_sim_settings

    get_sim_settings.cache_clear()
    monkeypatch.setenv("ROS_SIM_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    _world, _jobs, client = setup
    bad = await client.post("/sim/snapshots", json={"name": "../x"})
    assert bad.status_code == 422 and bad.json()["error"] == "invalid_name"
    (tmp_path / "snapshots" / "taken").mkdir(parents=True)
    taken = await client.post("/sim/snapshots", json={"name": "taken"})
    assert taken.status_code == 409 and taken.json()["error"] == "snapshot_exists"
    unknown = await client.post("/sim/snapshots/nope/restore")
    assert unknown.status_code == 404 and unknown.json()["error"] == "unknown_snapshot"
    get_sim_settings.cache_clear()

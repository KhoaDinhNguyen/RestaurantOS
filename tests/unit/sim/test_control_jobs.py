"""The job routes: the scenarios over HTTP (architecture Section 6.3),
on the fake world of test_scenarios."""

from __future__ import annotations

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


async def test_one_job_at_a_time(setup) -> None:
    _world, jobs, client = setup
    first = (await client.post("/sim/scenarios/T5", json={"fault_s": 5})).json()
    busy = await client.post("/sim/scenarios/T2")
    assert busy.status_code == 409
    assert busy.json()["error"] == "busy" and busy.json()["job_id"] == first["job_id"]
    await jobs.wait(first["job_id"])

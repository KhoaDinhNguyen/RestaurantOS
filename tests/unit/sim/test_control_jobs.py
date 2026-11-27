"""The scenario routes over HTTP (architecture Section 6.3), on the fake
world of test_scenarios. The snapshot, restore and check routes are
`control_api.add_sim_jobs` (PR #33) and are tested in test_control_api."""

from __future__ import annotations

from typing import Any

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
    add_job_routes(app, jobs=jobs, scenarios=world.env)
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


async def test_t1_without_a_streaming_standby_refuses_at_the_post(setup) -> None:
    world, _jobs, client = setup
    world.probe.streaming_count = 0
    answer = await client.post("/sim/scenarios/T1")
    assert answer.status_code == 409 and answer.json()["error"] == "scenario_refused"
    assert "rejoin" in answer.json()["message"]
    # nothing was accepted: no run and no job
    assert world.playback.started is None


async def test_a_probe_that_cannot_read_still_accepts_the_scenario(setup) -> None:
    world, jobs, client = setup
    original = world.probe.region
    calls = {"count": 0}

    def flaky(region_id: str) -> dict[str, Any]:
        calls["count"] += 1
        if calls["count"] == 1:  # only the POST's pre-start check fails
            raise RuntimeError("the databases are busy")
        return original(region_id)

    world.probe.region = flaky  # type: ignore[method-assign]
    answer = await client.post("/sim/scenarios/T5", json={"fault_s": 5})
    assert answer.status_code == 202
    done = await jobs.wait(answer.json()["job_id"])
    assert done.state == "done", done.error
    assert done.result["passed"] is True


async def test_a_changed_cluster_file_refuses_naming_the_restart(setup, tmp_path) -> None:
    world, _jobs, client = setup
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 1\n", encoding="utf-8")
    world.env.cluster_file = cluster_file
    answer = await client.post("/sim/scenarios/T2")
    assert answer.status_code == 409 and answer.json()["error"] == "scenario_refused"
    assert "restart rosctl control" in answer.json()["message"]
    assert world.playback.started is None


async def test_an_unchanged_cluster_file_does_not_refuse(setup, tmp_path) -> None:
    world, jobs, client = setup
    cluster_file = tmp_path / "cluster.toml"
    cluster_file.write_text("[cluster]\nregions = 2\nstandbys = true\n", encoding="utf-8")
    world.env.cluster_file = cluster_file
    answer = await client.post("/sim/scenarios/T2", json={"fault_s": 5})
    assert answer.status_code == 202
    done = await jobs.wait(answer.json()["job_id"])
    assert done.state == "done" and done.result["passed"] is True


async def test_one_job_at_a_time(setup) -> None:
    _world, jobs, client = setup
    first = (await client.post("/sim/scenarios/T5", json={"fault_s": 5})).json()
    busy = await client.post("/sim/scenarios/T2")
    assert busy.status_code == 409
    assert busy.json()["error"] == "busy" and busy.json()["job_id"] == first["job_id"]
    await jobs.wait(first["job_id"])

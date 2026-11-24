"""The links/faults/nodes/jobs routes: Section 6.3's bodies and codes."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fake_compose import FakeCompose
from fake_toxiproxy import FakeToxiproxyServer
from fastapi import FastAPI

from restaurantos.common.cluster import Cluster, link_groups, proxies
from restaurantos.sim.control_api import add_control_routes
from restaurantos.sim.faults import FaultLayer
from restaurantos.sim.jobs import JobRunner
from restaurantos.sim.nodes import Nodes

CLUSTER = Cluster(regions=1)


@pytest.fixture
def server() -> FakeToxiproxyServer:
    return FakeToxiproxyServer(proxy.name for proxy in proxies(CLUSTER))


@pytest.fixture
async def app_and_client(server):
    toxiproxy = server.client()
    app = FastAPI()
    jobs = JobRunner()
    add_control_routes(
        app,
        faults=FaultLayer(toxiproxy, link_groups(CLUSTER)),
        nodes=Nodes(CLUSTER, compose=FakeCompose({"r1-primary"})),
        jobs=jobs,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://control.test"
    ) as client:
        yield app, client
    await jobs.aclose()
    await toxiproxy.aclose()


@pytest.fixture
def client(app_and_client):
    return app_and_client[1]


async def test_create_answers_201_with_the_fault(client) -> None:
    response = await client.post(
        "/sim/faults",
        json={"target": "link:r1<->hq", "fault": "latency", "params": {"latency_ms": 4000}},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["fault_id"] == "f1"
    assert body["target"] == "link:r1<->hq"
    assert body["fault"] == "latency"
    assert body["params"] == {"latency_ms": 4000, "jitter_ms": 0}
    assert body["proxies"] == link_groups(CLUSTER)["link:r1<->hq"]
    assert body["created_at"]


async def test_create_error_codes(client) -> None:
    unknown = await client.post("/sim/faults", json={"target": "node:nope", "fault": "cut"})
    assert unknown.status_code == 404 and unknown.json()["error"] == "unknown_group"
    bad = await client.post("/sim/faults", json={"target": "repl:r1", "fault": "latency"})
    assert bad.status_code == 422 and bad.json()["error"] == "invalid_params"
    first = await client.post("/sim/faults", json={"target": "repl:r1", "fault": "cut"})
    assert first.status_code == 201
    again = await client.post("/sim/faults", json={"target": "repl:r1", "fault": "cut"})
    assert again.status_code == 409
    assert again.json()["error"] == "fault_exists"
    assert again.json()["fault_id"] == first.json()["fault_id"]
    # body validation keeps FastAPI's shape
    missing = await client.post("/sim/faults", json={"fault": "cut"})
    assert missing.status_code == 422 and "detail" in missing.json()


async def test_heal_and_heal_all(client) -> None:
    created = (await client.post("/sim/faults", json={"target": "repl:r1", "fault": "cut"})).json()
    healed = await client.delete(f"/sim/faults/{created['fault_id']}")
    assert healed.status_code == 200 and healed.json()["fault_id"] == created["fault_id"]
    gone = await client.delete(f"/sim/faults/{created['fault_id']}")
    assert gone.status_code == 404 and gone.json()["error"] == "unknown_fault"
    await client.post("/sim/faults", json={"target": "repl:r1", "fault": "silence"})
    await client.post("/sim/faults", json={"target": "node:r1-primary", "fault": "cut"})
    response = await client.post("/sim/faults/heal-all")
    assert response.status_code == 200 and response.json() == {"healed": 2}


async def test_links_list_every_group_with_faults_and_unexplained(client) -> None:
    await client.post("/sim/faults", json={"target": "repl:r1", "fault": "silence"})
    links = (await client.get("/sim/links")).json()
    assert {entry["group"] for entry in links} == set(link_groups(CLUSTER))
    repl = next(entry for entry in links if entry["group"] == "repl:r1")
    assert [fault["fault"] for fault in repl["faults"]] == ["silence"]
    assert set(repl) == {"group", "proxies", "faults", "unexplained"}


async def test_toxiproxy_down_is_a_502(client, server) -> None:
    server.fail.add(("GET", "/proxies"))
    response = await client.get("/sim/links")
    assert response.status_code == 502 and response.json()["error"] == "toxiproxy_error"


async def test_nodes(client) -> None:
    killed = await client.post("/sim/nodes/r1-primary/kill")
    assert killed.status_code == 200
    assert killed.json() == {"node_id": "r1-primary", "running": False}
    started = await client.post("/sim/nodes/r1-primary/start")
    assert started.json() == {"node_id": "r1-primary", "running": True}
    unknown = await client.post("/sim/nodes/baseline/kill")
    assert unknown.status_code == 404 and unknown.json()["error"] == "unknown_node"


async def test_jobs_route(app_and_client) -> None:
    app, client = app_and_client
    release = asyncio.Event()

    async def work() -> dict:
        await release.wait()
        return {"passed": True}

    job = app.state.jobs.submit("check", None, work)
    running = (await client.get(f"/sim/jobs/{job.job_id}")).json()
    assert running["state"] == "running" and running["kind"] == "check"
    assert set(running) == {
        "job_id",
        "kind",
        "name",
        "state",
        "started_at",
        "finished_at",
        "result",
        "error",
    }
    release.set()
    await app.state.jobs.wait(job.job_id)
    done = (await client.get(f"/sim/jobs/{job.job_id}")).json()
    assert done["state"] == "done" and done["result"] == {"passed": True}
    missing = await client.get("/sim/jobs/j99")
    assert missing.status_code == 404 and missing.json()["error"] == "unknown_job"

"""The links/faults/nodes/jobs routes: Section 6.3's bodies and codes."""

from __future__ import annotations

import asyncio
import re

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


async def _app_and_client(server):
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
async def app_and_client(server):
    async for result in _app_and_client(server):
        yield result


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
    assert set(repl) == {"group", "proxies", "faults", "preexisting_disabled", "unexplained"}


async def test_a_cut_over_a_pre_disabled_proxy_is_left_alone(client, server) -> None:
    shared = "r1-standby__r1-primary"
    server.proxies[shared]["enabled"] = False  # someone else's state
    created = await client.post("/sim/faults", json={"target": "repl:r1", "fault": "cut"})
    assert created.json()["already_disabled"] == [shared]
    links = next(
        entry for entry in (await client.get("/sim/links")).json() if entry["group"] == "repl:r1"
    )
    assert links["preexisting_disabled"] == [shared]
    healed = await client.delete("/sim/faults/f1")
    assert healed.json()["left_disabled"] == [shared]
    assert server.proxies[shared]["enabled"] is False  # untouched
    links = next(
        entry for entry in (await client.get("/sim/links")).json() if entry["group"] == "repl:r1"
    )
    assert links["preexisting_disabled"] == []
    assert links["unexplained"]["disabled"] == [shared]


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


# --- cross-site protection (CodeRabbit review, finding 3) -------------------


@pytest.fixture
def fresh_sim_settings(monkeypatch):
    from restaurantos.sim.config import get_sim_settings

    get_sim_settings.cache_clear()
    monkeypatch.setenv(
        "ROS_SIM_CONTROL_ALLOWED_ORIGINS", "http://localhost:5173, https://ui.internal:8443/"
    )
    get_sim_settings.cache_clear()
    yield
    get_sim_settings.cache_clear()


async def test_cross_site_markers_are_rejected(empty_allowlist_app_and_client) -> None:
    """Sec-Fetch-Site: cross-site or a foreign Origin rejects the
    state-changing routes: kill, start, heal-all and a JSON-body one."""
    client = empty_allowlist_app_and_client[1]
    requests = [
        ("POST", "/sim/nodes/r1-primary/kill", None, {"Sec-Fetch-Site": "cross-site"}),
        ("POST", "/sim/nodes/r1-primary/start", None, {"Sec-Fetch-Site": "cross-site"}),
        ("POST", "/sim/faults/heal-all", None, {"Sec-Fetch-Site": "cross-site"}),
        (
            "POST",
            "/sim/faults",
            {"target": "repl:r1", "fault": "cut"},
            {"Origin": "http://evil.example"},
        ),
        ("POST", "/sim/faults/heal-all", None, {"Origin": "null"}),
    ]
    for method, path, body, headers in requests:
        response = await client.request(method, path, json=body, headers=headers)
        assert response.status_code == 403, (method, path, headers)
        assert response.json() == {"detail": "cross-origin request rejected"}


async def test_a_browser_origin_must_match_the_allowlist_exactly(client, fresh_sim_settings):
    # the allowlisted origin goes through
    allowed = await client.post(
        "/sim/nodes/r1-primary/kill", headers={"Origin": "http://localhost:5173"}
    )
    assert allowed.status_code == 200
    for origin in (
        "http://localhost:5174",  # a different port
        "http://localhost.evil.com",
        "http://localhost:5173.evil.com",
        "http://localhost",
    ):
        rejected = await client.request("POST", "/sim/faults/heal-all", headers={"Origin": origin})
        assert rejected.status_code == 403, origin
    # a configured origin with a trailing slash matches its slashless form
    slashed = await client.request(
        "POST", "/sim/nodes/r1-primary/start", headers={"Origin": "https://ui.internal:8443"}
    )
    assert slashed.status_code == 200


async def test_requests_without_origin_and_same_site_markers_pass(client) -> None:
    # the CLI's shape: no Origin, no Sec-Fetch-Site
    killed = await client.post("/sim/nodes/r1-primary/kill")
    assert killed.status_code == 200 and killed.json()["running"] is False
    unknown = await client.post("/sim/nodes/r9-primary/kill")
    assert unknown.status_code == 404  # still the route's own answer
    assert (await client.post("/sim/faults/heal-all")).status_code == 200
    for marker in ("same-origin", "same-site", "none"):
        response = await client.post(
            "/sim/nodes/r1-primary/start", headers={"Sec-Fetch-Site": marker}
        )
        assert response.status_code == 200, marker


async def test_gets_with_a_foreign_origin_are_still_served(client) -> None:
    response = await client.get("/sim/links", headers={"Origin": "http://evil.example"})
    assert response.status_code == 200


async def test_the_playback_routes_get_the_same_guard(empty_allowlist) -> None:
    from restaurantos.sim.control import make_control_app

    calls: list[str] = []

    class StubPlayback:
        def clock(self) -> dict:
            return {}

        def status(self) -> dict:
            return {}

        async def start(self, **kwargs) -> None:
            calls.append("start")

        async def pause(self) -> None:
            calls.append("pause")

        async def resume(self) -> None:
            calls.append("resume")

        async def stop(self) -> None:
            calls.append("stop")

        async def set_speed(self, speed: float) -> None:
            calls.append("speed")

    app = make_control_app(StubPlayback())  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://control.test"
    ) as playback_client:
        rejected = await playback_client.post(
            "/sim/playback", json={"action": "pause"}, headers={"Origin": "http://evil.example"}
        )
        assert rejected.status_code == 403
        assert rejected.json() == {"detail": "cross-origin request rejected"}
        assert calls == []  # the route never ran
        served = await playback_client.post("/sim/playback", json={"action": "pause"})
        assert served.status_code == 200
        assert calls == ["pause"]


# --- CORS: browser clients read answers, the guard blocks (round 3) --------


def test_origin_normalisation() -> None:
    from restaurantos.sim.config import normalise_origin, parse_allowed_origins

    assert normalise_origin("HTTP://LocalHost:5173/") == "http://localhost:5173"
    assert normalise_origin("https://x.test:443") == "https://x.test"
    assert normalise_origin(" http://[::1]:8002 ") == "http://[::1]:8002"
    # IPv6 literals canonicalise to the compressed form a browser sends
    for entry in ("http://[0:0:0:0:0:0:0:1]:5173", "http://[::1]:5173", "HTTP://[0::1]:5173/"):
        assert normalise_origin(entry) == "http://[::1]:5173"
    assert parse_allowed_origins("http://[0:0:0:0:0:0:0:1]:5173,http://[::1]:5173") == (
        "http://[::1]:5173",
    )
    assert normalise_origin("http://[::1]") == "http://[::1]"
    assert normalise_origin("http://[::1]:80") == "http://[::1]"
    # zone ids and IPv4-mapped forms are rejected on purpose (see
    # normalise_origin): they never reach a browser's Origin header
    for unsupported in ("http://[fe80::1%25eth0]:5173", "http://[::ffff:127.0.0.1]:5173"):
        with pytest.raises(ValueError):
            normalise_origin(unsupported)
    # non-ASCII hostnames are rejected: browsers send the punycode form
    with pytest.raises(ValueError, match="punycode|xn--"):
        normalise_origin("http://bücher.example:5173")
    with pytest.raises(ValueError, match="punycode|xn--"):
        normalise_origin("http://straße.example")
    assert normalise_origin("HTTP://XN--BCHER-KVA.example:5173/") == (
        "http://xn--bcher-kva.example:5173"
    )
    for bad in (
        "*",
        "null",
        "localhost:5173",
        "http://localhost:5173/app",
        "http://localhost:5173?q=1",
        "ftp://x.test",
        "http://user:pass@localhost:5173",
        "",
    ):
        with pytest.raises(ValueError, match=re.escape(bad) if bad else "invalid origin"):
            normalise_origin(bad)
    # duplicates (after normalisation) collapse, order is kept
    assert parse_allowed_origins("http://a.test:1, http://a.test:1/,http://b.test") == (
        "http://a.test:1",
        "http://b.test",
    )
    assert parse_allowed_origins("") == ()
    # one bad entry is enough to reject the whole setting
    with pytest.raises(ValueError):
        parse_allowed_origins("http://ok.test,*")


@pytest.fixture
def cors_allowlist(monkeypatch):
    from restaurantos.sim.config import get_sim_settings

    get_sim_settings.cache_clear()
    monkeypatch.setenv("ROS_SIM_CONTROL_ALLOWED_ORIGINS", "http://localhost:5173")
    get_sim_settings.cache_clear()
    yield
    get_sim_settings.cache_clear()


@pytest.fixture
async def allowlisted_app_and_client(cors_allowlist, server):
    """The app built AFTER the allowlist env is set: explicit
    dependency, no reliance on fixture argument order."""
    async for result in _app_and_client(server):
        yield result


@pytest.fixture
def empty_allowlist(monkeypatch):
    """Pin the setting to empty: the app must not register CORS even if
    a shell variable or a developer .env sets a non-empty allowlist."""
    from restaurantos.sim.config import get_sim_settings

    monkeypatch.setenv("ROS_SIM_CONTROL_ALLOWED_ORIGINS", "")
    get_sim_settings.cache_clear()
    yield
    get_sim_settings.cache_clear()


@pytest.fixture
async def empty_allowlist_app_and_client(empty_allowlist, server):
    async for result in _app_and_client(server):
        yield result


async def test_a_preflight_from_a_listed_origin_is_answered(allowlisted_app_and_client) -> None:
    client = allowlisted_app_and_client[1]
    response = await client.options(
        "/sim/faults/heal-all",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )
    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:5173"
    assert "POST" in response.headers["Access-Control-Allow-Methods"]
    assert "Access-Control-Allow-Credentials" not in response.headers


async def test_a_preflight_from_an_unlisted_origin_fails(allowlisted_app_and_client) -> None:
    response = await allowlisted_app_and_client[1].options(
        "/sim/faults/heal-all",
        headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "POST"},
    )
    assert response.status_code == 400
    assert "Access-Control-Allow-Origin" not in response.headers


async def test_a_preflight_for_an_unlisted_header_fails(allowlisted_app_and_client) -> None:
    response = await allowlisted_app_and_client[1].options(
        "/sim/faults/heal-all",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "x-foo",
        },
    )
    assert response.status_code == 400


async def test_a_listed_origin_post_carries_cors_headers(allowlisted_app_and_client) -> None:
    response = await allowlisted_app_and_client[1].post(
        "/sim/faults/heal-all",
        headers={"Origin": "http://localhost:5173", "Sec-Fetch-Site": "same-site"},
    )
    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:5173"


async def test_an_unlisted_origin_post_reaches_the_guard_without_cors_headers(
    allowlisted_app_and_client,
) -> None:
    # CORS does not block real requests: the guard does, and its 403
    # carries no CORS headers for an origin the browser must not read
    response = await allowlisted_app_and_client[1].post(
        "/sim/faults/heal-all", headers={"Origin": "http://evil.example"}
    )
    assert response.status_code == 403
    assert "Access-Control-Allow-Origin" not in response.headers


async def test_the_guards_403_carries_cors_headers(allowlisted_app_and_client) -> None:
    # layer ordering: CORS sits outside the guard, so a listed origin
    # can read even the rejection (the UI shows the message instead of
    # a vague "CORS error")
    response = await allowlisted_app_and_client[1].post(
        "/sim/faults/heal-all",
        headers={"Origin": "http://localhost:5173", "Sec-Fetch-Site": "cross-site"},
    )
    assert response.status_code == 403
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:5173"


async def test_cli_requests_get_no_cors_headers(app_and_client) -> None:
    response = await app_and_client[1].post("/sim/faults/heal-all")
    assert response.status_code == 200
    assert "Access-Control-Allow-Origin" not in response.headers


async def test_an_empty_allowlist_answers_no_preflights(empty_allowlist_app_and_client) -> None:
    # the default is CLI-only: no CORS middleware, so a preflight-looking
    # OPTIONS falls through to the router (405) with no CORS headers
    response = await empty_allowlist_app_and_client[1].options(
        "/sim/faults/heal-all",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert response.status_code == 405
    assert "Access-Control-Allow-Origin" not in response.headers


async def test_cors_methods_cover_every_route(server) -> None:
    """Drift: a new route using a method outside CONTROL_CORS_METHODS
    would silently break browser access (its preflight would 400)."""
    from fastapi.routing import APIRoute

    from restaurantos.sim.control import make_control_app
    from restaurantos.sim.control_api import CONTROL_CORS_METHODS

    class StubPlayback:
        def clock(self) -> dict:
            return {}

        def status(self) -> dict:
            return {}

    app = make_control_app(StubPlayback())  # type: ignore[arg-type]
    add_control_routes(
        app,
        faults=FaultLayer(server.client(), link_groups(CLUSTER)),
        nodes=Nodes(CLUSTER, compose=FakeCompose({"r1-primary"})),
        jobs=JobRunner(),
    )
    used = {m for r in app.routes if isinstance(r, APIRoute) for m in r.methods} - {
        "HEAD",
        "OPTIONS",
    }
    assert used <= set(CONTROL_CORS_METHODS), used - set(CONTROL_CORS_METHODS)


@pytest.fixture
def ipv6_allowlist(monkeypatch):
    from restaurantos.sim.config import get_sim_settings

    get_sim_settings.cache_clear()
    monkeypatch.setenv("ROS_SIM_CONTROL_ALLOWED_ORIGINS", "http://[0:0:0:0:0:0:0:1]:5173")
    get_sim_settings.cache_clear()
    yield
    get_sim_settings.cache_clear()


@pytest.fixture
async def ipv6_app_and_client(ipv6_allowlist, server):
    async for result in _app_and_client(server):
        yield result


async def test_an_expanded_ipv6_entry_matches_the_browsers_form(ipv6_app_and_client) -> None:
    """A config entry written in expanded IPv6 normalises to the
    compressed form the browser sends, so the guard and CORS both
    recognise the request."""
    response = await ipv6_app_and_client[1].post(
        "/sim/faults/heal-all", headers={"Origin": "http://[::1]:5173"}
    )
    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "http://[::1]:5173"


@pytest.fixture
def punycode_allowlist(monkeypatch):
    from restaurantos.sim.config import get_sim_settings

    get_sim_settings.cache_clear()
    monkeypatch.setenv("ROS_SIM_CONTROL_ALLOWED_ORIGINS", "http://xn--bcher-kva.example:5173")
    get_sim_settings.cache_clear()
    yield
    get_sim_settings.cache_clear()


@pytest.fixture
async def punycode_app_and_client(punycode_allowlist, server):
    async for result in _app_and_client(server):
        yield result


async def test_a_punycode_origin_passes_guard_and_cors(punycode_app_and_client) -> None:
    """Guard and CORS take their allowlist from the same normalised
    tuple: an Origin browsers actually send passes both."""
    origin = "http://xn--bcher-kva.example:5173"
    posted = await punycode_app_and_client[1].post(
        "/sim/faults/heal-all", headers={"Origin": origin, "Sec-Fetch-Site": "same-site"}
    )
    assert posted.status_code == 200
    assert posted.headers["Access-Control-Allow-Origin"] == origin
    preflight = await punycode_app_and_client[1].options(
        "/sim/faults/heal-all",
        headers={"Origin": origin, "Access-Control-Request-Method": "POST"},
    )
    assert preflight.status_code == 200
    assert preflight.headers["Access-Control-Allow-Origin"] == origin


# --- the check and snapshot job routes (the WP6b join) -----------------------


class _NoHost:
    """A PlaybackClient stand-in: no playback host answers."""

    bases: list[str] = []

    def __init__(self, base_url: str, **kwargs) -> None:
        self.bases.append(base_url)

    async def status(self):
        return None


async def test_check_snapshot_and_restore_run_as_jobs(server, monkeypatch, tmp_path) -> None:
    """The join commit: rosctl check and rosctl snapshot save|restore as
    jobs on the control runner - one at a time, polled through
    /sim/jobs/{id} - and GET /sim/snapshots as a plain read. The work is
    faked here; the CLI tests cover the real code."""
    import restaurantos.common.cluster as common_cluster
    import restaurantos.common.config as common_config
    import restaurantos.sim.checker as checker
    import restaurantos.sim.config as sim_config
    import restaurantos.sim.snapshot as snapshot
    from restaurantos.sim.control_api import add_sim_jobs

    seen: dict = {}

    async def fake_save(name, ctx):
        seen["saved"] = name
        return {"name": name}

    async def fake_restore(name, ctx, *, replace_cluster_file=False, services="auto"):
        seen["restored"] = (name, replace_cluster_file, services)
        return {"name": name}

    def fake_run_check(cluster, run_ids, **kwargs):
        seen["checked"] = (list(run_ids), kwargs.get("allow_unacked"), kwargs.get("live_run"))
        return {"passed": True}

    monkeypatch.setattr(snapshot, "save", fake_save)
    monkeypatch.setattr(snapshot, "restore", fake_restore)
    monkeypatch.setattr(checker, "run_check", fake_run_check)
    ports: list = []

    def fake_context(cluster_file, sim, **kwargs):
        ports.append(kwargs.get("port"))
        return object()

    monkeypatch.setattr(snapshot, "default_context", fake_context)
    monkeypatch.setattr(snapshot, "PlaybackClient", _NoHost)
    _NoHost.bases.clear()
    monkeypatch.setattr(common_cluster, "load_cluster", lambda path: CLUSTER)
    monkeypatch.setattr(
        common_config,
        "get_settings",
        lambda: type("S", (), {"cluster_file": str(tmp_path / "cluster.toml")})(),
    )
    monkeypatch.setenv("ROS_SIM_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    sim_config.get_sim_settings.cache_clear()

    from restaurantos.sim.config import get_sim_settings  # noqa: F401 - cache warmed

    app = FastAPI()
    jobs = JobRunner()
    add_control_routes(
        app,
        faults=FaultLayer(server.client(), link_groups(CLUSTER)),
        nodes=Nodes(CLUSTER),
        jobs=jobs,
    )
    # a control process on another port: its jobs reach its own playback there
    add_sim_jobs(app, port=8099)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://control.test"
    ) as client:

        async def done(job_id: str) -> dict:
            await jobs.wait(job_id)
            return (await client.get(f"/sim/jobs/{job_id}")).json()

        # the check: 202 with the job, the result through the job
        posted = await client.post("/sim/check", json={"runs": ["run-a"]})
        assert posted.status_code == 202
        body = posted.json()
        assert body["kind"] == "check" and body["name"] is None
        assert (await done(body["job_id"]))["result"] == {"passed": True}
        assert seen["checked"] == (["run-a"], False, None)
        posted = await client.post("/sim/check", json={"runs": ["run-b"], "allow_unacked": True})
        assert (await done(posted.json()["job_id"]))["result"] == {"passed": True}
        assert seen["checked"] == (["run-b"], True, None)
        # the runs are run ids: at least one, each one path component
        for runs in ([], ["../etc"], ["run-a", "a/b"], [".hidden"], [""]):
            bad = await client.post("/sim/check", json={"runs": runs})
            assert bad.status_code == 422, runs

        # the listing is a plain read of the snapshot directory
        listed = await client.get("/sim/snapshots")
        assert listed.status_code == 200 and listed.json() == {"snapshots": []}

        # save and restore are jobs too; a bad name answers 422
        posted = await client.post("/sim/snapshots", json={"name": "evening"})
        assert posted.status_code == 202 and posted.json()["kind"] == "snapshot"
        assert posted.json()["name"] == "evening"
        await done(posted.json()["job_id"])
        assert seen["saved"] == "evening"
        bad = await client.post("/sim/snapshots", json={"name": "../escape"})
        assert bad.status_code == 422
        # a taken name and an unknown snapshot are answered at once, not
        # as failed jobs (nothing is queued)
        snapshots = tmp_path / "snapshots"
        (snapshots / "evening").mkdir(parents=True)
        queued = len(jobs._jobs)
        taken = await client.post("/sim/snapshots", json={"name": "evening"})
        assert taken.status_code == 409 and taken.json()["error"] == "snapshot_exists"
        unknown = await client.post("/sim/snapshots/day7/restore")
        assert unknown.status_code == 404 and unknown.json()["error"] == "unknown_snapshot"
        bad = await client.post("/sim/snapshots/.hidden/restore")
        assert bad.status_code == 422 and bad.json()["error"] == "invalid_name"
        assert len(jobs._jobs) == queued
        (snapshots / "day7").mkdir()
        (snapshots / "day7" / "snapshot.json").write_text("{}", encoding="utf-8")
        posted = await client.post(
            "/sim/snapshots/day7/restore",
            json={"replace_cluster_file": True, "services": "none"},
        )
        assert posted.status_code == 202 and posted.json()["kind"] == "restore"
        assert posted.json()["name"] == "day7"
        await done(posted.json()["job_id"])
        assert seen["restored"] == ("day7", True, "none")
        posted = await client.post("/sim/snapshots/day7/restore")
        await done(posted.json()["job_id"])
        assert seen["restored"] == ("day7", False, "auto")  # no body: the defaults
        # every job asked the control process's own port, never the default
        assert ports and set(ports) == {8099}
        assert _NoHost.bases and all(base.endswith(":8099") for base in _NoHost.bases)
        bad = await client.post("/sim/snapshots/day7/restore", json={"services": "both"})
        assert bad.status_code == 422

        # one job at a time: a busy runner answers 409 with the job id
        release = asyncio.Event()

        async def blocked() -> None:
            await release.wait()

        running = app.state.jobs.submit("check", None, blocked)
        busy = await client.post("/sim/check", json={"runs": ["run-a"]})
        assert busy.status_code == 409
        assert busy.json()["error"] == "busy" and busy.json()["job_id"] == running.job_id
        release.set()
        await jobs.wait(running.job_id)
    await jobs.aclose()
    sim_config.get_sim_settings.cache_clear()
    monkeypatch.delenv("ROS_SIM_SNAPSHOT_DIR")


async def test_sim_jobs_mount_with_the_control_process(server) -> None:
    """`rosctl control` serves them: _add_control_extras mounts the sim
    jobs beside the control routes."""
    from pathlib import Path

    import restaurantos.sim.playback_cli as playback_cli

    app = FastAPI()
    extras = playback_cli._add_control_extras(app, Path("infra/cluster.toml"))
    paths = {getattr(route, "path", None) for route in app.routes}
    assert {
        "/sim/check",
        "/sim/snapshots",
        "/sim/snapshots/{name}/restore",
    } <= paths
    await extras.aclose()

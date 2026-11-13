"""The Toxiproxy client against a MockTransport: no server needed."""

import json

import httpx
import pytest

from restaurantos.common.cluster import load_cluster, proxies
from restaurantos.sim.toxiproxy import Toxiproxy, ToxiproxyError


class FakeToxiproxy:
    """A stateful stand-in for the Toxiproxy server over one transport."""

    def __init__(self, existing: dict[str, dict[str, object]]) -> None:
        self.state = existing
        self.calls: list[tuple[str, str, object]] = []
        self.fail_delete: set[str] = set()

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path == "/proxies":
            return httpx.Response(200, json=self.state)
        if request.method == "POST" and request.url.path == "/proxies":
            assert isinstance(body, dict)
            if body["name"] in self.state:
                return httpx.Response(409, text="proxy already exists")
            # like the real server: proxy.go rewrites the listen address to
            # the socket it bound, so a 0.0.0.0 wildcard comes back as [::]
            if str(body.get("listen", "")).startswith("0.0.0.0:"):
                body = {**body, "listen": "[::]:" + body["listen"].split(":", 1)[1]}
            self.state[body["name"]] = body
            return httpx.Response(201, json=body)
        if request.method == "POST" and request.url.path == "/reset":
            for proxy in self.state.values():
                proxy["enabled"] = True
            return httpx.Response(200)
        if request.method == "DELETE" and request.url.path.startswith("/proxies/"):
            name = request.url.path.removeprefix("/proxies/")
            if name in self.fail_delete:
                return httpx.Response(500, text="internal error, see 404 in log")
            if name not in self.state:
                return httpx.Response(404, text="proxy not found")
            del self.state[name]
            return httpx.Response(200)
        if request.method == "POST" and request.url.path.startswith("/proxies/"):
            name = request.url.path.removeprefix("/proxies/")
            if name not in self.state:
                return httpx.Response(404, text="proxy not found")
            assert isinstance(body, dict)
            self.state[name]["enabled"] = body["enabled"]
            return httpx.Response(200, json=self.state[name])
        return httpx.Response(404, text="unknown route")


@pytest.fixture
def cluster():
    return load_cluster()


async def client(existing: dict[str, dict[str, object]]) -> tuple[Toxiproxy, FakeToxiproxy]:
    fake = FakeToxiproxy(existing)
    transport = httpx.MockTransport(fake.handler)
    tox = Toxiproxy("http://test", transport=transport)
    return tox, fake


async def test_list_normalizes_the_dict_answer(cluster):
    tox, fake = await client({"b": {"name": "b"}, "a": {"name": "a"}})
    listed = await tox.list_proxies()
    assert [entry["name"] for entry in listed] == ["a", "b"]
    await tox.aclose()


async def test_create_sends_the_cluster_body(cluster):
    tox, fake = await client({})
    proxy = proxies(cluster)[0]
    await tox.create_proxy(proxy)
    method, path, body = fake.calls[-1]
    assert (method, path) == ("POST", "/proxies")
    assert body == proxy.as_toxiproxy()
    assert body["name"] == proxy.name
    await tox.aclose()


async def test_sync_creates_missing_deletes_extras_reenables(cluster):
    wanted = proxies(cluster)
    keep_disabled = dict(wanted[1].as_toxiproxy(), enabled=False)
    tox, fake = await client(
        {
            wanted[0].name: dict(wanted[0].as_toxiproxy()),
            wanted[1].name: keep_disabled,
            "leftover": {"name": "leftover", "listen": "0.0.0.0:1", "upstream": "x:5432"},
        }
    )
    await tox.sync(cluster)
    await tox.aclose()
    names = set(fake.state)
    assert "leftover" not in names
    assert {p.name for p in wanted} <= names
    assert fake.state[wanted[1].name]["enabled"] is True
    created = [c for c in fake.calls if c[:2] == ("POST", "/proxies")]
    assert [c[2]["name"] for c in created] == [
        p.name for p in wanted if p.name not in (wanted[0].name, wanted[1].name)
    ]


async def test_sync_is_a_noop_when_the_server_already_matches(cluster):
    wanted = proxies(cluster)
    state = {p.name: dict(p.as_toxiproxy()) for p in wanted}
    tox, fake = await client(state)
    await tox.sync(cluster)
    await tox.aclose()
    assert [c for c in fake.calls if c[0] == "POST" or c[0] == "DELETE"] == []


async def test_delete_ignores_a_missing_proxy():
    tox, fake = await client({})
    await tox.delete_proxy("nope")
    await tox.aclose()


async def test_errors_carry_status_and_body():
    tox, fake = await client({})
    with pytest.raises(ToxiproxyError) as info:
        await tox.set_enabled("nope", True)
    assert "404" in str(info.value)
    await tox.aclose()


async def test_reset_reenables_everything():
    tox, fake = await client({"a": {"name": "a", "enabled": False}})
    await tox.reset()
    await tox.aclose()
    assert fake.state["a"]["enabled"] is True


async def test_delete_server_error_propagates_and_keeps_its_status():
    tox, fake = await client(
        {"boom": {"name": "boom", "listen": "0.0.0.0:1", "upstream": "x:5432"}}
    )
    fake.fail_delete = {"boom"}
    # the 500 body mentions "404": the error must be classified by status,
    # not by message text
    with pytest.raises(ToxiproxyError) as info:
        await tox.delete_proxy("boom")
    assert info.value.status == 500
    await tox.aclose()


async def test_sync_propagates_a_failed_delete(cluster):
    tox, fake = await client(
        {
            "boom": {"name": "boom", "listen": "0.0.0.0:1", "upstream": "x:5432"},
        }
    )
    fake.fail_delete = {"boom"}
    with pytest.raises(ToxiproxyError) as info:
        await tox.sync(cluster)
    assert info.value.status == 500
    await tox.aclose()


async def test_a_second_sync_is_a_noop(cluster):
    """The real server rewrites a 0.0.0.0 listen to [::] (proxy.go stores
    the bound address), and sync must not treat that as a stale address:
    deleting every proxy on every call would drop the replication links
    the standbys opened through them."""
    tox, fake = await client({})
    await tox.sync(cluster)
    fake.calls.clear()
    await tox.sync(cluster)
    await tox.aclose()
    mutations = [(method, path) for method, path, _ in fake.calls if method in ("DELETE", "POST")]
    assert mutations == []


async def test_sync_recreates_proxies_whose_address_changed(cluster):
    import dataclasses

    moved = dataclasses.replace(cluster, proxy_base_port=cluster.proxy_base_port + 100)
    wanted = proxies(moved)
    # the server holds the old cluster's proxies: right names, stale addresses
    tox, fake = await client({p.name: dict(p.as_toxiproxy()) for p in proxies(cluster)})
    await tox.sync(moved)
    await tox.aclose()
    expected = {p.name: p.as_toxiproxy() for p in wanted}
    assert set(fake.state) == set(expected)
    for name, body in expected.items():
        stored = fake.state[name]
        # the port is what a base-port change moves; the host spelling is
        # the server's to make (a wildcard comes back as [::])
        assert stored["listen"].rsplit(":", 1)[-1] == body["listen"].rsplit(":", 1)[-1]
        assert stored["upstream"] == body["upstream"]
    # every delete happened before the first create, so a freed port is
    # free again by the time its replacement is created
    events = [
        (method, path)
        for method, path, _ in fake.calls
        if (method == "DELETE" and path.startswith("/proxies/"))
        or (method == "POST" and path == "/proxies")
    ]
    first_create = next(i for i, (method, _) in enumerate(events) if method == "POST")
    assert all(method == "DELETE" for method, _ in events[:first_create])


async def test_error_status_is_none_when_the_request_itself_fails():
    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no server")

    tox = Toxiproxy("http://test", transport=httpx.MockTransport(unreachable))
    with pytest.raises(ToxiproxyError) as info:
        await tox.reset()
    assert info.value.status is None
    await tox.aclose()


async def test_base_url_defaults_to_settings(monkeypatch):
    monkeypatch.setenv("ROS_NETWORK", "host")
    from restaurantos.common.config import get_settings

    get_settings.cache_clear()
    tox = Toxiproxy()
    assert tox.base_url == "http://127.0.0.1:8474"
    await tox.aclose()
    get_settings.cache_clear()

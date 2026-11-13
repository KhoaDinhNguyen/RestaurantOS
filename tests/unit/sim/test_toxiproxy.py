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

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path == "/proxies":
            return httpx.Response(200, json=self.state)
        if request.method == "POST" and request.url.path == "/proxies":
            assert isinstance(body, dict)
            if body["name"] in self.state:
                return httpx.Response(409, text="proxy already exists")
            self.state[body["name"]] = body
            return httpx.Response(201, json=body)
        if request.method == "POST" and request.url.path == "/reset":
            for proxy in self.state.values():
                proxy["enabled"] = True
            return httpx.Response(200)
        if request.method == "DELETE" and request.url.path.startswith("/proxies/"):
            name = request.url.path.removeprefix("/proxies/")
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


async def test_base_url_defaults_to_settings(monkeypatch):
    monkeypatch.setenv("ROS_NETWORK", "host")
    from restaurantos.common.config import get_settings

    get_settings.cache_clear()
    tox = Toxiproxy()
    assert tox.base_url == "http://127.0.0.1:8474"
    await tox.aclose()
    get_settings.cache_clear()

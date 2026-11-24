"""The toxic half of the Toxiproxy client against a MockTransport."""

from __future__ import annotations

import json

import httpx
import pytest

from restaurantos.sim.toxiproxy import Toxiproxy, ToxiproxyError


def _client(handler) -> Toxiproxy:
    return Toxiproxy("http://toxiproxy.test", transport=httpx.MockTransport(handler))


async def test_add_toxic_posts_the_body_and_returns_the_answer() -> None:
    seen: list[tuple[str, str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append((request.method, request.url.path, body))
        return httpx.Response(200, json={**body, "name": body.get("name", "latency_upstream")})

    async with _client(handler) as client:
        answer = await client.add_toxic(
            "gateway__r1-primary", "latency", {"latency": 100, "jitter": 5}, stream="upstream"
        )
    assert seen == [
        (
            "POST",
            "/proxies/gateway__r1-primary/toxics",
            {
                "type": "latency",
                "stream": "upstream",
                "toxicity": 1.0,
                "attributes": {"latency": 100, "jitter": 5},
            },
        )
    ]
    assert answer["name"] == "latency_upstream"


async def test_add_toxic_sends_the_name_when_given() -> None:
    bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json=bodies[-1])

    async with _client(handler) as client:
        await client.add_toxic("p", "timeout", {"timeout": 0}, name="f1-silence-down")
    assert bodies[0]["name"] == "f1-silence-down"
    assert bodies[0]["stream"] == "downstream"


async def test_add_toxic_refuses_an_unknown_stream() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - never called
        raise AssertionError("no request expected")

    async with _client(handler) as client:
        with pytest.raises(ValueError, match="stream"):
            await client.add_toxic("p", "timeout", {"timeout": 0}, stream="sideways")


async def test_add_toxic_raises_the_server_error_with_its_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, text="toxic already exists")

    async with _client(handler) as client:
        with pytest.raises(ToxiproxyError) as caught:
            await client.add_toxic("p", "timeout", {"timeout": 0}, name="x")
    assert caught.value.status == 409


async def test_list_toxics_returns_the_list() -> None:
    toxics = [{"name": "a", "type": "timeout", "stream": "upstream", "attributes": {}}]

    def handler(request: httpx.Request) -> httpx.Response:
        assert (request.method, request.url.path) == ("GET", "/proxies/p/toxics")
        return httpx.Response(200, json=toxics)

    async with _client(handler) as client:
        assert await client.list_toxics("p") == toxics


async def test_list_toxics_refuses_a_non_list_answer() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"not": "a list"})

    async with _client(handler) as client:
        with pytest.raises(ToxiproxyError, match="unexpected answer"):
            await client.list_toxics("p")


async def test_remove_toxic_ignores_a_404_and_raises_the_rest() -> None:
    answers = iter([httpx.Response(404, text="toxic not found"), httpx.Response(500, text="boom")])
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(f"{request.method} {request.url.path}")
        return next(answers)

    async with _client(handler) as client:
        await client.remove_toxic("p", "gone")
        with pytest.raises(ToxiproxyError) as caught:
            await client.remove_toxic("p", "broken")
    assert caught.value.status == 500
    assert paths == ["DELETE /proxies/p/toxics/gone", "DELETE /proxies/p/toxics/broken"]

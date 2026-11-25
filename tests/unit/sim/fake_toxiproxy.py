"""A stateful stand-in for the Toxiproxy server, proxies AND toxics, over
an httpx.MockTransport. Shared by the fault-layer and control-API tests
(imported by name: the sim conftest puts this folder on sys.path)."""

from __future__ import annotations

import json
from collections.abc import Iterable

import httpx

from restaurantos.sim.toxiproxy import Toxiproxy


class FakeToxiproxyServer:
    def __init__(self, proxy_names: Iterable[str]) -> None:
        self.proxies: dict[str, dict] = {
            name: {"name": name, "enabled": True, "toxics": []} for name in proxy_names
        }
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
        if method == "POST" and path == "/reset":
            for proxy in self.proxies.values():
                proxy["enabled"] = True
                proxy["toxics"] = []
            return httpx.Response(204)
        parts = path.strip("/").split("/")
        if parts[0] != "proxies" or len(parts) < 2 or parts[1] not in self.proxies:
            return httpx.Response(404, text="proxy not found")
        proxy = self.proxies[parts[1]]
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

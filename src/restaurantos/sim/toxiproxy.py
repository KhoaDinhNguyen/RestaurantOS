"""A small async client for the Toxiproxy HTTP API: the proxy half.

Bootstrap uses it to make the running server's proxy list equal the
cluster's. WP6b adds the toxic methods to this same class. It is our own
client over httpx; do not add a third-party Toxiproxy library.
"""

from __future__ import annotations

import httpx

from restaurantos.common.cluster import Cluster, Proxy, proxies
from restaurantos.common.config import get_settings


class ToxiproxyError(RuntimeError):
    """The Toxiproxy server answered with an error, or did not answer.

    ``status`` is the HTTP status code when the server did answer, and
    None when the request itself failed (no connection, timeout).
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class Toxiproxy:
    """The proxy half of the Toxiproxy control API.

    The base URL defaults to ``Settings.toxiproxy_url`` (which follows
    ROS_NETWORK: http://127.0.0.1:8474 in host mode, http://toxiproxy:8474
    in compose mode). Names, ports and upstreams come only from
    ``common.cluster``; never type a proxy name by hand.
    """

    def __init__(
        self,
        base_url: str | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        url = base_url if base_url is not None else get_settings().toxiproxy_url
        self.base_url = url
        self._client = httpx.AsyncClient(base_url=url, transport=transport)

    async def __aenter__(self) -> Toxiproxy:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, json: object | None = None) -> object:
        try:
            response = await self._client.request(method, path, json=json)
        except httpx.HTTPError as error:
            raise ToxiproxyError(f"{method} {path}: {error}") from error
        if response.status_code >= 300:
            raise ToxiproxyError(
                f"{method} {path}: {response.status_code} {response.text.strip()}",
                status=response.status_code,
            )
        if not response.content:
            return None
        return response.json()

    async def list_proxies(self) -> list[dict[str, object]]:
        """Every proxy the server holds, ordered by name.

        The server answers with a JSON object keyed by proxy name; the
        answer is normalized to a list so callers never see the difference.
        """
        answer = await self._request("GET", "/proxies")
        if isinstance(answer, dict):
            return [answer[name] for name in sorted(answer)]
        if not isinstance(answer, list):
            raise ToxiproxyError(f"GET /proxies: unexpected answer {type(answer).__name__}")
        return answer

    async def create_proxy(self, proxy: Proxy) -> None:
        await self._request("POST", "/proxies", json=proxy.as_toxiproxy())

    async def set_enabled(self, name: str, enabled: bool) -> None:
        await self._request("POST", f"/proxies/{name}", json={"enabled": enabled})

    async def delete_proxy(self, name: str) -> None:
        """Delete one proxy. A proxy that is already gone is not an error."""
        try:
            await self._request("DELETE", f"/proxies/{name}")
        except ToxiproxyError as error:
            if error.status != 404:
                raise

    async def sync(self, cluster: Cluster) -> None:
        """Make the server's proxy list equal the cluster's.

        Deletes first — proxies this cluster does not know, and proxies
        whose name is right but whose address is stale (for example after
        a base-port change) — then creates the missing ones, so a port
        freed by a deleted proxy is free for its replacement. Proxies
        that already match are left alone, except disabled ones, which
        are re-enabled. Toxiproxy usually starts with the right list from
        its config file; this is the check that makes it true.
        """
        wanted = proxies(cluster)
        wanted_bodies = {proxy.name: proxy.as_toxiproxy() for proxy in wanted}
        existing = {str(entry.get("name")): entry for entry in await self.list_proxies()}
        stale: set[str] = set()
        for name in sorted(existing):
            if name not in wanted_bodies:
                await self.delete_proxy(name)
                continue
            stored = existing[name]
            body = wanted_bodies[name]
            if stored.get("listen") != body["listen"] or stored.get("upstream") != body["upstream"]:
                await self.delete_proxy(name)
                stale.add(name)
        for proxy in wanted:
            if proxy.name not in existing or proxy.name in stale:
                await self.create_proxy(proxy)
            elif not bool(existing[proxy.name].get("enabled", True)):
                await self.set_enabled(proxy.name, True)

    async def reset(self) -> None:
        """Re-enable every proxy and remove every toxic."""
        await self._request("POST", "/reset")

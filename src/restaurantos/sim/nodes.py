"""Real crashes (T1b): `docker compose kill` and `start` on one node.

Goes through bootstrap's `_compose`, so the project name, the generated
Compose file and the `.env` handling are exactly bootstrap's. The Compose
service name IS the node id, so no container name is ever built by hand.
Architecture Section 6.3: killing a stopped node or starting a running
one changes nothing.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
from collections.abc import Callable
from typing import Any

from restaurantos.common.cluster import Cluster

Compose = Callable[..., subprocess.CompletedProcess]


class UnknownNodeError(KeyError):
    """Not one of this cluster's database nodes (baseline is not a fault target)."""


class NodeError(RuntimeError):
    """docker compose failed; the message carries its output."""


def _bootstrap_compose(*args: str) -> subprocess.CompletedProcess:
    from restaurantos.sim.bootstrap import _compose

    return _compose(*args)


def parse_running(text: str) -> bool:
    """True if `docker compose ps --format json` lists a running container.

    Newer Compose prints one JSON object per line, older versions one
    JSON array; both are read."""
    text = text.strip()
    if not text:
        return False
    entries: list[Any]
    try:
        parsed = json.loads(text)
        entries = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        entries = [json.loads(line) for line in text.splitlines() if line.strip()]
    return any(
        isinstance(entry, dict) and str(entry.get("State", "")).lower() == "running"
        for entry in entries
    )


class Nodes:
    def __init__(self, cluster: Cluster, *, compose: Compose | None = None) -> None:
        self._nodes = tuple(cluster.nodes)
        self._compose = compose or _bootstrap_compose

    @property
    def nodes(self) -> tuple[str, ...]:
        return self._nodes

    def _check(self, node: str) -> None:
        if node not in self._nodes:
            raise UnknownNodeError(f"no node {node!r}; this cluster has {', '.join(self._nodes)}")

    async def _run(self, *args: str) -> subprocess.CompletedProcess:
        try:
            return await asyncio.to_thread(self._compose, *args)
        except Exception as error:  # noqa: BLE001 - BootstrapError, a missing docker, ...
            raise NodeError(str(error)) from error

    async def running(self, node: str) -> bool:
        self._check(node)
        result = await self._run("ps", "--all", "--format", "json", node)
        return parse_running(result.stdout)

    async def kill(self, node: str) -> dict[str, Any]:
        if await self.running(node):
            await self._run("kill", node)
        return {"node_id": node, "running": await self.running(node)}

    async def start(self, node: str) -> dict[str, Any]:
        if not await self.running(node):
            await self._run("start", node)
        return {"node_id": node, "running": await self.running(node)}

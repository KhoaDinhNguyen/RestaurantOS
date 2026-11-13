"""Cluster naming: node IDs, Toxiproxy proxy names, ports and link groups.

Everything here is derived from infra/cluster.toml, so the Compose file, the
Toxiproxy config, the services and the fault scripts all agree on names.
Standard library only.

    python -m restaurantos.common.cluster            # print the proxy list
    python -m restaurantos.common.cluster --write    # write infra/toxiproxy/proxies.json
"""

from __future__ import annotations

import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

Network = Literal["compose", "host"]
"""Where the caller runs: inside the Compose network, or on the Docker host."""

TOXIPROXY_HOST = "toxiproxy"
HOST_ADDRESS = "127.0.0.1"
POSTGRES_PORT = 5432
HOST_BASE_PORT = 5500
SERVICE_PORTS = {"gateway": 8000, "coordinator": 8001, "control": 8002}
EDGE_BASE_PORT = 8010
HQ_PRIMARY = "hq-primary"
HQ_STANDBY = "hq-standby"
BASELINE = "baseline"
"""The single-node comparison database. Outside the system: no proxy, no standby."""


@dataclass(frozen=True)
class Cluster:
    regions: int = 1
    standbys: bool = True
    synchronous: bool = False
    proxy_base_port: int = 21000

    def __post_init__(self) -> None:
        if self.regions < 1:
            raise ValueError("a cluster needs at least one region")

    @property
    def region_ids(self) -> list[str]:
        return [f"r{n}" for n in range(1, self.regions + 1)]

    @property
    def nodes(self) -> list[str]:
        """Every PostgreSQL node ID, HQ first."""
        out = [HQ_PRIMARY]
        if self.standbys:
            out.append(HQ_STANDBY)
        for rid in self.region_ids:
            out.append(primary_of(rid))
            if self.standbys:
                out.append(standby_of(rid))
        return out


def load_cluster(path: Path | str = "infra/cluster.toml") -> Cluster:
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)
    section = raw.get("cluster", {})
    return Cluster(
        regions=section.get("regions", 1),
        standbys=section.get("standbys", True),
        synchronous=section.get("synchronous", False),
        proxy_base_port=raw.get("toxiproxy", {}).get("base_port", 21000),
    )


def primary_of(region_id: str) -> str:
    return f"{region_id}-primary"


def standby_of(region_id: str) -> str:
    return f"{region_id}-standby"


def slot_name(standby_node: str) -> str:
    """Replication slot name for a standby (slots cannot contain hyphens)."""
    return standby_node.replace("-", "_")


def proxy_name(client: str, target: str) -> str:
    return f"{client}__{target}"


@dataclass(frozen=True)
class Proxy:
    name: str
    client: str
    target: str
    port: int

    def as_toxiproxy(self) -> dict[str, object]:
        return {
            "name": self.name,
            "listen": f"0.0.0.0:{self.port}",
            "upstream": f"{self.target}:{POSTGRES_PORT}",
            "enabled": True,
        }


def _pairs(cluster: Cluster) -> list[tuple[str, str]]:
    """(client, target) for every proxied link. HQ first, then region by region,
    so adding a region never changes the ports of existing proxies.

    With standbys on, the two nodes of a pair are treated alike: every client
    that can reach the primary can also reach the standby, and replication is
    proxied in both directions. After a promotion the standby is the primary,
    so the gateway, the HQ-side workers and the relays must still have a
    proxy to it, and the old primary must be able to rejoin as a standby.
    """
    pairs = [("gateway", HQ_PRIMARY), ("coordinator", HQ_PRIMARY)]
    if cluster.standbys:
        pairs += [("coordinator", HQ_STANDBY), (HQ_STANDBY, HQ_PRIMARY)]
        pairs += [("gateway", HQ_STANDBY), (HQ_PRIMARY, HQ_STANDBY)]
    for rid in cluster.region_ids:
        prim, stby = primary_of(rid), standby_of(rid)
        pairs += [("gateway", prim), ("coordinator", prim), ("hq", prim)]
        pairs += [(f"relay-{rid}", HQ_PRIMARY)]
        if cluster.standbys:
            pairs += [("gateway", stby), ("coordinator", stby), (stby, prim)]
            pairs += [("hq", stby), (prim, stby), (f"relay-{rid}", HQ_STANDBY)]
    return pairs


# Each region gets a fixed block of ports so that turning standbys on or off,
# or adding regions, never moves an existing proxy. New slots go at the end of
# a list, never in the middle. The region block is full (10 of 10).
_HQ_SLOTS = [
    ("gateway", HQ_PRIMARY),
    ("coordinator", HQ_PRIMARY),
    ("coordinator", HQ_STANDBY),
    (HQ_STANDBY, HQ_PRIMARY),
    ("gateway", HQ_STANDBY),
    (HQ_PRIMARY, HQ_STANDBY),
]
_BLOCK = 10


def proxies(cluster: Cluster) -> list[Proxy]:
    wanted = set(_pairs(cluster))
    out: list[Proxy] = []
    for i, pair in enumerate(_HQ_SLOTS):
        if pair in wanted:
            out.append(Proxy(proxy_name(*pair), *pair, cluster.proxy_base_port + i))
    for n, rid in enumerate(cluster.region_ids, start=1):
        prim, stby = primary_of(rid), standby_of(rid)
        slots = [
            ("gateway", prim),
            ("coordinator", prim),
            ("hq", prim),
            (f"relay-{rid}", HQ_PRIMARY),
            ("gateway", stby),
            ("coordinator", stby),
            (stby, prim),
            ("hq", stby),
            (prim, stby),
            (f"relay-{rid}", HQ_STANDBY),
        ]
        base = cluster.proxy_base_port + n * _BLOCK
        for i, pair in enumerate(slots):
            if pair in wanted:
                out.append(Proxy(proxy_name(*pair), *pair, base + i))
    return out


def proxy_for(cluster: Cluster, client: str, target: str) -> Proxy:
    name = proxy_name(client, target)
    for proxy in proxies(cluster):
        if proxy.name == name:
            return proxy
    raise KeyError(f"no proxy {name!r} in this cluster")


def link_groups(cluster: Cluster) -> dict[str, list[str]]:
    """Named sets of proxies that fault scenarios act on.

    node:<node>     every proxy whose target is that node (isolate one node)
    link:<rid><->hq every link between a region and HQ (partition a region)
    repl:<rid|hq>   replication to the standby, plus the coordinator's view of it
                    (named for the roles at bootstrap: <rid>-standby follows
                    <rid>-primary)
    """
    all_proxies = proxies(cluster)
    groups: dict[str, list[str]] = {}
    for node in cluster.nodes:
        groups[f"node:{node}"] = [p.name for p in all_proxies if p.target == node]
    for rid in cluster.region_ids:
        own = {primary_of(rid), standby_of(rid)}
        groups[f"link:{rid}<->hq"] = [
            p.name
            for p in all_proxies
            if (p.target in own and p.client in {"gateway", "coordinator", "hq"})
            or p.client == f"relay-{rid}"
        ]
    if cluster.standbys:
        for scope, prim, stby in [("hq", HQ_PRIMARY, HQ_STANDBY)] + [
            (rid, primary_of(rid), standby_of(rid)) for rid in cluster.region_ids
        ]:
            groups[f"repl:{scope}"] = [
                proxy_name(stby, prim),
                proxy_name("coordinator", stby),
            ]
    return groups


def conninfo(
    cluster: Cluster,
    client: str,
    node: str,
    *,
    dbname: str = "restaurantos",
    user: str = "ros",
    password: str = "ros",
    direct: bool = False,
    network: Network = "compose",
) -> str:
    """libpq connection string for `client` to reach `node`.

    Through the client's Toxiproxy proxy by default. direct=True goes straight
    to the node: only for a service talking to its own region's nodes (edge
    API, relay), which must keep working when the region is cut off, and for
    `baseline`, which has no proxy.

    network says where the caller runs. "compose": inside the Compose network,
    where containers are reached by name. "host": on the Docker host, where the
    same proxies and nodes are reached on 127.0.0.1 through published ports.
    The route is the same in both; only the address differs.
    """
    if node != BASELINE and node not in cluster.nodes:
        raise KeyError(f"no node {node!r} in this cluster")
    if not direct:
        port = proxy_for(cluster, client, node).port
        host = TOXIPROXY_HOST if network == "compose" else HOST_ADDRESS
    elif network == "compose":
        host, port = node, POSTGRES_PORT
    else:
        host, port = HOST_ADDRESS, host_port(cluster, node)
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def host_port(cluster: Cluster, node: str) -> int:
    """Port on the Docker host that is mapped to `node`'s PostgreSQL port.

    hq-primary 5500, hq-standby 5501, baseline 5509; region n: 5500 + 10n for
    the primary and 5501 + 10n for the standby. The Compose file publishes
    exactly these, so tools on the host never type a port by hand.
    """
    if node == BASELINE:
        return HOST_BASE_PORT + _BLOCK - 1
    if node not in cluster.nodes:
        raise KeyError(f"no node {node!r} in this cluster")
    scope, _, role = node.rpartition("-")
    block = 0 if scope == "hq" else cluster.region_ids.index(scope) + 1
    return HOST_BASE_PORT + block * _BLOCK + (1 if role == "standby" else 0)


def host_conninfo(
    cluster: Cluster,
    node: str,
    *,
    dbname: str = "restaurantos",
    user: str = "ros",
    password: str = "ros",
) -> str:
    """libpq connection string from the Docker host straight to `node`.

    Bypasses Toxiproxy, so it is only for batch tools that run on the host and
    are outside the system under test: bootstrap loads, the invariant checker,
    snapshots, tests. Never for a service on a request path.
    """
    return conninfo(
        cluster,
        "",
        node,
        dbname=dbname,
        user=user,
        password=password,
        direct=True,
        network="host",
    )


def service_port(cluster: Cluster, service: str) -> int:
    """HTTP port of an application service: gateway, coordinator, control, edge-r<n>.

    The same in both networks. Each edge API gets its own port (8010 + n) so
    that several can run side by side on one host.
    """
    if service in SERVICE_PORTS:
        return SERVICE_PORTS[service]
    scope, _, rid = service.partition("-")
    if scope == "edge" and rid in cluster.region_ids:
        return EDGE_BASE_PORT + cluster.region_ids.index(rid) + 1
    raise KeyError(f"no service {service!r} in this cluster")


TOXIPROXY_API_PORT = 8474


def published_ports(cluster: Cluster, *, baseline: bool = False) -> list[int]:
    """Every port the Compose file must publish on 127.0.0.1, in order.

    The Toxiproxy API, each proxy (so services can run on the host) and each
    PostgreSQL node (for batch tools), plus `baseline` when asked. The Compose
    generator and the test for the committed Compose file both read this list.
    """
    ports = [TOXIPROXY_API_PORT, *(p.port for p in proxies(cluster))]
    ports += [host_port(cluster, node) for node in cluster.nodes]
    if baseline:
        ports.append(host_port(cluster, BASELINE))
    return sorted(ports)


def toxiproxy_config(cluster: Cluster) -> str:
    return json.dumps([p.as_toxiproxy() for p in proxies(cluster)], indent=2) + "\n"


def main(argv: list[str]) -> int:
    cluster = load_cluster()
    text = toxiproxy_config(cluster)
    if "--write" in argv:
        Path("infra/toxiproxy/proxies.json").write_text(text, encoding="utf-8")
        print(f"wrote infra/toxiproxy/proxies.json ({len(proxies(cluster))} proxies)")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

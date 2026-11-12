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

TOXIPROXY_HOST = "toxiproxy"
POSTGRES_PORT = 5432
HQ_PRIMARY = "hq-primary"
HQ_STANDBY = "hq-standby"


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
    so adding a region never changes the ports of existing proxies."""
    pairs = [("gateway", HQ_PRIMARY), ("coordinator", HQ_PRIMARY)]
    if cluster.standbys:
        pairs += [("coordinator", HQ_STANDBY), (HQ_STANDBY, HQ_PRIMARY)]
    for rid in cluster.region_ids:
        prim, stby = primary_of(rid), standby_of(rid)
        pairs += [("gateway", prim), ("coordinator", prim), ("hq", prim)]
        pairs += [(f"relay-{rid}", HQ_PRIMARY)]
        if cluster.standbys:
            pairs += [("gateway", stby), ("coordinator", stby), (stby, prim)]
    return pairs


# Each region gets a fixed block of ports so that turning standbys on or off,
# or adding regions, never moves an existing proxy.
_HQ_SLOTS = [
    ("gateway", HQ_PRIMARY),
    ("coordinator", HQ_PRIMARY),
    ("coordinator", HQ_STANDBY),
    (HQ_STANDBY, HQ_PRIMARY),
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
) -> str:
    """libpq connection string for `client` to reach `node`.

    Through the client's Toxiproxy proxy by default. direct=True goes straight
    to the container: only for a service talking to its own region's primary
    (edge API, relay), which must keep working when the region is cut off.
    """
    if direct:
        host, port = node, POSTGRES_PORT
    else:
        host, port = TOXIPROXY_HOST, proxy_for(cluster, client, node).port
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


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

import json
from pathlib import Path

import pytest

from restaurantos.common.cluster import (
    BASELINE,
    Cluster,
    conninfo,
    host_conninfo,
    host_port,
    link_groups,
    load_cluster,
    proxies,
    proxy_for,
    published_ports,
    service_port,
    slot_name,
    toxiproxy_config,
)

REPO = Path(__file__).resolve().parents[3]


def names(cluster):
    return {p.name for p in proxies(cluster)}


def test_one_region_proxy_names():
    assert names(Cluster(regions=1)) == {
        "gateway__hq-primary",
        "coordinator__hq-primary",
        "coordinator__hq-standby",
        "hq-standby__hq-primary",
        "gateway__hq-standby",
        "hq-primary__hq-standby",
        "gateway__r1-primary",
        "gateway__r1-standby",
        "coordinator__r1-primary",
        "coordinator__r1-standby",
        "r1-standby__r1-primary",
        "r1-primary__r1-standby",
        "relay-r1__hq-primary",
        "relay-r1__hq-standby",
        "hq__r1-primary",
        "hq__r1-standby",
    }


def test_a_promoted_standby_is_reachable_by_everyone_who_reached_the_primary():
    cluster = Cluster(regions=2)
    have = names(cluster)
    for proxy in proxies(cluster):
        if proxy.target.endswith("-primary"):
            twin = proxy.target.removesuffix("-primary") + "-standby"
            if proxy.client == twin:
                # replication: the old primary must be able to follow the new one
                assert f"{proxy.target}__{twin}" in have
            else:
                assert f"{proxy.client}__{twin}" in have, proxy.name


def test_ports_are_unique_and_stable_when_the_cluster_grows():
    small = {p.name: p.port for p in proxies(Cluster(regions=1))}
    big = {p.name: p.port for p in proxies(Cluster(regions=3))}
    assert len(set(big.values())) == len(big)
    assert all(big[name] == port for name, port in small.items())


def test_ports_do_not_move_when_standbys_are_turned_off():
    with_standby = {p.name: p.port for p in proxies(Cluster(regions=2))}
    without = {p.name: p.port for p in proxies(Cluster(regions=2, standbys=False))}
    assert not any("standby" in name for name in without)
    assert all(with_standby[name] == port for name, port in without.items())


def test_isolating_a_node_covers_every_path_to_it():
    groups = link_groups(Cluster(regions=2))
    assert set(groups["node:r1-primary"]) == {
        "gateway__r1-primary",
        "coordinator__r1-primary",
        "hq__r1-primary",
        "r1-standby__r1-primary",
    }


def test_region_partition_group_cuts_both_directions_but_not_local_replication():
    group = set(link_groups(Cluster(regions=2))["link:r1<->hq"])
    assert group == {
        "gateway__r1-primary",
        "gateway__r1-standby",
        "coordinator__r1-primary",
        "coordinator__r1-standby",
        "hq__r1-primary",
        "hq__r1-standby",
        "relay-r1__hq-primary",
        "relay-r1__hq-standby",
    }
    assert "r1-standby__r1-primary" not in group
    assert "r1-primary__r1-standby" not in group
    assert not any("r2" in name for name in group)


def test_every_group_member_is_a_real_proxy():
    cluster = Cluster(regions=3)
    known = names(cluster)
    for group, members in link_groups(cluster).items():
        assert members, group
        assert set(members) <= known, group


def test_conninfo_goes_through_toxiproxy_unless_direct():
    cluster = Cluster(regions=1)
    port = proxy_for(cluster, "gateway", "r1-primary").port
    assert f"host=toxiproxy port={port} " in conninfo(cluster, "gateway", "r1-primary")
    assert "host=r1-primary port=5432 " in conninfo(cluster, "edge-r1", "r1-primary", direct=True)
    with pytest.raises(KeyError):
        conninfo(cluster, "edge-r1", "r1-primary")


def test_host_ports_follow_the_same_blocks_as_the_proxies():
    cluster = Cluster(regions=3)
    assert host_port(cluster, "hq-primary") == 5500
    assert host_port(cluster, "hq-standby") == 5501
    assert host_port(cluster, "r1-primary") == 5510
    assert host_port(cluster, "r3-standby") == 5531
    assert host_port(cluster, BASELINE) == 5509
    ports = [host_port(cluster, node) for node in [*cluster.nodes, BASELINE]]
    assert len(set(ports)) == len(ports)
    with pytest.raises(KeyError):
        host_port(cluster, "r4-primary")
    with pytest.raises(KeyError):
        host_port(Cluster(regions=1, standbys=False), "r1-standby")


def test_host_conninfo_bypasses_toxiproxy():
    assert "host=127.0.0.1 port=5510 " in host_conninfo(Cluster(regions=1), "r1-primary")


def test_conninfo_takes_the_same_route_in_both_networks():
    cluster = Cluster(regions=2)
    port = proxy_for(cluster, "gateway", "r2-primary").port
    inside = conninfo(cluster, "gateway", "r2-primary")
    outside = conninfo(cluster, "gateway", "r2-primary", network="host")
    assert inside.startswith(f"host=toxiproxy port={port} ")
    assert outside.startswith(f"host=127.0.0.1 port={port} ")

    inside = conninfo(cluster, "relay-r2", "r2-standby", direct=True)
    outside = conninfo(cluster, "relay-r2", "r2-standby", direct=True, network="host")
    assert inside.startswith("host=r2-standby port=5432 ")
    assert outside.startswith("host=127.0.0.1 port=5521 ")


def test_baseline_is_reached_directly_or_not_at_all():
    cluster = Cluster(regions=1)
    inside = conninfo(cluster, "gateway", BASELINE, direct=True)
    outside = conninfo(cluster, "gateway", BASELINE, direct=True, network="host")
    assert inside.startswith("host=baseline port=5432 ")
    assert outside.startswith("host=127.0.0.1 port=5509 ")
    with pytest.raises(KeyError):
        conninfo(cluster, "gateway", BASELINE)
    with pytest.raises(KeyError):
        conninfo(cluster, "gateway", "r9-primary", direct=True)


def test_service_ports():
    cluster = Cluster(regions=3)
    assert service_port(cluster, "gateway") == 8000
    assert service_port(cluster, "coordinator") == 8001
    assert service_port(cluster, "control") == 8002
    assert [service_port(cluster, f"edge-r{n}") for n in (1, 2, 3)] == [8011, 8012, 8013]
    with pytest.raises(KeyError):
        service_port(cluster, "edge-r4")
    with pytest.raises(KeyError):
        service_port(cluster, "relay-r1")


def test_published_ports_cover_the_api_every_proxy_and_every_node():
    cluster = Cluster(regions=2)
    ports = published_ports(cluster, baseline=True)
    assert len(set(ports)) == len(ports)
    assert 8474 in ports
    assert {p.port for p in proxies(cluster)} <= set(ports)
    assert {host_port(cluster, node) for node in [*cluster.nodes, BASELINE]} <= set(ports)
    assert host_port(cluster, BASELINE) not in published_ports(cluster)


def test_slot_name_has_no_hyphen():
    assert slot_name("r1-standby") == "r1_standby"


def test_committed_files_agree_with_cluster_toml():
    cluster = load_cluster(REPO / "infra" / "cluster.toml")
    committed = (REPO / "infra" / "toxiproxy" / "proxies.json").read_text(encoding="utf-8")
    assert json.loads(committed) == json.loads(toxiproxy_config(cluster))
    compose = (REPO / "infra" / "compose.yaml").read_text(encoding="utf-8")
    for standby, primary in [("hq-standby", "hq-primary"), ("r1-standby", "r1-primary")]:
        port = proxy_for(cluster, standby, primary).port
        assert f'ROS_PRIMARY_PORT: "{port}"' in compose
    # Published on 127.0.0.1 only, and nothing is missing: host mode needs
    # every proxy, batch tools need every node.
    node_ports = {host_port(cluster, node) for node in cluster.nodes}
    for port in published_ports(cluster):
        inside = 5432 if port in node_ports else port
        assert f'- "127.0.0.1:{port}:{inside}"' in compose, port
    assert compose.count('- "127.0.0.1:') == len(published_ports(cluster))

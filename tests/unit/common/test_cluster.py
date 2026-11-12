import json
from pathlib import Path

import pytest

from restaurantos.common.cluster import (
    Cluster,
    conninfo,
    link_groups,
    load_cluster,
    proxies,
    proxy_for,
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
        "gateway__r1-primary",
        "gateway__r1-standby",
        "coordinator__r1-primary",
        "coordinator__r1-standby",
        "r1-standby__r1-primary",
        "relay-r1__hq-primary",
        "hq__r1-primary",
    }


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
        "relay-r1__hq-primary",
    }
    assert "r1-standby__r1-primary" not in group
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

"""The service table, pure: names, commands, identity environment and
ports follow the "How each service starts" table in architecture
Section 4, for 1 and 3 regions. No process is spawned here."""

from __future__ import annotations

import pytest

from restaurantos.common.cluster import Cluster, service_port
from restaurantos.sim.services import dialed_ports, service_specs


def test_one_region_starts_five_services() -> None:
    specs = service_specs(Cluster(regions=1))
    assert [spec.name for spec in specs] == [
        "gateway",
        "coordinator",
        "hq-worker",
        "relay-r1",
        "edge-r1",
    ]


@pytest.mark.parametrize(
    ("name", "args", "env", "port"),
    [
        (
            "gateway",
            ("-m", "restaurantos.gateway"),
            {"ROS_MODE": "gateway", "ROS_CLIENT": "gateway"},
            8000,
        ),
        ("coordinator", ("-m", "restaurantos.coordinator"), {"ROS_CLIENT": "coordinator"}, 8001),
        (
            "hq-worker",
            ("-m", "restaurantos.relay"),
            {"ROS_RELAY_ROLE": "hq", "ROS_CLIENT": "hq"},
            None,
        ),
        (
            "relay-r1",
            ("-m", "restaurantos.relay"),
            {"ROS_RELAY_ROLE": "region", "ROS_CLIENT": "relay-r1", "ROS_REGION_ID": "r1"},
            None,
        ),
        (
            "edge-r1",
            ("-m", "restaurantos.gateway"),
            {"ROS_MODE": "edge", "ROS_REGION_ID": "r1"},
            8011,
        ),
    ],
)
def test_service_rows_match_the_architecture_table(
    name: str, args: tuple[str, ...], env: dict[str, str], port: int | None
) -> None:
    by_name = {spec.name: spec for spec in service_specs(Cluster(regions=1))}
    spec = by_name[name]
    assert spec.args == args
    assert dict(spec.env) == env
    assert spec.port == port


def test_three_regions_add_one_relay_and_one_edge_each() -> None:
    specs = service_specs(Cluster(regions=3))
    assert [spec.name for spec in specs] == [
        "gateway",
        "coordinator",
        "hq-worker",
        "relay-r1",
        "edge-r1",
        "relay-r2",
        "edge-r2",
        "relay-r3",
        "edge-r3",
    ]
    by_name = {spec.name: spec for spec in specs}
    assert by_name["relay-r2"].env == {
        "ROS_RELAY_ROLE": "region",
        "ROS_CLIENT": "relay-r2",
        "ROS_REGION_ID": "r2",
    }
    assert by_name["edge-r3"].env == {"ROS_MODE": "edge", "ROS_REGION_ID": "r3"}


def test_edge_ports_come_from_service_port_and_stay_distinct() -> None:
    cluster = Cluster(regions=3)
    specs = service_specs(cluster)
    ports = [spec.port for spec in specs if spec.port is not None]
    assert len(ports) == len(set(ports))
    for spec in specs:
        if spec.name.startswith("edge-"):
            assert spec.port == service_port(cluster, spec.name)


def test_a_cluster_without_standbys_has_the_same_services() -> None:
    # standbys change the proxy list, never the service list
    assert [s.name for s in service_specs(Cluster(regions=2, standbys=False))] == [
        "gateway",
        "coordinator",
        "hq-worker",
        "relay-r1",
        "edge-r1",
        "relay-r2",
        "edge-r2",
    ]


def test_dialed_ports_are_the_childrens_own_links() -> None:
    cluster = Cluster(regions=1)
    ports = dialed_ports(cluster)
    # every proxy the request path uses, and every node's published port
    # for the direct region links - but not the Toxiproxy API and not
    # baseline, which no service dials
    assert 21000 in ports and 21005 in ports
    assert 5500 in ports and 5511 in ports
    assert 8474 not in ports
    assert 5509 not in ports
    assert len(ports) == len(set(ports))

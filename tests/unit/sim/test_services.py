"""The service table, pure: names, commands, identity environment,
ports, readiness routes and dials follow the "How each service starts"
table in architecture Section 4 and the services' own connection calls,
for 1 and 3 regions. No process is spawned here."""

from __future__ import annotations

from pathlib import Path

import pytest

from restaurantos.common.cluster import Cluster, service_port
from restaurantos.sim.services import (
    _stop_key,
    dialed_ports,
    dials,
    port_labels,
    running_names,
    service_specs,
    spec_for_name,
)


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
    ("name", "args", "env", "port", "ready"),
    [
        (
            "gateway",
            ("-m", "restaurantos.gateway"),
            {"ROS_MODE": "gateway", "ROS_CLIENT": "gateway"},
            8000,
            "/health",
        ),
        (
            "coordinator",
            ("-m", "restaurantos.coordinator"),
            {"ROS_CLIENT": "coordinator"},
            8001,
            "/admin/cluster",
        ),
        (
            "hq-worker",
            ("-m", "restaurantos.relay"),
            {"ROS_RELAY_ROLE": "hq", "ROS_CLIENT": "hq"},
            None,
            None,
        ),
        (
            "relay-r1",
            ("-m", "restaurantos.relay"),
            {"ROS_RELAY_ROLE": "region", "ROS_CLIENT": "relay-r1", "ROS_REGION_ID": "r1"},
            None,
            None,
        ),
        (
            "edge-r1",
            ("-m", "restaurantos.gateway"),
            {"ROS_MODE": "edge", "ROS_REGION_ID": "r1"},
            8011,
            "/health",
        ),
    ],
)
def test_service_rows_match_the_architecture_table(
    name: str, args: tuple[str, ...], env: dict[str, str], port: int | None, ready: str | None
) -> None:
    by_name = {spec.name: spec for spec in service_specs(Cluster(regions=1))}
    spec = by_name[name]
    assert spec.args == args
    assert dict(spec.env) == env
    assert spec.port == port
    assert spec.ready == ready


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


# --- spec_for_name: the one table ----------------------------------------


def test_every_table_row_is_its_name_built_by_spec_for_name() -> None:
    cluster = Cluster(regions=1)
    for spec in service_specs(cluster):
        assert spec == spec_for_name(spec.name, cluster)


def test_a_spec_can_be_built_from_a_pid_file_alone() -> None:
    # down reads pid files, not a cluster: the region id is in the name
    spec = spec_for_name("relay-r9")
    assert spec.env == {"ROS_RELAY_ROLE": "region", "ROS_CLIENT": "relay-r9", "ROS_REGION_ID": "r9"}
    assert spec.port is None  # no cluster given, and down needs no port
    assert spec_for_name("edge-r9").port is None
    assert spec_for_name("gateway").port == 8000  # the fixed ports need no cluster


def test_a_name_matching_no_pattern_is_an_error() -> None:
    for bad in (
        "",
        "web",
        "relay-",
        "edge-",
        "coordinator-r1",
        # region ids are canonical: no r0, no leading zero, no suffix
        "relay-r0",
        "relay-r01",
        "edge-r1x",
        "relay-not-a-region",
    ):
        with pytest.raises(KeyError):
            spec_for_name(bad)


def test_a_patterned_name_builds_even_outside_the_cluster() -> None:
    # deliberate: down reads pid files, so relay-r9 of a 3-region run
    # must still be stoppable while reading a 1-region cluster file -
    # the region id is taken from the name, not validated against the
    # cluster (only against the canonical id pattern)
    assert spec_for_name("relay-r9").env["ROS_REGION_ID"] == "r9"
    assert spec_for_name("edge-r12", Cluster(regions=1)).port is None


# --- down order ----------------------------------------------------------


def test_stop_order_puts_the_coordinator_first_and_the_region_side_last() -> None:
    names = ["gateway", "hq-worker", "edge-r1", "coordinator", "relay-r1"]
    assert sorted(names, key=_stop_key) == [
        "coordinator",
        "gateway",
        "hq-worker",
        "relay-r1",
        "edge-r1",
    ]


def test_running_names_lists_pid_files_in_down_order(tmp_path: Path) -> None:
    services_dir = tmp_path / "runs" / "services"
    services_dir.mkdir(parents=True)
    for name in ("edge-r1", "gateway", "coordinator", "relay-r2", "relay-r1", "edge-r2"):
        (services_dir / f"{name}.pid").write_text("1 1\n", encoding="utf-8")
    assert running_names(tmp_path / "runs") == [
        "coordinator",
        "gateway",
        "relay-r1",
        "relay-r2",
        "edge-r1",
        "edge-r2",
    ]


# --- reachability --------------------------------------------------------


def test_dials_are_each_services_own_links() -> None:
    # derived from the services' connection calls, not from the proxy
    # list: the gateway and the coordinator go through their own
    # proxies, the hq worker additionally dials HQ directly, the
    # region relay goes through its HQ proxies and dials its own
    # region's nodes directly, the edge dials its region directly
    cluster = Cluster(regions=1)
    by_name = {spec.name: spec for spec in service_specs(cluster)}
    assert dials(by_name["gateway"], cluster) == [21000, 21004, 21010, 21014]
    assert dials(by_name["coordinator"], cluster) == [21001, 21002, 21011, 21015]
    assert dials(by_name["hq-worker"], cluster) == [21012, 21017, 5500]
    assert dials(by_name["relay-r1"], cluster) == [21013, 21019, 5510, 5511]
    assert dials(by_name["edge-r1"], cluster) == [5510, 5511]


def test_dialed_ports_are_the_childrens_own_links() -> None:
    cluster = Cluster(regions=1)
    ports = dialed_ports(cluster)
    # every proxy a service dials, and every node's published port for
    # the direct links - but not the replication proxies (a standby to
    # its primary is postgres, not a service), not the Toxiproxy API
    # and not baseline, which no service dials
    assert 21000 in ports and 21010 in ports
    assert 21005 not in ports  # hq-primary__hq-standby: replication
    assert 21003 not in ports  # hq-standby__hq-primary: replication
    assert 21016 not in ports  # r1-standby__r1-primary: replication
    assert 5500 in ports and 5511 in ports
    assert 8474 not in ports
    assert 5509 not in ports
    assert len(ports) == len(set(ports))


def test_port_labels_name_what_each_dial_is() -> None:
    labels = port_labels(Cluster(regions=1))
    assert labels[21010] == "gateway__r1-primary"
    assert labels[21013] == "relay-r1__hq-primary"
    assert labels[5500] == "hq-primary"
    assert labels[5511] == "r1-standby"

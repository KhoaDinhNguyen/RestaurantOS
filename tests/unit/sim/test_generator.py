"""The cluster generator: what it writes, and its agreement with the
committed dev baseline, which WP6a's brief makes a rule: for regions = 1
the database services must match infra/compose.yaml in image, flags,
volumes and ports.

Checked as text, like tests/unit/common/test_cluster.py checks the
committed file: no YAML dependency, and a drifted line fails by name.
"""

import re
from pathlib import Path

import pytest

from restaurantos.common.cluster import (
    BASELINE,
    Cluster,
    host_port,
    primary_of,
    proxies,
    published_ports,
    standby_of,
    toxiproxy_config,
)
from restaurantos.sim.generator import IMAGE_DEFAULT, compose_text, generate

REPO = Path(__file__).resolve().parents[3]
COMMITTED = (REPO / "infra" / "compose.yaml").read_text(encoding="utf-8")


def service_block(text: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n((?:    .*\n|\n)*?)"
        rf"(?=^  [a-z]|\Z|^volumes:)",
        text,
        re.MULTILINE,
    )
    assert match, f"service {name} not found"
    return match.group(0)


def port_lines(block: str) -> list[str]:
    return [
        line.strip()
        for line in block.splitlines()
        if ':5432"' in line or re.match(r"^ {6}- \"127\.0\.0\.1:\d+:\d+\"", line)
    ]


def test_regions_one_publishes_the_same_ports_as_the_committed_baseline():
    generated = compose_text(Cluster(regions=1))
    for port in published_ports(Cluster(regions=1), baseline=True):
        assert f"127.0.0.1:{port}:" in generated, port
    # the node port mappings match the committed file exactly
    for node in ("hq-primary", "hq-standby", "r1-primary", "r1-standby"):
        assert port_lines(service_block(generated, node)) == port_lines(
            service_block(COMMITTED, node)
        ), node


def test_regions_one_matches_the_committed_flags_and_image():
    generated = compose_text(Cluster(regions=1))
    for anchor in (
        "x-postgres-image: &pg-image ${ROS_POSTGRES_IMAGE:-" + IMAGE_DEFAULT,
        "x-pg-command: &pg-command",
    ):
        assert anchor in generated and anchor in COMMITTED
    # the same postgres flags, in the same order
    flags = re.search(r"x-pg-command: &pg-command\n((?:  - .*\n)+)", COMMITTED)
    assert flags and flags.group(1) in generated
    for health in (
        'test: ["CMD-SHELL", "pg_isready -U ros -d restaurantos"]',
        "interval: 3s",
        "retries: 40",
    ):
        assert health in generated


def test_regions_one_wires_the_standby_replication_the_same_way():
    generated = compose_text(Cluster(regions=1))
    for node, primary in (("hq-standby", "hq-primary"), ("r1-standby", "r1-primary")):
        new = service_block(generated, node)
        old = service_block(COMMITTED, node)
        for line in (
            f'ROS_PRIMARY_PORT: "{_proxy_port(node, primary)}"',
            f"ROS_SLOT_NAME: {node.removesuffix('-standby')}_standby",
            f"      {primary}:",
            "        condition: service_healthy",
        ):
            assert line in new, (node, line)
        assert port_lines(new) == port_lines(old), node


def _proxy_port(client: str, target: str) -> int:
    for proxy in proxies(Cluster(regions=1)):
        if proxy.name == f"{client}__{target}":
            return proxy.port
    raise KeyError(client)


def test_every_cluster_node_has_a_service_and_a_volume():
    cluster = Cluster(regions=3)
    generated = compose_text(cluster)
    for node in (*cluster.nodes, BASELINE):
        assert f"  {node}:" in generated
        assert f"  {node}-data:" in generated


def test_baseline_is_a_region_kind_node_without_a_standby():
    generated = compose_text(Cluster(regions=1))
    block = service_block(generated, BASELINE)
    assert "ROS_NODE_KIND: region" in block
    assert f"ROS_REGION_ID: {BASELINE}" in block
    assert f"127.0.0.1:{host_port(Cluster(regions=1), BASELINE)}:5432" in block
    assert "standby" not in block


def test_region_services_carry_their_region_id_and_port_block():
    cluster = Cluster(regions=3)
    generated = compose_text(cluster)
    for rid in cluster.region_ids:
        primary = service_block(generated, primary_of(rid))
        assert f"ROS_REGION_ID: {rid}" in primary
        assert f"127.0.0.1:{host_port(cluster, primary_of(rid))}:5432" in primary
        if cluster.standbys:
            standby = service_block(generated, standby_of(rid))
            assert f"ROS_SLOT_NAME: {rid}_standby" in standby
            assert f"127.0.0.1:{host_port(cluster, standby_of(rid))}:5432" in standby


def test_toxiproxy_publishes_the_api_and_every_proxy_port():
    for regions in (1, 3):
        cluster = Cluster(regions=regions)
        block = service_block(compose_text(cluster), "toxiproxy")
        for port in sorted({8474, *(p.port for p in proxies(cluster))}):
            assert f'- "127.0.0.1:{port}:{port}"' in block, (regions, port)


def test_synchronous_replication_is_refused_not_faked():
    # nothing sets it today and the regions=1 output must match the
    # committed (asynchronous) baseline; refuse loudly instead of writing
    # a file that does not do what the cluster description says
    with pytest.raises(NotImplementedError, match="synchronous"):
        compose_text(Cluster(regions=1, synchronous=True))


def test_generate_writes_both_files(tmp_path):
    cluster = Cluster(regions=2)
    compose, proxy_list = generate(cluster, tmp_path)
    assert compose.read_text(encoding="utf-8") == compose_text(cluster)
    assert proxy_list.read_text(encoding="utf-8") == toxiproxy_config(cluster)
    assert proxy_list.name == "proxies.json"
    # the Compose file mounts the generated proxy list, not the committed one
    assert "./proxies.json:/config/proxies.json:ro" in compose.read_text(encoding="utf-8")
    assert "../toxiproxy/proxies.json" not in compose.read_text(encoding="utf-8")

"""The cluster generator: what it writes, and its agreement with the
committed dev baseline, which WP6a's brief makes a rule: for regions = 1
the database services must match infra/compose.yaml in image, flags,
volumes and ports. The service entries for container mode are checked
against the one service table (sim.services) the same way: every name,
command, identity variable and port comes from it, never typed here.

Checked as text, like tests/unit/common/test_cluster.py checks the
committed file: no YAML dependency for the content checks (a drifted
line fails by name), with one parse test that proves the whole file is
still valid YAML.
"""

import re
from pathlib import Path

import pytest
import yaml

from restaurantos.common.cluster import (
    BASELINE,
    Cluster,
    host_port,
    primary_of,
    proxies,
    published_ports,
    service_port,
    standby_of,
    toxiproxy_config,
)
from restaurantos.sim.generator import IMAGE_DEFAULT, compose_text, generate
from restaurantos.sim.services import service_specs

REPO = Path(__file__).resolve().parents[3]
COMMITTED = (REPO / "infra" / "compose.yaml").read_text(encoding="utf-8")

# pyyaml is not a project dependency; it rides uv.lock via pre-commit,
# which `uv run` resolves exactly, so the parse test can rely on it.


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


def test_synchronous_replication_uses_reloadable_matched_names():
    generated = compose_text(Cluster(regions=2, synchronous=True))
    for scope in ("hq", "r1", "r2"):
        block = service_block(generated, f"{scope}-primary")
        assert f"ROS_SYNC_STANDBY: {scope}_standby" in block
        assert "synchronous_standby_names" not in block
    assert "ROS_SYNC_STANDBY" not in service_block(generated, BASELINE)


def test_synchronous_replication_requires_standbys():
    with pytest.raises(ValueError, match="requires standbys"):
        compose_text(Cluster(standbys=False, synchronous=True))


def test_generate_writes_all_three_files(tmp_path):
    cluster = Cluster(regions=2)
    compose, proxy_list, cluster_file = generate(cluster, tmp_path, "regions = 2\n")
    assert compose.read_text(encoding="utf-8") == compose_text(cluster)
    assert proxy_list.read_text(encoding="utf-8") == toxiproxy_config(cluster)
    assert proxy_list.name == "proxies.json"
    # the copy of the config bootstrap ran with, which the service
    # entries mount (a region-count change needs no rebuild)
    assert cluster_file.name == "cluster.toml"
    assert cluster_file.read_text(encoding="utf-8") == "regions = 2\n"
    text = compose.read_text(encoding="utf-8")
    # the Compose file mounts the generated proxy list and the copied
    # cluster config, never the committed ones
    assert "./proxies.json:/config/proxies.json:ro" in text
    assert "../toxiproxy/proxies.json" not in text
    assert "./cluster.toml:/app/infra/cluster.toml:ro" in text
    assert "../cluster.toml" not in text


# ---- container mode: one entry per service_specs row ------------------------------


def test_service_entries_mirror_the_table_for_one_and_three_regions():
    for regions, count in ((1, 5), (3, 9)):
        cluster = Cluster(regions=regions)
        generated = compose_text(cluster)
        specs = service_specs(cluster)
        assert len(specs) == count
        for spec in specs:
            block = service_block(generated, spec.name)
            # every entry is gated behind the services profile, so
            # bootstrap's plain `up -d --wait` starts only the databases
            assert 'profiles: ["services"]' in block, spec.name
            assert "image: restaurantos-services:local" in block
            command = "[" + ", ".join(['"python"', *(f'"{arg}"' for arg in spec.args)]) + "]"
            assert f"command: {command}" in block, spec.name
            assert "init: true" in block
            # the identity environment comes from the table, plus the
            # container-mode keys
            for key, value in spec.env.items():
                assert f"      {key}: {value}" in block, (spec.name, key)
            assert "      ROS_NETWORK: compose" in block
            assert "      ROS_CLUSTER_FILE: /app/infra/cluster.toml" in block
            assert "      - ./cluster.toml:/app/infra/cluster.toml:ro" in block


def test_service_ports_and_healthchecks_come_from_the_table():
    for regions in (1, 3):
        cluster = Cluster(regions=regions)
        generated = compose_text(cluster)
        for spec in service_specs(cluster):
            block = service_block(generated, spec.name)
            if spec.port is None:
                # the relays and the HQ worker: no port to publish, no
                # ready route to probe
                assert "    ports:" not in block, spec.name
                assert "    healthcheck:" not in block, spec.name
            else:
                assert spec.port == service_port(cluster, spec.name)
                assert f'- "127.0.0.1:{spec.port}:{spec.port}"' in block, spec.name
                # the image has no curl: the healthcheck is a stdlib
                # one-liner against the spec's ready route
                assert (
                    f"urllib.request.urlopen('http://127.0.0.1:{spec.port}{spec.ready}',"
                    " timeout=2)" in block
                ), spec.name


def test_depends_on_edges_mirror_the_start_order():
    # the region side first, the gateway and the HQ worker after it, the
    # coordinator last; service_healthy where the depended-on service
    # has a ready route, service_started for the relays and the HQ worker
    cluster = Cluster(regions=3)
    generated = compose_text(cluster)
    for rid in cluster.region_ids:
        assert "    depends_on:" not in service_block(generated, f"relay-{rid}")
        assert "    depends_on:" not in service_block(generated, f"edge-{rid}")
    for name in ("gateway", "hq-worker"):
        block = service_block(generated, name)
        for rid in cluster.region_ids:
            assert f"      relay-{rid}:\n        condition: service_started" in block, name
            assert f"      edge-{rid}:\n        condition: service_healthy" in block, name
    coordinator = service_block(generated, "coordinator")
    assert "      gateway:\n        condition: service_healthy" in coordinator
    assert "      hq-worker:\n        condition: service_started" in coordinator


def test_no_control_or_web_entries():
    # control (WP6b) and web (WP7) get their own entries; 8002 is the
    # control plane's port and must not appear here either
    for regions in (1, 3):
        generated = compose_text(Cluster(regions=regions))
        assert not re.search(r"^  control:$", generated, re.MULTILINE)
        assert not re.search(r"^  web:$", generated, re.MULTILINE)
        assert ":8002" not in generated


def test_generated_file_is_valid_yaml_with_exactly_the_table_services():
    for regions in (1, 3):
        cluster = Cluster(regions=regions)
        doc = yaml.safe_load(compose_text(cluster))
        expected = {spec.name for spec in service_specs(cluster)}
        expected |= {"toxiproxy", *cluster.nodes, BASELINE}
        assert set(doc["services"]) == expected
        for spec in service_specs(cluster):
            entry = doc["services"][spec.name]
            assert entry["profiles"] == ["services"]
            assert entry["image"] == "restaurantos-services:local"
            assert entry["command"] == ["python", *spec.args]
            expected_env = dict(spec.env)
            expected_env["ROS_NETWORK"] = "compose"
            expected_env["ROS_CLUSTER_FILE"] = "/app/infra/cluster.toml"
            assert entry["environment"] == expected_env
            assert entry["volumes"] == ["./cluster.toml:/app/infra/cluster.toml:ro"]

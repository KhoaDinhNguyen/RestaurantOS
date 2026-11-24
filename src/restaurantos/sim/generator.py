"""Write the cluster's Compose file and proxy list into infra/generated/
(git-ignored), for any number of regions, plus one profile-gated
service entry per application service for container mode (architecture
Section 4): same names, ports and identity environment as host mode,
with ROS_NETWORK=compose, all read from the one service table in
sim.services.

For `regions = 1` the database services must match the committed dev
baseline `infra/compose.yaml` in image, flags, volumes and ports — the
two files share the Compose project name `restaurantos`, so only one can
run at a time and neither may drift from the other. The unit tests check
that agreement; this generator only ever writes to infra/generated/, and
never to the committed files.
"""

from __future__ import annotations

from pathlib import Path

from restaurantos.common.cluster import (
    BASELINE,
    HQ_PRIMARY,
    HQ_STANDBY,
    Cluster,
    host_port,
    primary_of,
    proxies,
    slot_name,
    standby_of,
    toxiproxy_config,
)
from restaurantos.sim.services import ServiceSpec, service_specs

# The committed infra/compose.yaml pins this digest; the generated file
# keeps the same default and the same ROS_POSTGRES_IMAGE override.
IMAGE_DEFAULT = (
    "postgres:18@sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42"
)

SERVICE_IMAGE = "restaurantos-services:local"
"""The one image every Python service runs from (infra/Dockerfile,
docs/setup.md). Built by hand; bootstrap never builds it."""

SERVICES_PROFILE = "services"
"""The Compose profile that gates the service entries: bootstrap's plain
`up -d --wait` starts ONLY the databases and Toxiproxy, and starting the
services is an explicit `--profile services` (a service entry without a
profile would be started before the data is loaded, and would fail if
the image is not built)."""

CLUSTER_MOUNT = "./cluster.toml:/app/infra/cluster.toml:ro"
"""The mounted cluster file, relative to the generated file's directory
(infra/generated/) - never `./infra/...`, which would resolve one level
too high. Bootstrap copies the config it ran with next to the generated
file under this name, so a region-count change needs no rebuild."""

CLUSTER_FILE_IN_CONTAINER = "/app/infra/cluster.toml"

_PG_COMMAND = """x-pg-command: &pg-command
  - postgres
  - -c
  - wal_level=replica
  - -c
  - max_wal_senders=10
  - -c
  - max_replication_slots=10
  - -c
  - hot_standby=on
  - -c
  - max_slot_wal_keep_size=2GB
  - -c
  - shared_buffers=256MB"""


def _proxy_port(cluster: Cluster, client: str, target: str) -> int:
    for proxy in proxies(cluster):
        if proxy.name == f"{client}__{target}":
            return proxy.port
    raise KeyError(f"no proxy {client}__{target} in this cluster")


def _start_rank(name: str) -> int:
    """The start order the service entries spell out: the region side
    first (the relays and the edge APIs), then the gateway and the HQ
    worker, the coordinator last (the reverse of the host `down`
    order)."""
    if name in ("gateway", "hq-worker"):
        return 1
    if name == "coordinator":
        return 2
    return 0


def _service_entry(spec: ServiceSpec, deps: list[ServiceSpec]) -> list[str]:
    """One Compose service entry, from the same table `rosctl up` reads
    (sim.services): the name, command, identity environment and port
    all come from the spec, never typed here."""
    command = "[" + ", ".join(['"python"', *(f'"{arg}"' for arg in spec.args)]) + "]"
    env = dict(spec.env)
    env["ROS_NETWORK"] = "compose"
    env["ROS_CLUSTER_FILE"] = CLUSTER_FILE_IN_CONTAINER
    block = [
        "",
        f"  {spec.name}:",
        f'    profiles: ["{SERVICES_PROFILE}"]',
        f"    image: {SERVICE_IMAGE}",
        f"    command: {command}",
        "    init: true",
        "    environment:",
    ]
    for key, value in env.items():
        block.append(f"      {key}: {value}")
    block += [
        "    volumes:",
        f"      - {CLUSTER_MOUNT}",
    ]
    if spec.ready is not None:
        if spec.port is None:
            raise ValueError(f"service {spec.name} has a ready route but no port")
        # The image has no curl (python:3.14-slim), so the healthcheck is
        # a stdlib one-liner against the spec's ready route; urlopen
        # raises on any non-2xx answer, which is a failed check.
        block += [
            "    healthcheck:",
            '      test: ["CMD", "python", "-c", "import urllib.request; '
            f"urllib.request.urlopen('http://127.0.0.1:{spec.port}{spec.ready}', timeout=2)\"]",
            "      interval: 3s",
            "      timeout: 3s",
            "      retries: 40",
            "      start_period: 5s",
        ]
    if deps:
        block.append("    depends_on:")
        for dep in deps:
            condition = "service_healthy" if dep.ready is not None else "service_started"
            block += [f"      {dep.name}:", f"        condition: {condition}"]
    if spec.port is not None:
        block += [
            "    ports:",
            f'      - "127.0.0.1:{spec.port}:{spec.port}"',
        ]
    return block


def _service_entries(cluster: Cluster) -> list[str]:
    """Every service entry for this cluster, one per `service_specs`
    row, in start order. Each entry depends on the previous rank: the
    gateway and the HQ worker wait for the region side, the coordinator
    waits for both - `service_healthy` where the depended-on service
    has a ready route, `service_started` for the relays and the HQ
    worker, which have no port."""
    by_rank: dict[int, list[ServiceSpec]] = {}
    for spec in service_specs(cluster):
        by_rank.setdefault(_start_rank(spec.name), []).append(spec)
    lines: list[str] = []
    for rank in sorted(by_rank):
        deps = by_rank.get(rank - 1, [])
        for spec in by_rank[rank]:
            lines += _service_entry(spec, deps)
    return lines


def compose_text(cluster: Cluster) -> str:
    """The complete Compose file for this cluster: the databases and the
    baseline, plus one profile-gated entry per application service.

    Written as text, not dumped through a YAML library: the Compose file is
    a deliverable people diff by hand, and the committed baseline it must
    stay in step with is written the same way.

    Synchronous settings are written by init-primary after schema setup,
    so they remain reloadable for coordinator fallback. The baseline has
    no standby and always stays asynchronous.
    """
    if cluster.synchronous and not cluster.standbys:
        raise ValueError("synchronous replication requires standbys")
    lines = [
        "# Generated by restaurantos.sim.generator (rosctl bootstrap).",
        "# Do not edit, do not commit; regenerate with a changed infra/cluster.toml.",
        "# For regions = 1 the database services match the committed",
        "# infra/compose.yaml in image, flags, volumes and ports.",
        "",
        "name: restaurantos",
        "",
        "x-postgres-image: &pg-image ${ROS_POSTGRES_IMAGE:-" + IMAGE_DEFAULT + "}",
        "",
        _PG_COMMAND,
        "",
        """x-pg-env: &pg-env
  POSTGRES_USER: ros
  POSTGRES_PASSWORD: ros
  POSTGRES_DB: restaurantos
  ROS_REPL_PASSWORD: replicate""",
        "",
        """x-pg-health: &pg-health
  test: ["CMD-SHELL", "pg_isready -U ros -d restaurantos"]
  interval: 3s
  timeout: 3s
  retries: 40""",
        "",
        "services:",
        "  toxiproxy:",
        "    image: ghcr.io/shopify/toxiproxy:2.12.0",
        '    command: ["-host=0.0.0.0", "-config=/config/proxies.json"]',
        "    volumes:",
        "      - ./proxies.json:/config/proxies.json:ro",
    ]
    # The Toxiproxy API and every proxy port, so services on the host take
    # the same routes as services in the Compose network.
    lines.append("    ports:")
    api_and_proxies = sorted({8474, *(p.port for p in proxies(cluster))})
    for port in api_and_proxies:
        lines.append(f'      - "127.0.0.1:{port}:{port}"')

    def primary_service(name: str, kind: str, region_id: str | None) -> list[str]:
        block = [
            "",
            f"  {name}:",
            "    image: *pg-image",
            "    command: *pg-command",
            "    environment:",
            "      <<: *pg-env",
            f"      ROS_NODE_KIND: {kind}",
        ]
        if cluster.synchronous and name != BASELINE:
            block.append(
                f"      ROS_SYNC_STANDBY: {slot_name(standby_of(name.removesuffix('-primary')))}"
            )
        if region_id is not None:
            block.append(f"      ROS_REGION_ID: {region_id}")
        block += [
            "    volumes:",
            f"      - {name}-data:/var/lib/postgresql",
            "      - ../../db:/ros/db:ro",
            "      - ../postgres/init-primary.sh:/docker-entrypoint-initdb.d/10-init-primary.sh:ro",
            "    healthcheck: *pg-health",
            "    ports:",
            f'      - "127.0.0.1:{host_port(cluster, name)}:5432"',
        ]
        return block

    def standby_service(name: str, primary: str) -> list[str]:
        scope = name.removesuffix("-standby")
        return [
            "",
            f"  {name}:",
            "    image: *pg-image",
            '    entrypoint: ["bash", "/ros/infra/standby-entrypoint.sh"]',
            "    command: *pg-command",
            "    environment:",
            "      <<: *pg-env",
            "      ROS_PRIMARY_HOST: toxiproxy",
            f'      ROS_PRIMARY_PORT: "{_proxy_port(cluster, name, primary)}"'
            f"        # {name}__{primary}",
            f"      ROS_SLOT_NAME: {scope}_standby",
            "    volumes:",
            f"      - {name}-data:/var/lib/postgresql",
            "      - ../postgres/standby-entrypoint.sh:/ros/infra/standby-entrypoint.sh:ro",
            "    depends_on:",
            f"      {primary}:",
            "        condition: service_healthy",
            "      toxiproxy:",
            "        condition: service_started",
            "    healthcheck: *pg-health",
            "    ports:",
            f'      - "127.0.0.1:{host_port(cluster, name)}:5432"',
        ]

    lines += primary_service(HQ_PRIMARY, "hq", None)
    if cluster.standbys:
        lines += standby_service(HQ_STANDBY, HQ_PRIMARY)
    for rid in cluster.region_ids:
        primary, standby = primary_of(rid), standby_of(rid)
        lines += primary_service(primary, "region", rid)
        if cluster.standbys:
            lines += standby_service(standby, primary)
    # The baseline: a region-kind node with no standby, for the invariant
    # checker and the cross-region queries (architecture Section 7.1).
    lines += primary_service(BASELINE, "region", BASELINE)
    # The application services, one entry per service_specs row, gated
    # behind the "services" profile so bootstrap's plain `up -d --wait`
    # starts only the databases and Toxiproxy.
    lines += _service_entries(cluster)

    lines.append("")
    lines.append("volumes:")
    for name in [*cluster.nodes, BASELINE]:
        lines.append(f"  {name}-data:")
    return "\n".join(lines) + "\n"


def write_generated(
    out_dir: Path, compose: str, proxy_list: str, cluster_toml: str
) -> tuple[Path, Path, Path]:
    """Write pre-rendered text into out_dir, and return the file paths:
    the Compose file, the Toxiproxy proxy list, and the copy of the
    cluster config the service entries mount (`cluster.toml`, from
    whatever --config bootstrap ran with).

    Bootstrap renders all three before it tears anything down, so that
    a rendering failure leaves the old cluster untouched; this function
    is called only after the teardown succeeded."""
    out_dir.mkdir(parents=True, exist_ok=True)
    compose_path = out_dir / "compose.yaml"
    proxy_path = out_dir / "proxies.json"
    cluster_path = out_dir / "cluster.toml"
    compose_path.write_text(compose, encoding="utf-8")
    proxy_path.write_text(proxy_list, encoding="utf-8")
    cluster_path.write_text(cluster_toml, encoding="utf-8")
    return compose_path, proxy_path, cluster_path


def generate(cluster: Cluster, out_dir: Path, cluster_toml: str) -> tuple[Path, Path, Path]:
    """Render for this cluster and write all three files into out_dir."""
    return write_generated(out_dir, compose_text(cluster), toxiproxy_config(cluster), cluster_toml)

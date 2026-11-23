"""One-way, operator-authorized HQ database promotion (architecture D10).

HQ has no node epoch or directory row. Fence its old primary with Docker,
not the regional epoch mechanism. Client routing requires a team decision.
"""

import asyncio
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from psycopg.types.json import Jsonb

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster, load_cluster
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.locks import CLUSTER_EVENTS_LOCK_KEY
from restaurantos.common.sqlfiles import load_query
from restaurantos.coordinator.config import CoordinatorSettings


class HQPromotionError(RuntimeError):
    """No promotion authorized, or completion still needs inspection/retry."""


async def command(args: list[str]) -> str:
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    # This deadline wraps a Docker subprocess, never a database call.
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=10)
    except TimeoutError as error:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise HQPromotionError("Docker command timed out; inspect HQ before retrying") from error
    if process.returncode:
        raise HQPromotionError(output.decode(errors="replace") or "Docker command failed")
    return output.decode()


async def inspect_container(container: str) -> dict:
    items = json.loads(await command(["docker", "inspect", container]))
    if len(items) != 1:
        raise HQPromotionError("expected exactly one HQ container")
    return items[0]


def validate_containers(definition: dict, containers: dict[str, dict]) -> None:
    """Reject a mismatched project, shared storage, or unexpected PGDATA."""
    sources = []
    for node, item in containers.items():
        labels = item["Config"].get("Labels", {})
        if (
            labels.get("com.docker.compose.service") != node
            or labels.get("com.docker.compose.project") != definition["name"]
        ):
            raise HQPromotionError("HQ container does not belong to this Compose project")
        mounts = [m for m in item["Mounts"] if m["Destination"] == "/var/lib/postgresql"]
        if len(mounts) != 1 or mounts[0]["Type"] != "volume":
            raise HQPromotionError("HQ promotion requires separate named PostgreSQL volumes")
        service_mounts = definition["services"][node]["volumes"]
        declared_mounts = [m for m in service_mounts if m["target"] == "/var/lib/postgresql"]
        if len(declared_mounts) != 1 or declared_mounts[0].get("type", "volume") != "volume":
            raise HQPromotionError("Compose must declare one named PostgreSQL volume per HQ node")
        declared = declared_mounts[0]
        expected = definition["volumes"][declared["source"]]["name"]
        if mounts[0]["Name"] != expected:
            raise HQPromotionError("HQ running volume differs from this Compose file")
        sources.append(mounts[0]["Name"])
        if not pgdata(item).startswith("/var/lib/postgresql/"):
            raise HQPromotionError("unexpected HQ PGDATA layout")
    if len(set(sources)) != 2:
        raise HQPromotionError("HQ containers share a data volume")
    if containers[HQ_PRIMARY]["Image"] != containers[HQ_STANDBY]["Image"]:
        raise HQPromotionError("HQ nodes must use the same PostgreSQL image")
    if not containers[HQ_STANDBY]["State"]["Running"]:
        raise HQPromotionError("HQ standby container is not running")


def pgdata(container: dict) -> str:
    return next(
        (v.removeprefix("PGDATA=") for v in container["Config"]["Env"] if v.startswith("PGDATA=")),
        "",
    )


async def source_identifier(container: dict) -> str:
    # Offline control-file read also works after a real crash. A read-only
    # mount and no network ensure this helper cannot start a second server.
    output = await command(
        [
            "docker",
            "run",
            "--rm",
            "--pull=never",
            "--network",
            "none",
            "--volumes-from",
            f"{container['Id']}:ro",
            "-e",
            "LC_ALL=C",
            "--entrypoint",
            "pg_controldata",
            container["Image"],
            pgdata(container),
        ]
    )
    for line in output.splitlines():
        if line.startswith("Database system identifier:"):
            return line.split(":", 1)[1].strip()
    raise HQPromotionError("could not verify the old HQ database system identifier")


async def role(conn) -> tuple:
    return await (await conn.execute(load_query("coordinator/hq_promotion_state"))).fetchone()


async def promote(
    cluster: Cluster, settings: Settings, config: CoordinatorSettings, compose_file: Path
) -> dict:
    if not cluster.standbys:
        raise HQPromotionError("HQ has no configured standby")
    compose = ["docker", "compose", "-f", str(compose_file.resolve())]
    definition = json.loads(await command([*compose, "config", "--format", "json"]))
    if not all(node in definition.get("services", {}) for node in (HQ_PRIMARY, HQ_STANDBY)):
        raise HQPromotionError("Compose must contain both HQ services")
    containers = {}
    for node in (HQ_PRIMARY, HQ_STANDBY):
        ids = (await command([*compose, "ps", "--all", "--quiet", node])).split()
        if len(ids) != 1:
            raise HQPromotionError(f"expected one existing container for {node}")
        containers[node] = await inspect_container(ids[0])
    validate_containers(definition, containers)
    old = containers[HQ_PRIMARY]
    identifier = await source_identifier(old)
    # Docker stop has a five-second graceful limit. Leave enough time for
    # fencing as well as bounded database operations, without cancelling SQL.
    limits = settings.model_copy(
        update={
            "request_deadline_s": config.failover_timeout_s + 40,
            "idle_in_transaction_timeout_ms": int((config.failover_timeout_s + 40) * 1000),
        }
    )
    async with make_pool(node_conninfo(cluster, HQ_STANDBY, settings=settings), limits) as pool:

        async def transition(conn):
            locked = await (
                await conn.execute(load_query("coordinator/hq_promotion_lock"))
            ).fetchone()
            if locked != (True,):
                raise HQPromotionError("HQ promotion is already running")
            state = await role(conn)
            if not state or state[1] != identifier or not state[2]:
                raise HQPromotionError("candidate is not a copy of this project's HQ database")
            if not state[0] and old["State"]["Running"]:
                async with make_pool(
                    node_conninfo(cluster, HQ_PRIMARY, settings=settings), settings
                ) as old_pool:
                    try:
                        old_role = await run_on(old_pool, role, settings=settings)
                    except RegionUnreachableError:
                        old_role = None
                    if old_role and old_role[0] and old_role[1] == identifier:
                        raise HQPromotionError(
                            "HQ is already promoted and its old primary has rejoined"
                        )
            # A failed or ambiguous fence must never authorize promotion.
            await command(["docker", "update", "--restart=no", old["Id"]])
            await command(["docker", "stop", "--time", "5", old["Id"]])
            stopped = await inspect_container(old["Id"])
            if (
                stopped["State"]["Running"]
                or stopped["HostConfig"]["RestartPolicy"]["Name"] != "no"
            ):
                raise HQPromotionError(
                    "old HQ primary is not stopped with automatic restart disabled"
                )
            if state[0]:
                await conn.execute(load_query("coordinator/promote"))
            deadline = time.monotonic() + config.failover_timeout_s
            while True:
                state = await role(conn)
                if state and not state[0] and state[1] == identifier and state[2]:
                    return state
                if time.monotonic() >= deadline:
                    raise HQPromotionError(
                        "HQ promotion not confirmed; old primary remains stopped"
                    )
                await asyncio.sleep(0.05)

        await run_on(pool, transition, region_id="hq", settings=limits)

        async def record(conn):
            # A fresh transaction after leaving recovery can write the event.
            await conn.execute(load_query("coordinator/local_epoch_commit"))
            state = await role(conn)
            if state[0] or state[1] != identifier or not state[2]:
                raise HQPromotionError("HQ candidate is no longer the promoted database")
            marker = f"hq-failover:{identifier}:{state[3]}"
            await conn.execute(
                load_query("coordinator/lock_events"), {"lock_key": CLUSTER_EVENTS_LOCK_KEY}
            )
            await conn.execute(
                load_query("coordinator/record_event"),
                {
                    "occurred_at": datetime.now(UTC),
                    "kind": "failover",
                    "region_id": None,
                    "node_id": HQ_STANDBY,
                    "event_key": marker,
                    "detail": Jsonb(
                        {
                            "coordinator_event_id": marker,
                            "scope": "hq",
                            "old_primary": HQ_PRIMARY,
                            "new_primary": HQ_STANDBY,
                            "manual": True,
                            "replication_after_promotion": "async",
                        }
                    ),
                },
            )
            event = await (
                await conn.execute(load_query("coordinator/failover_event"), {"marker": marker})
            ).fetchone()
            return {
                "primary_node": HQ_STANDBY,
                "old_primary_stopped": True,
                "event_id": event[0],
                "client_switch_required": False,
            }

        result = await run_on(pool, record, region_id="hq", settings=settings)
        # The old partner is fenced. Retained sync settings cannot be
        # satisfied until rejoin, so manual promotion leaves HQ async.
        async with make_pool(
            node_conninfo(cluster, HQ_STANDBY, settings=settings), settings
        ) as config_pool:

            async def disable(conn):
                await conn.set_autocommit(True)
                await conn.execute(load_query("coordinator/disable_sync"))
                await conn.execute(load_query("coordinator/reload_config"))

            await run_on(config_pool, disable, settings=settings)
        deadline = time.monotonic() + config.failover_timeout_s

        async def names(conn):
            return await (await conn.execute(load_query("coordinator/sync_names"))).fetchone()

        while await run_on(pool, names, settings=settings) != ("",):
            if time.monotonic() >= deadline:
                raise HQPromotionError(
                    "HQ asynchronous policy not confirmed; inspect before retrying"
                )
            await asyncio.sleep(0.05)
        return result


def run(args) -> int:
    from restaurantos.coordinator.hq_rejoin import recover
    from restaurantos.coordinator.rejoin import RecoveryError

    operation = args.hq_command
    if not args.yes:
        print(
            f"rosctl hq {operation}: requires --yes; this changes HQ and may lose "
            "unreplicated data.",
            file=sys.stderr,
        )
        return 1
    settings = Settings(client="coordinator", network="host")
    try:
        cluster = load_cluster(args.config or settings.cluster_file)
        if operation == "rejoin":
            output = asyncio.run(
                recover(cluster, settings, CoordinatorSettings(), args.compose_file)
            )
            print(output, file=sys.stderr, end="")
            result = {"primary_node": HQ_STANDBY, "standby_node": HQ_PRIMARY, "rejoined": True}
        else:
            result = asyncio.run(
                promote(cluster, settings, CoordinatorSettings(), args.compose_file)
            )
    except (HQPromotionError, RecoveryError, RegionUnreachableError, OSError, ValueError) as error:
        print(
            f"rosctl hq {operation}: {error}. Inspect HQ before retrying; do not restart "
            "the old primary without rebuilding/rejoining it.",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result))
    return 0

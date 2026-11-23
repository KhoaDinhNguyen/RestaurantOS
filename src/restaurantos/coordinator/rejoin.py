"""Host-side operator command for rejoining HQ's recorded regional standby.

Docker operations run outside the coordinator service, like rosctl's lifecycle
commands. Only the standby's volume may be replaced. Source selection and the
reverse replication proxy come from the shared cluster helpers and HQ.
"""

import argparse
import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from restaurantos.common.cluster import (
    HQ_PRIMARY,
    Cluster,
    load_cluster,
    primary_of,
    proxy_for,
    slot_name,
    standby_of,
)
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, probe_node, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.sqlfiles import load_query
from restaurantos.coordinator.config import CoordinatorSettings

ROOT = Path(__file__).resolve().parents[3]


class RecoveryError(RuntimeError):
    """Refuse recovery or keep a partially recovered target stopped."""


@dataclass(frozen=True)
class RejoinPlan:
    region_id: str
    epoch: int
    source: str
    target: str

    def validate(self, cluster: Cluster) -> None:
        if self.region_id not in cluster.region_ids or not cluster.standbys:
            raise RecoveryError("region must have a configured standby")
        pair = {primary_of(self.region_id), standby_of(self.region_id)}
        if {self.source, self.target} != pair or self.epoch < 1:
            raise RecoveryError("HQ does not name a valid regional primary/standby pair")


async def read_plan(cluster: Cluster, settings: Settings, region_id: str) -> RejoinPlan:
    pool = make_pool(node_conninfo(cluster, HQ_PRIMARY, settings=settings), settings)
    await pool.open()
    try:

        async def read(conn):
            cursor = await conn.execute(
                load_query("coordinator/rejoin_target"), {"region_id": region_id}
            )
            row = await cursor.fetchone()
            if row is None:
                raise RecoveryError("region is not in HQ's directory")
            return RejoinPlan(*row)

        plan = await run_on(pool, read, region_id="hq", settings=settings)
        plan.validate(cluster)
        view = await probe_node(
            plan.source, node_conninfo(cluster, plan.source, settings=settings), settings
        )
        if not view.writable or view.epoch != plan.epoch:
            raise RecoveryError("finish the promotion before rejoining the old primary")
        return plan
    finally:
        await pool.close()


async def command(args: list[str], *, env: dict | None = None, script: str | None = None) -> str:
    process = await asyncio.create_subprocess_exec(
        *args,
        env=env,
        stdin=asyncio.subprocess.PIPE if script is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate(None if script is None else script.encode())
    text = output.decode(errors="replace")
    if process.returncode:
        # Do not include arguments: connection credentials are carried in env.
        raise RecoveryError(text or "Docker recovery command failed")
    return text


def validate_storage(config: dict, plan: RejoinPlan) -> None:
    """Do not stop a node if this Compose file shares its data with the source."""

    def volume(node):
        service = config.get("services", {}).get(node)
        if service is None:
            raise RecoveryError(f"Compose file has no service {node}")
        mounts = [v for v in service.get("volumes", []) if v["target"] == "/var/lib/postgresql"]
        if len(mounts) != 1:
            raise RecoveryError("rejoin requires the documented PostgreSQL volume layout")
        mount = mounts[0]
        source = mount["source"]
        if mount.get("type", "volume") == "volume":
            source = config.get("volumes", {}).get(source, {}).get("name", source)
        return mount.get("type", "volume"), source

    if volume(plan.source) == volume(plan.target):
        raise RecoveryError("source and target share a data volume")


async def stop_target(
    cluster: Cluster, settings: Settings, plan: RejoinPlan, compose: list[str]
) -> None:
    """Serialize stopping against automatic promotion of this very node."""
    pool = make_pool(node_conninfo(cluster, HQ_PRIMARY, settings=settings), settings)
    await pool.open()
    try:

        async def stop(conn):
            await conn.execute(load_query("coordinator/promotion_limits"), {"timeout_ms": "45000"})
            cursor = await conn.execute(
                load_query("coordinator/rejoin_lock"), {"region_id": plan.region_id}
            )
            row = await cursor.fetchone()
            if row is None or RejoinPlan(*row) != plan:
                raise RecoveryError("HQ changed or a promotion is running; no node stopped")
            await command([*compose, "stop", "--timeout", "30", plan.target])

        await run_on(
            pool,
            stop,
            region_id="hq",
            settings=settings.model_copy(update={"request_deadline_s": 40}),
        )
    finally:
        await pool.close()


async def streaming(cluster: Cluster, settings: Settings, plan: RejoinPlan) -> bool:
    pool = make_pool(node_conninfo(cluster, plan.source, settings=settings), settings)
    await pool.open()
    try:

        async def read(conn):
            cursor = await conn.execute(
                load_query("coordinator/rejoin_streaming"), {"slot": slot_name(plan.target)}
            )
            return await cursor.fetchone() == (True,)

        return await run_on(pool, read, settings=settings)
    except RegionUnreachableError:
        return False
    finally:
        await pool.close()


async def recover(
    cluster: Cluster,
    settings: Settings,
    config: CoordinatorSettings,
    region_id: str,
    compose_file: Path,
) -> str:
    plan = await read_plan(cluster, settings, region_id)
    compose = ["docker", "compose", "-f", str(compose_file.resolve())]
    validate_storage(json.loads(await command([*compose, "config", "--format", "json"])), plan)
    proxy = proxy_for(cluster, plan.target, plan.source)
    source_settings = settings.model_copy(update={"client": plan.target, "network": "compose"})
    env = dict(
        os.environ,
        ROS_REJOIN_OFFLINE="1",
        ROS_REJOIN_SOURCE_DSN=node_conninfo(cluster, plan.source, settings=source_settings),
        ROS_REJOIN_REGION=region_id,
        ROS_REJOIN_EPOCH=str(plan.epoch),
        ROS_PRIMARY_HOST="toxiproxy",
        ROS_PRIMARY_PORT=str(proxy.port),
        ROS_SLOT_NAME=slot_name(plan.target),
        ROS_REJOIN_TIMEOUT_S=str(config.rejoin_timeout_s),
    )
    await stop_target(cluster, settings, plan, compose)
    # Verify HQ again after stopping, before any files can be replaced.
    if await read_plan(cluster, settings, region_id) != plan:
        raise RecoveryError("HQ changed; target remains stopped")
    overrides = [
        item
        for key in env
        if key.startswith("ROS_REJOIN_")
        or key in ("ROS_PRIMARY_HOST", "ROS_PRIMARY_PORT", "ROS_SLOT_NAME")
        for item in ("-e", key)
    ]
    output = await command(
        [
            *compose,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "bash",
            "--volume",
            f"{ROOT / 'db/queries/coordinator'}:/ros/rejoin-sql:ro",
            *overrides,
            plan.target,
            "-s",
        ],
        env=env,
        script=(ROOT / "infra/postgres/rejoin-node.sh").read_text(),
    )
    if await read_plan(cluster, settings, region_id) != plan:
        raise RecoveryError("HQ changed; recovered target remains stopped")
    await command([*compose, "start", plan.target])
    # Never report success merely because Docker started the process.
    deadline = time.monotonic() + config.rejoin_timeout_s
    while time.monotonic() < deadline:
        view = await probe_node(
            plan.target, node_conninfo(cluster, plan.target, settings=settings), settings
        )
        if (
            view.reachable
            and view.in_recovery
            and view.epoch == plan.epoch
            and await streaming(cluster, settings, plan)
        ):
            return output + f"{plan.target} rejoined as standby of {plan.source}\n"
        await asyncio.sleep(1)
    # If HQ assigned a new primary during readiness checks, do not stop it.
    await stop_target(cluster, settings, plan, compose)
    raise RecoveryError("standby did not recover to HQ's epoch; target stopped for inspection")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("region_id")
    parser.add_argument("--compose-file", type=Path, default=Path("infra/compose.yaml"))
    parser.add_argument("--plan", action="store_true", help="show roles without changing anything")
    args = parser.parse_args()
    settings = Settings(client="coordinator", network="host")
    cluster = load_cluster(settings.cluster_file)
    try:
        if args.plan:
            print(json.dumps(asdict(asyncio.run(read_plan(cluster, settings, args.region_id)))))
        else:
            print(
                asyncio.run(
                    recover(
                        cluster, settings, CoordinatorSettings(), args.region_id, args.compose_file
                    )
                ),
                end="",
            )
    except (RecoveryError, RegionUnreachableError) as exc:
        parser.exit(1, f"rejoin: {exc}\n")


if __name__ == "__main__":
    main()

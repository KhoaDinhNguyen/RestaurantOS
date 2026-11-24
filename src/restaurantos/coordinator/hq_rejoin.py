"""Rejoin the fenced original HQ primary after D10 promotion.

The candidate-side lock is shared with HQ promotion. Only the stopped old
node's data may be rewound/rebuilt; its source must have a committed D10 event.
"""

import asyncio
import json
import os
import time
from pathlib import Path

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster, proxy_for, slot_name
from restaurantos.common.config import Settings
from restaurantos.common.db import make_pool, node_conninfo, run_on
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.common.sqlfiles import load_query
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.hq_promotion import (
    inspect_container,
    role,
    validate_containers,
)
from restaurantos.coordinator.rejoin import ROOT, RecoveryError, command


async def require_async(conn) -> None:
    """A promotion event is durable before its policy change is complete."""
    names = await (await conn.execute(load_query("coordinator/sync_names"))).fetchone()
    if names != ("",):
        raise RecoveryError(
            "HQ synchronous policy is still enabled; finish promotion before rejoin"
        )


async def recover(
    cluster: Cluster, settings: Settings, config: CoordinatorSettings, compose_file: Path
) -> str:
    if not cluster.standbys:
        raise RecoveryError("HQ has no standby")
    compose = ["docker", "compose", "-f", str(compose_file.resolve())]
    definition = json.loads(await command([*compose, "config", "--format", "json"]))
    containers = {}
    for node in (HQ_PRIMARY, HQ_STANDBY):
        ids = (await command([*compose, "ps", "--all", "--quiet", node])).split()
        if len(ids) != 1:
            raise RecoveryError(f"expected one existing container for {node}")
        containers[node] = await inspect_container(ids[0])
    validate_containers(definition, containers)
    target = containers[HQ_PRIMARY]
    if target["State"]["Running"] or target["HostConfig"]["RestartPolicy"]["Name"] != "no":
        raise RecoveryError("old HQ must remain fenced by the promotion command before rejoin")
    limits = settings.model_copy(
        update={
            "request_deadline_s": config.rejoin_timeout_s + 60,
            "idle_in_transaction_timeout_ms": int((config.rejoin_timeout_s + 60) * 1000),
        }
    )
    async with make_pool(node_conninfo(cluster, HQ_STANDBY, settings=settings), limits) as source:

        async def rebuild(conn):
            locked = await (
                await conn.execute(load_query("coordinator/hq_promotion_lock"))
            ).fetchone()
            if locked != (True,):
                raise RecoveryError("another HQ promotion or rejoin is running")
            state = await role(conn)
            if state[0] or not state[2]:
                raise RecoveryError("HQ standby has not been promoted")
            marker = f"hq-failover:{state[1]}:{state[3]}"
            if (
                await (
                    await conn.execute(load_query("coordinator/failover_event"), {"marker": marker})
                ).fetchone()
                is None
            ):
                raise RecoveryError("finish HQ promotion event recording before rejoin")
            await require_async(conn)
            # Recheck the physical fence inside the shared operation lock.
            stopped = await inspect_container(target["Id"])
            if stopped["State"]["Running"]:
                raise RecoveryError("old HQ was restarted; refuse to touch its data")
            proxy = proxy_for(cluster, HQ_PRIMARY, HQ_STANDBY)
            replica_settings = settings.model_copy(
                update={"client": HQ_PRIMARY, "network": "compose"}
            )
            env = dict(
                os.environ,
                ROS_REJOIN_OFFLINE="1",
                ROS_REJOIN_KIND="hq",
                ROS_REJOIN_HQ_ID=state[1],
                ROS_REJOIN_HQ_TIMELINE=state[3],
                ROS_REJOIN_SOURCE_DSN=node_conninfo(cluster, HQ_STANDBY, settings=replica_settings),
                ROS_PRIMARY_HOST="toxiproxy",
                ROS_PRIMARY_PORT=str(proxy.port),
                ROS_SLOT_NAME=slot_name(HQ_PRIMARY),
                ROS_REJOIN_TIMEOUT_S=str(config.rejoin_timeout_s),
            )
            overrides = [
                part
                for key in env
                if key.startswith("ROS_REJOIN_")
                or key in ("ROS_PRIMARY_HOST", "ROS_PRIMARY_PORT", "ROS_SLOT_NAME")
                for part in ("-e", key)
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
                    HQ_PRIMARY,
                    "-s",
                ],
                env=env,
                script=(ROOT / "infra/postgres/rejoin-node.sh").read_text(),
            )
            if await role(conn) != state:
                raise RecoveryError("HQ source changed; target remains stopped")
            await require_async(conn)
            await command([*compose, "start", HQ_PRIMARY])
            return output, state

        output, state = await run_on(source, rebuild, region_id="hq", settings=limits)

        async with make_pool(
            node_conninfo(cluster, HQ_PRIMARY, settings=settings), settings
        ) as standby:
            deadline = time.monotonic() + config.rejoin_timeout_s
            while time.monotonic() < deadline:
                try:
                    current = await run_on(standby, role, settings=settings)
                    if not current[0]:
                        await command(["docker", "stop", "--time", "5", target["Id"]])
                        raise RecoveryError("recovered HQ is writable; stopped for inspection")

                    async def streaming(conn):
                        return await (
                            await conn.execute(
                                load_query("coordinator/rejoin_streaming"),
                                {"slot": slot_name(HQ_PRIMARY)},
                            )
                        ).fetchone()

                    if (
                        current[0]
                        and current[1] == state[1]
                        and await run_on(source, streaming, settings=settings) == (True,)
                    ):
                        await run_on(source, require_async, region_id="hq", settings=settings)
                        return output + "hq-primary rejoined as standby of hq-standby\n"
                except RegionUnreachableError:
                    pass
                await asyncio.sleep(0.2)
        await command(["docker", "stop", "--time", "5", target["Id"]])
        raise RecoveryError("HQ standby did not become ready; old node stopped for inspection")

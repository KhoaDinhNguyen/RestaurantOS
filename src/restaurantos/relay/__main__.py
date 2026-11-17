"""Run the startup checks for a regional relay or the HQ worker."""

import asyncio
import sys

from pydantic import ValidationError

from restaurantos.common.cluster import Cluster, load_cluster
from restaurantos.common.config import Settings, get_settings
from restaurantos.common.db import find_local_primary, make_pool, node_conninfo
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.outbox import read_pending


async def _check_region(
    cluster: Cluster, region_id: str, settings: Settings, relay: RelaySettings
) -> str:
    node = await find_local_primary(cluster, region_id, settings=settings)
    if node is None:
        raise RegionUnreachableError("no writable local node", region_id=region_id)

    conninfo = node_conninfo(cluster, node, direct=True, settings=settings)
    async with make_pool(conninfo, settings) as pool:
        events = await read_pending(pool, region_id, relay.batch_size, settings=settings)
    return f"{region_id} relay startup check read {len(events)} pending events from {node}."


def main() -> int:
    try:
        relay = RelaySettings()
        settings = get_settings()
        region_id = settings.region_id
        if relay.role == "hq":
            if settings.client != "hq":
                raise ValueError("the HQ worker requires ROS_CLIENT=hq")
            print("HQ worker configuration is valid.")
            return 0

        cluster = load_cluster(settings.cluster_file)
        if region_id is None or region_id not in cluster.region_ids:
            raise ValueError("set ROS_REGION_ID to a region in the cluster configuration")
        if settings.client != f"relay-{region_id}":
            raise ValueError(f"the region relay requires ROS_CLIENT=relay-{region_id}")
    except ValidationError as exc:
        print(f"Relay configuration error: {exc.errors(include_input=False)}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Relay configuration error: {exc}", file=sys.stderr)
        return 1

    try:
        print(asyncio.run(_check_region(cluster, region_id, settings, relay)))
    except RegionUnreachableError as exc:
        print(f"{region_id} relay startup check failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

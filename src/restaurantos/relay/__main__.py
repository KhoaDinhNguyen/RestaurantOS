"""Start the configured relay role."""

import asyncio
import logging
import sys

from pydantic import ValidationError

from restaurantos.common.cluster import load_cluster
from restaurantos.common.config import get_settings
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.worker import run_region_relay


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

    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run_region_relay(cluster, region_id, settings, relay))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Start the configured relay role."""

import asyncio
import logging
import sys
from importlib import import_module

from pydantic import ValidationError

from restaurantos.common.cluster import load_cluster
from restaurantos.common.config import get_settings
from restaurantos.relay.config import RelaySettings
from restaurantos.relay.holding import run_hq_worker
from restaurantos.relay.worker import run_region_relay


def main() -> int:
    try:
        relay = RelaySettings()
        settings = get_settings()
        region_id = settings.region_id
        if relay.role == "hq" and settings.client != "hq":
            raise ValueError("the HQ worker requires ROS_CLIENT=hq")

        cluster = load_cluster(settings.cluster_file)
        if relay.role == "hq":
            region = import_module("restaurantos.region")
            place_order = getattr(region, "place_order", None)
            if not callable(place_order):
                raise ValueError(
                    "HQ holding-queue delivery needs WP1: restaurantos.region.place_order"
                )
            apply_menu_version = getattr(region, "apply_menu_version", None)
            if not callable(apply_menu_version):
                raise ValueError(
                    "HQ menu publishing needs WP1: restaurantos.region.apply_menu_version"
                )
        else:
            if region_id is None or region_id not in cluster.region_ids:
                raise ValueError("set ROS_REGION_ID to a region in the cluster configuration")
            if settings.client != f"relay-{region_id}":
                raise ValueError(f"the region relay requires ROS_CLIENT=relay-{region_id}")
    except ValidationError as exc:
        print(f"Relay configuration error: {exc.errors(include_input=False)}", file=sys.stderr)
        return 1
    except (ImportError, OSError, ValueError) as exc:
        print(f"Relay configuration error: {exc}", file=sys.stderr)
        return 1

    logging.basicConfig(level=logging.INFO)
    try:
        if relay.role == "hq":
            asyncio.run(run_hq_worker(cluster, settings, relay, place_order, apply_menu_version))
        else:
            asyncio.run(run_region_relay(cluster, region_id, settings, relay))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Run the RestaurantOS gateway or edge API."""

import uvicorn

from restaurantos.common.cluster import load_cluster, service_port
from restaurantos.common.config import get_settings


def main() -> None:
    settings = get_settings()
    cluster = load_cluster(settings.cluster_file)

    if settings.mode == "edge":
        if settings.region_id is None:
            raise ValueError("ROS_REGION_ID is required in edge mode")
        service = f"edge-{settings.region_id}"
    else:
        service = "gateway"

    uvicorn.run(
        "restaurantos.gateway.app:app",
        # on the host, loopback only: 0.0.0.0 would put the gateway and
        # every edge API on whatever network the laptop is on (the same
        # reason the architecture publishes every Docker port on
        # 127.0.0.1, Section 4 / decision D16); inside compose, 0.0.0.0
        # so the published port reaches the container
        host="127.0.0.1" if settings.network == "host" else "0.0.0.0",
        port=service_port(cluster, service),
    )


if __name__ == "__main__":
    main()

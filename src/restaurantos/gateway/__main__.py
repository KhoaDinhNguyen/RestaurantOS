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
        host="0.0.0.0",
        port=service_port(cluster, service),
    )


if __name__ == "__main__":
    main()

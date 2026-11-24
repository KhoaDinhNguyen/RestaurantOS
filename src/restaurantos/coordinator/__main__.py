"""Start the coordinator on the documented service port."""

import uvicorn

from restaurantos.common.cluster import load_cluster, service_port
from restaurantos.common.config import get_settings


def main() -> None:
    settings = get_settings()
    if settings.client != "coordinator":
        raise ValueError("the coordinator requires ROS_CLIENT=coordinator")
    uvicorn.run(
        "restaurantos.coordinator.app:app",
        # loopback in host mode (architecture Section 4 / decision D16),
        # 0.0.0.0 inside compose so the published port reaches it
        host="127.0.0.1" if settings.network == "host" else "0.0.0.0",
        port=service_port(load_cluster(settings.cluster_file), "coordinator"),
    )


if __name__ == "__main__":
    main()

"""Edge API behavior against a real multi-region cluster.

Run locally after bootstrapping at least two regions and starting the r1 edge:

    uv run pytest -m cluster tests/cluster/gateway/test_edge.py

The r1 edge must be running with ROS_NETWORK=host, ROS_MODE=edge,
ROS_REGION_ID=r1, and ROS_CLIENT=edge-r1.
"""

import httpx
import psycopg
import pytest

from restaurantos.common.cluster import load_cluster, service_port

pytestmark = pytest.mark.cluster

HQ_DSN = "postgresql://ros:ros@127.0.0.1:5500/restaurantos"


@pytest.fixture
def cluster():
    cluster = load_cluster()

    if len(cluster.region_ids) < 2:
        pytest.skip("cluster needs at least two regions")

    return cluster


@pytest.fixture
def edge_client(cluster):
    port = service_port(cluster, "edge-r1")

    client = httpx.Client(
        base_url=f"http://127.0.0.1:{port}",
        timeout=5,
    )

    try:
        response = client.get("/health")
        response.raise_for_status()
    except httpx.HTTPError:
        client.close()
        pytest.skip(f"r1 edge is not answering on port {port}")

    if response.json().get("mode") != "edge":
        client.close()
        pytest.skip(f"service on port {port} is not running in edge mode")

    yield client
    client.close()


@pytest.fixture
def region_vendors():
    with psycopg.connect(HQ_DSN) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (sd.region_id)
                sd.region_id,
                v.vendor_id
            FROM vendors AS v
            INNER JOIN shard_directory AS sd
                ON v.geohash = sd.geohash
            ORDER BY sd.region_id, v.vendor_id
            """
        ).fetchall()

    vendors = dict(rows)

    if "r1" not in vendors or "r2" not in vendors:
        pytest.skip("cluster needs populated r1 and r2 regions")

    return vendors


def test_edge_serves_own_region_and_rejects_other_region(
    edge_client,
    region_vendors,
):
    r1_vendor = region_vendors["r1"]
    r2_vendor = region_vendors["r2"]

    own = edge_client.get(f"/api/v1/vendors/{r1_vendor}/menu")
    other = edge_client.get(f"/api/v1/vendors/{r2_vendor}/menu")

    assert own.status_code == 200
    assert own.json()

    assert other.status_code == 404
    assert other.json() == {"error": "not_found"}


def test_edge_only_serves_own_region_summary(edge_client):
    own = edge_client.get("/api/v1/regions/r1/summary?day=14")
    other = edge_client.get("/api/v1/regions/r2/summary?day=14")

    assert own.status_code == 200

    assert other.status_code == 404
    assert other.json() == {"error": "not_found"}


def test_hq_only_routes_are_not_exposed_on_edge(edge_client):
    regions = edge_client.get("/api/v1/regions")
    orders = edge_client.post("/api/v1/orders", json={})

    assert regions.status_code == 404
    assert orders.status_code == 404

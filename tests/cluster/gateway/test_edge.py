"""Edge API behavior against a real multi-region cluster.

Run locally after bootstrapping at least two regions and starting the r1 edge:

    uv run pytest -m cluster tests/cluster/gateway/test_edge.py

The r1 edge must be running with ROS_NETWORK=host, ROS_MODE=edge,
ROS_REGION_ID=r1, and ROS_CLIENT=edge-r1.
"""

from uuid import uuid4

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


@pytest.fixture
def r1_order_item(region_vendors):
    vendor_id = region_vendors["r1"]

    with psycopg.connect(HQ_DSN) as conn:
        row = conn.execute(
            """
            SELECT p.product_id
            FROM products AS p
            WHERE p.vendor_id = %s
            ORDER BY p.product_id
            LIMIT 1
            """,
            (vendor_id,),
        ).fetchone()

    if row is None:
        pytest.skip("r1 vendor has no products")

    return vendor_id, row[0]


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

    assert regions.status_code == 404


def test_edge_places_and_deduplicates_order(
    edge_client,
    r1_order_item,
):
    vendor_id, product_id = r1_order_item

    restocked = edge_client.post(f"/api/v1/vendors/{vendor_id}/restock")
    assert restocked.status_code == 200

    body = {
        "channel": "cluster-edge",
        "external_order_id": f"cluster-edge-{uuid4()}",
        "vendor_id": vendor_id,
        "customer_id": "cluster-test",
        "customer_geohash": "w21z7",
        "order_day": 14,
        "order_time": "13:00:00",
        "items": [
            {
                "product_id": product_id,
                "quantity": 1,
            }
        ],
    }

    first = edge_client.post("/api/v1/orders", json=body)
    second = edge_client.post("/api/v1/orders", json=body)

    assert first.status_code == 201
    assert second.status_code == 200

    first_body = first.json()
    second_body = second.json()

    assert first_body["duplicate"] is False
    assert second_body["duplicate"] is True
    assert first_body["order_id"] == second_body["order_id"]
    assert first_body["region_id"] == "r1"
    assert second_body["region_id"] == "r1"


def test_edge_rejects_order_for_other_region(
    edge_client,
    region_vendors,
):
    response = edge_client.post(
        "/api/v1/orders",
        json={
            "channel": "cluster-edge",
            "external_order_id": f"foreign-edge-{uuid4()}",
            "vendor_id": region_vendors["r2"],
            "customer_id": "cluster-test",
            "customer_geohash": "w21zt",
            "order_day": 14,
            "order_time": "13:10:00",
            "items": [
                {
                    "product_id": "not-local",
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 404
    assert response.json() == {"error": "not_found"}

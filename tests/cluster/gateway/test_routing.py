"""Gateway routing against a real multi-region cluster.

Run locally after bootstrapping at least two regions:

    uv run pytest -m cluster tests/cluster/gateway/test_routing.py

The Gateway must already be running on localhost:8000 with
ROS_NETWORK=host and ROS_CLIENT=gateway.
"""

import time

import httpx
import psycopg
import pytest

pytestmark = pytest.mark.cluster

GATEWAY_URL = "http://127.0.0.1:8000"
TOXIPROXY_URL = "http://127.0.0.1:8474"

R1_PROXY = "gateway__r1-primary"
HQ_DSN = "postgresql://ros:ros@127.0.0.1:5500/restaurantos"

SILENCE = {
    "type": "timeout",
    "attributes": {"timeout": 0},
}

LATENCY = {
    "type": "latency",
    "attributes": {
        "latency": 5000,
        "jitter": 0,
    },
}


def wait_for_status(
    client,
    path,
    expected,
    timeout_s=10.0,
):
    deadline = time.monotonic() + timeout_s

    while True:
        response = client.get(path)

        if response.status_code == expected:
            return response

        if time.monotonic() >= deadline:
            pytest.fail(f"{path} stayed at {response.status_code}; expected {expected}")

        time.sleep(0.25)


def add_fault(toxiproxy, fault):
    for stream in ("upstream", "downstream"):
        body = {
            "name": f"gateway_test_{fault['type']}_{stream}",
            "stream": stream,
            "toxicity": 1.0,
            **fault,
        }

        response = toxiproxy.post(
            f"/proxies/{R1_PROXY}/toxics",
            json=body,
        )
        response.raise_for_status()


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
        pytest.skip("cluster needs at least two populated regions")

    return vendors


@pytest.fixture
def gateway():
    client = httpx.Client(
        base_url=GATEWAY_URL,
        timeout=5,
    )

    try:
        response = client.get("/health")
        response.raise_for_status()
    except httpx.HTTPError:
        client.close()
        pytest.skip("Gateway is not answering on localhost:8000")

    yield client
    client.close()


@pytest.fixture
def toxiproxy():
    client = httpx.Client(
        base_url=TOXIPROXY_URL,
        timeout=5,
    )

    try:
        client.get("/version").raise_for_status()
    except httpx.HTTPError:
        client.close()
        pytest.skip("Toxiproxy is not answering on localhost:8474")

    client.post("/reset").raise_for_status()

    yield client

    client.post("/reset").raise_for_status()
    client.close()


def test_vendors_route_to_both_regions(
    gateway,
    toxiproxy,
    region_vendors,
):
    r1_vendor = region_vendors["r1"]
    r2_vendor = region_vendors["r2"]

    r1 = wait_for_status(
        gateway,
        f"/api/v1/vendors/{r1_vendor}/menu",
        200,
    )
    r2 = wait_for_status(
        gateway,
        f"/api/v1/vendors/{r2_vendor}/menu",
        200,
    )

    assert r1.json()
    assert r2.json()

    assert all(item["vendor_id"] == r1_vendor for item in r1.json())
    assert all(item["vendor_id"] == r2_vendor for item in r2.json())


def test_cut_region_does_not_affect_other_region(
    gateway,
    toxiproxy,
    region_vendors,
):
    r1_vendor = region_vendors["r1"]
    r2_vendor = region_vendors["r2"]

    response = toxiproxy.post(
        f"/proxies/{R1_PROXY}",
        json={"enabled": False},
    )
    response.raise_for_status()

    r1 = gateway.get(f"/api/v1/vendors/{r1_vendor}/menu")
    r2 = gateway.get(f"/api/v1/vendors/{r2_vendor}/menu")

    assert r1.status_code == 503
    assert r1.json() == {
        "error": "region_unreachable",
        "region_id": "r1",
    }

    assert r2.status_code == 200
    assert r2.json()


@pytest.mark.parametrize(
    "fault",
    [SILENCE, LATENCY],
    ids=["silence", "latency"],
)
def test_faulty_region_read_returns_503_within_deadline(
    gateway,
    toxiproxy,
    region_vendors,
    fault,
):
    r1_vendor = region_vendors["r1"]

    # Make sure the route is healthy before injecting the fault.
    healthy = wait_for_status(
        gateway,
        f"/api/v1/vendors/{r1_vendor}/menu",
        200,
    )
    assert healthy.status_code == 200

    add_fault(toxiproxy, fault)

    started = time.monotonic()

    response = gateway.get(f"/api/v1/vendors/{r1_vendor}/menu")

    elapsed = time.monotonic() - started

    assert response.status_code == 503
    assert response.json() == {
        "error": "region_unreachable",
        "region_id": "r1",
    }

    # Default ROS_REQUEST_DEADLINE_S is 3 seconds.
    # Give a small scheduling/network margin.
    assert elapsed < 3.5, f"Gateway took {elapsed:.2f}s to fail the regional request"

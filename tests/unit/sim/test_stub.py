"""The stub: contract shape, and the determinism that lets the run log
be compared byte for byte."""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from datetime import time as dt_time

import httpx
import pytest

from restaurantos.common.simclock import to_sim_seconds
from restaurantos.sim.stub import STUB_EPOCH, make_stub_app

ORDER = {
    "channel": "dhrd",
    "external_order_id": "0042",
    "vendor_id": "v1",
    "customer_id": "c1",
    "customer_geohash": "w21z7",
    "order_day": 14,
    "order_time": "12:03:29",
    "items": [{"product_id": "p1", "quantity": 2}],
}


@pytest.fixture
def client():
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_stub_app()), base_url="http://stub.test"
    )


async def test_answers_201_on_both_paths(client):
    for path in ("/orders", "/api/v1/orders"):
        response = await client.post(path, json=ORDER)
        assert response.status_code == 201, response.text
        body = response.json()
        assert body["outcome"] == "accepted"
        assert body["region_id"] == "r-stub"
        assert body["menu_version"] == 1


async def test_body_is_validated_against_the_real_model(client):
    bad = {**ORDER, "items": []}  # a request needs at least one line
    response = await client.post("/orders", json=bad)
    assert response.status_code == 422


async def test_response_fields_come_from_the_request(client):
    response = await client.post("/orders", json=ORDER)
    body = response.json()
    # the three gateway timestamps are the order's own simulated time,
    # on a fixed epoch: same request in, same answer out
    expected = STUB_EPOCH + timedelta(seconds=to_sim_seconds(14, dt_time(12, 3, 29)))
    for field in ("received_at", "committed_at", "acked_at"):
        stamped = datetime.fromisoformat(body[field].replace("Z", "+00:00"))
        assert stamped == expected, field
    assert body["order_id"]  # stable uuid5 of channel + external id
    assert body["order_day"] == 14


async def test_same_request_gives_the_same_answer(client):
    first = (await client.post("/orders", json=ORDER)).json()
    second = (await client.post("/orders", json=ORDER)).json()
    assert first == second


async def test_delay_option_slows_answers():

    app = make_stub_app(delay_seconds=0.1)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://stub.test"
    ) as client:
        before = time.monotonic()
        await client.post("/orders", json=ORDER)
        assert time.monotonic() - before >= 0.1


# --- the status and restock routes (playback extras) -------------------------


async def test_status_change_answers_201_on_both_paths(client):
    order_id = "12345678-1234-5678-1234-567812345678"
    body = {"event_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "to_status": "accepted"}
    for path in (
        f"/vendors/v1/orders/{order_id}/status",
        f"/api/v1/vendors/v1/orders/{order_id}/status",
    ):
        response = await client.post(path, json=body)
        assert response.status_code == 201, response.text
        answer = response.json()
        assert answer == {
            "order_id": order_id,
            "status": "accepted",
            "applied": True,
            "duplicate": False,
            "conflict": False,
        }


async def test_status_change_validates_the_real_model(client):
    # to_status must be an OrderStatus; event_id must be a UUID
    for bad in (
        {"event_id": "not-a-uuid", "to_status": "accepted"},
        {"event_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "to_status": "cooked"},
    ):
        response = await client.post(
            "/vendors/v1/orders/12345678-1234-5678-1234-567812345678/status", json=bad
        )
        assert response.status_code == 422, bad


async def test_status_change_is_a_pure_function_of_the_request(client):
    """Review round 2, item 14: both answers are kept - each is a 201 -
    and the bodies are compared, not just assumed equal."""
    path = "/vendors/v1/orders/12345678-1234-5678-1234-567812345678/status"
    body = {"event_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "to_status": "preparing"}
    first_response = await client.post(path, json=body)
    second_response = await client.post(path, json=body)
    assert first_response.status_code == 201
    assert second_response.status_code == 201
    assert first_response.json() == second_response.json()


async def test_restock_answers_the_decided_shape_on_both_paths(client):
    """api-gaps 1.2: 200 with {vendor_id, region_id, counters_restocked,
    products_back_on}. The stub has no stock, so the counts are zero."""
    for path in ("/vendors/v1/restock", "/api/v1/vendors/v1/restock"):
        response = await client.post(path)
        assert response.status_code == 200, response.text
        assert response.json() == {
            "vendor_id": "v1",
            "region_id": "r-stub",
            "counters_restocked": 0,
            "products_back_on": 0,
        }


async def test_delay_slows_the_new_routes_too():
    """Review round 2, item 14: the delay applies to the status route
    and to the restock route, each timed separately with its own
    expected status."""
    app = make_stub_app(delay_seconds=0.1)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://stub.test"
    ) as client:
        before = time.monotonic()
        status_response = await client.post(
            "/vendors/v1/orders/12345678-1234-5678-1234-567812345678/status",
            json={"event_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "to_status": "preparing"},
        )
        assert status_response.status_code == 201
        assert time.monotonic() - before >= 0.1

        before = time.monotonic()
        restock_response = await client.post("/vendors/v1/restock")
        assert restock_response.status_code == 200
        assert time.monotonic() - before >= 0.1

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

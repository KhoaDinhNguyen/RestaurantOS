from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

from tests.unit.gateway.fakes import place_order_accepted

from restaurantos.common.models import (
    OrderItemIn,
    OrderOutcome,
    OrderRequest,
    OrderResult,
    OrderStatus,
    RefusalReason,
)
from restaurantos.gateway import orders


async def test_place_region_order(monkeypatch) -> None:
    connection = AsyncMock()

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(orders, "run_on", fake_run_on)

    request = OrderRequest(
        channel="dhrd",
        external_order_id="123",
        vendor_id="v001",
        customer_id="c001",
        customer_geohash="w21z7",
        order_day=14,
        order_time="12:03:29",
        items=[
            OrderItemIn(
                product_id="p003",
                quantity=1,
            )
        ],
    )

    received_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)

    result = await orders.place_region_order(
        object(),
        request,
        "r1",
        3,
        received_at,
        place_order_accepted,
    )

    assert result.outcome.value == "accepted"
    assert result.duplicate is False
    assert result.status.value == "received"
    assert result.menu_version == 1
    assert result.committed_at is not None


async def test_park_order(monkeypatch) -> None:
    connection = AsyncMock()

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(orders, "run_on", fake_run_on)

    request = OrderRequest(
        channel="dhrd",
        external_order_id="123",
        vendor_id="v001",
        customer_id="c001",
        customer_geohash="w21z7",
        order_day=14,
        order_time="12:03:29",
        items=[
            OrderItemIn(
                product_id="p003",
                quantity=1,
            )
        ],
    )

    received_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)

    await orders.park_order(
        object(),
        request,
        "r1",
        received_at,
    )

    connection.execute.assert_awaited_once()

    sql, params = connection.execute.await_args.args

    assert "holding_queue" in sql
    assert params["region_id"] == "r1"
    assert params["channel"] == "dhrd"
    assert params["external_order_id"] == "123"
    assert params["received_at"] == received_at

    assert params["request"].obj["vendor_id"] == "v001"
    assert params["request"].obj["items"] == [
        {
            "product_id": "p003",
            "quantity": 1,
        }
    ]


def test_order_response_for_new_order() -> None:
    received_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    committed_at = datetime(2026, 11, 15, 12, 0, 1, tzinfo=UTC)
    acked_at = datetime(2026, 11, 15, 12, 0, 2, tzinfo=UTC)

    result = OrderResult(
        outcome=OrderOutcome.ACCEPTED,
        duplicate=False,
        order_id=uuid4(),
        order_day=14,
        status=OrderStatus.RECEIVED,
        menu_version=1,
        committed_at=committed_at,
    )

    response = orders.order_response(
        result,
        "r1",
        received_at,
        acked_at,
    )

    assert orders.order_status_code(result) == 201
    assert response.region_id == "r1"
    assert response.received_at == received_at
    assert response.committed_at == committed_at
    assert response.acked_at == acked_at
    assert response.queued is False


def test_duplicate_order_returns_200() -> None:
    result = OrderResult(
        outcome=OrderOutcome.ACCEPTED,
        duplicate=True,
    )

    assert orders.order_status_code(result) == 200


def test_refused_order_returns_409() -> None:
    result = OrderResult(
        outcome=OrderOutcome.REFUSED,
        refusal_reason=RefusalReason.OUT_OF_STOCK,
    )

    assert orders.order_status_code(result) == 409


def test_queued_order_response() -> None:
    received_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)

    response = orders.queued_order_response(
        "r1",
        received_at,
    )

    assert response.region_id == "r1"
    assert response.received_at == received_at
    assert response.queued is True
    assert response.outcome is None
    assert response.committed_at is None


async def test_get_order_status(monkeypatch) -> None:
    order_id = uuid4()
    changed_at = datetime(2026, 11, 15, 12, 1, tzinfo=UTC)
    received_at = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    committed_at = datetime(2026, 11, 15, 12, 0, 1, tzinfo=UTC)

    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchone.return_value = (
        order_id,
        14,
        "v001",
        "received",
        changed_at,
        1,
        received_at,
        committed_at,
    )
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(orders, "run_on", fake_run_on)

    result = await orders.get_order_status(
        object(),
        "v001",
        order_id,
        "r1",
    )

    assert result["order_id"] == order_id
    assert result["vendor_id"] == "v001"
    assert result["status"] == "received"
    assert result["order_day"] == 14
    assert result["menu_version"] == 1


async def test_get_vendor_orders(monkeypatch) -> None:
    order_id = uuid4()
    order_time = datetime.strptime("12:03:29", "%H:%M:%S").time()
    changed_at = datetime(2026, 11, 15, 12, 4, tzinfo=UTC)

    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchall.return_value = [
        (
            order_id,
            14,
            "v001",
            "accepted",
            order_time,
            changed_at,
            1,
            changed_at,
            changed_at,
        )
    ]
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(orders, "run_on", fake_run_on)

    result = await orders.get_vendor_orders(
        object(),
        "v001",
        14,
        "accepted",
        "r1",
    )

    assert len(result) == 1
    assert result[0]["order_id"] == order_id
    assert result[0]["vendor_id"] == "v001"
    assert result[0]["status"] == "accepted"

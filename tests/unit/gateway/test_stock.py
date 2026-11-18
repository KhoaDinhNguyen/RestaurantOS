from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.errors import UnknownVendorError
from restaurantos.gateway import stock


async def test_get_vendor_stock(monkeypatch) -> None:
    updated_at = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)

    connection = AsyncMock()

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = (1,)

    stock_cursor = AsyncMock()
    stock_cursor.fetchall.return_value = [
        (
            "dish",
            "d001",
            8,
            20,
            updated_at,
            "Dumplings",
            None,
        ),
        (
            "ingredient",
            "garlic",
            12,
            35,
            updated_at,
            "garlic",
            "portion",
        ),
    ]

    connection.execute.side_effect = [
        vendor_cursor,
        stock_cursor,
    ]

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(stock, "run_on", fake_run_on)

    result = await stock.get_vendor_stock(
        object(),
        "v001",
        "r1",
    )

    assert result == [
        {
            "item_kind": "dish",
            "item_id": "d001",
            "name": "Dumplings",
            "unit": None,
            "on_hand": 8,
            "par_level": 20,
            "updated_at": updated_at,
        },
        {
            "item_kind": "ingredient",
            "item_id": "garlic",
            "name": "garlic",
            "unit": "portion",
            "on_hand": 12,
            "par_level": 35,
            "updated_at": updated_at,
        },
    ]


async def test_get_vendor_stock_allows_no_counters(monkeypatch) -> None:
    connection = AsyncMock()

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = (1,)

    stock_cursor = AsyncMock()
    stock_cursor.fetchall.return_value = []

    connection.execute.side_effect = [
        vendor_cursor,
        stock_cursor,
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(stock, "run_on", fake_run_on)

    result = await stock.get_vendor_stock(
        object(),
        "v001",
        "r1",
    )

    assert result == []


async def test_get_vendor_stock_rejects_unknown_vendor(monkeypatch) -> None:
    connection = AsyncMock()

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = None

    connection.execute.return_value = vendor_cursor

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(stock, "run_on", fake_run_on)

    with pytest.raises(UnknownVendorError):
        await stock.get_vendor_stock(
            object(),
            "missing",
            "r1",
        )

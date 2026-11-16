from unittest.mock import AsyncMock

import pytest

from restaurantos.common.errors import UnknownVendorError
from restaurantos.gateway import menu


async def test_get_vendor_menu(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchall.return_value = [
        ("v001", "p003", "Menu item 003", 2, 1, None, True),
        ("v001", "p006", "Menu item 006", 11, 1, "manual", False),
    ]
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(menu, "run_on", fake_run_on)

    result = await menu.get_vendor_menu(
        object(),
        "v001",
        "r1",
    )

    assert result == [
        {
            "vendor_id": "v001",
            "product_id": "p003",
            "name": "Menu item 003",
            "price_units": 2,
            "menu_version": 1,
            "reason": None,
            "available": True,
        },
        {
            "vendor_id": "v001",
            "product_id": "p006",
            "name": "Menu item 006",
            "price_units": 11,
            "menu_version": 1,
            "reason": "manual",
            "available": False,
        },
    ]


async def test_get_vendor_menu_rejects_unknown_vendor(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchall.return_value = []
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(menu, "run_on", fake_run_on)

    with pytest.raises(UnknownVendorError):
        await menu.get_vendor_menu(
            object(),
            "missing",
            "r1",
        )

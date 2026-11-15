from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from restaurantos.common.errors import UnknownVendorError
from restaurantos.gateway import directory
from restaurantos.gateway.directory import DirectoryCache, RegionRoute


async def test_get_regions(monkeypatch) -> None:
    updated_at = datetime(2026, 11, 13, tzinfo=UTC)

    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchall.return_value = [
        (
            "r1",
            1,
            "r1-primary",
            "r1-standby",
            1,
            updated_at,
            ["w21z7", "w21z8"],
        )
    ]
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(directory, "run_on", fake_run_on)

    result = await directory.get_regions(object())

    assert result == [
        {
            "region_id": "r1",
            "epoch": 1,
            "primary_node": "r1-primary",
            "standby_node": "r1-standby",
            "menu_version": 1,
            "updated_at": updated_at,
            "geohashes": ["w21z7", "w21z8"],
        }
    ]


def test_directory_routes_vendor() -> None:
    cache = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
    )

    route = cache.route_vendor("v001")

    assert route.region_id == "r1"
    assert route.epoch == 1
    assert route.primary_node == "r1-primary"


def test_directory_rejects_unknown_vendor() -> None:
    cache = DirectoryCache(
        vendor_cells={},
        cell_routes={},
    )

    with pytest.raises(UnknownVendorError):
        cache.route_vendor("missing")


async def test_directory_refresh(monkeypatch) -> None:
    connection = AsyncMock()

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchall.return_value = [
        ("v001", "w21z7"),
        ("v002", "w21z8"),
    ]

    region_cursor = AsyncMock()
    region_cursor.fetchall.return_value = [
        (
            "r1",
            3,
            "r1-standby",
            "r1-primary",
            1,
            None,
            ["w21z7", "w21z8"],
        )
    ]

    connection.execute.side_effect = [
        vendor_cursor,
        region_cursor,
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(directory, "run_on", fake_run_on)

    cache = DirectoryCache(
        vendor_cells={},
        cell_routes={},
    )

    await cache.refresh(object())

    assert cache.vendor_cells == {
        "v001": "w21z7",
        "v002": "w21z8",
    }

    assert cache.route_vendor("v001") == RegionRoute(
        region_id="r1",
        epoch=3,
        primary_node="r1-standby",
    )


def test_vendors_in_same_cell_share_route() -> None:
    route = RegionRoute(
        region_id="r1",
        epoch=1,
        primary_node="r1-primary",
    )

    cache = DirectoryCache(
        vendor_cells={
            "v001": "w21z7",
            "v002": "w21z7",
        },
        cell_routes={"w21z7": route},
    )

    assert cache.route_vendor("v001") == route
    assert cache.route_vendor("v002") == route

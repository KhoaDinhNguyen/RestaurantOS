from datetime import UTC, datetime
from unittest.mock import AsyncMock

from restaurantos.gateway import directory


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

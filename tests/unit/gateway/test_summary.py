from datetime import UTC, datetime
from unittest.mock import AsyncMock

from restaurantos.gateway import summary


async def test_get_region_summary(monkeypatch) -> None:
    data_as_of = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)

    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchone.return_value = (
        "r1",  # 0 region_id
        1,  # 1 epoch
        1,  # 2 menu_version
        3,  # 3 total
        0,  # 4 received
        3,  # 5 accepted
        0,  # 6 preparing
        0,  # 7 ready
        0,  # 8 picked_up
        0,  # 9 canceled
        1,  # 10 refused
        42,  # 11 sales_units
        0,  # 12 products_unavailable
        0,  # 13 outbox_backlog
        14,  # 14 order_day
        1,  # 15 vendors
        data_as_of,  # 16 data_as_of
    )
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(summary, "run_on", fake_run_on)

    result = await summary.get_region_summary(
        object(),
        "r1",
        14,
    )

    assert result["region_id"] == "r1"
    assert result["day"] == 14
    assert result["epoch"] == 1
    assert result["menu_version"] == 1
    assert result["vendors"] == 1
    assert result["orders"]["total"] == 3
    assert result["orders"]["accepted"] == 3
    assert result["refused"] == 1
    assert result["sales_units"] == 42
    assert result["products_unavailable"] == 0
    assert result["outbox_backlog"] == 0
    assert result["data_as_of"] == data_as_of

from unittest.mock import AsyncMock

from restaurantos.gateway import vendors


def test_search_pattern() -> None:
    assert vendors.search_pattern(None) is None
    assert vendors.search_pattern("noodles") == "%noodles%"
    assert vendors.search_pattern("50%_off\\today") == "%50\\%\\_off\\\\today%"


async def test_search_vendors(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchall.return_value = [
        ("v001", None, "w21z7", "noodles"),
        ("v002", "c001", "w21z7", "chinese"),
    ]
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(vendors, "run_on", fake_run_on)

    result = await vendors.search_vendors(
        object(),
        "w21z7",
        "noodle",
        "r1",
    )

    assert result == [
        {
            "vendor_id": "v001",
            "chain_id": None,
            "geohash": "w21z7",
            "primary_cuisine": "noodles",
        },
        {
            "vendor_id": "v002",
            "chain_id": "c001",
            "geohash": "w21z7",
            "primary_cuisine": "chinese",
        },
    ]

    connection.execute.assert_awaited_once()

    params = connection.execute.await_args.args[1]

    assert params == {
        "geohash": "w21z7",
        "pattern": "%noodle%",
    }


async def test_search_vendors_without_query(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchall.return_value = []
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(vendors, "run_on", fake_run_on)

    result = await vendors.search_vendors(
        object(),
        "w21z7",
        None,
        "r1",
    )

    assert result == []

    params = connection.execute.await_args.args[1]
    assert params["pattern"] is None

from unittest.mock import AsyncMock

from restaurantos.gateway import dishes


async def test_search_dishes(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchall.return_value = [
        ("d001", "Laksa", "Laksa Special", "global", 42),
        ("d002", None, "Seafood Laksa", "chain:c001", 8),
    ]
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] is None
        return await work(connection)

    monkeypatch.setattr(dishes, "run_on", fake_run_on)

    result = await dishes.search_dishes(
        object(),
        "laksa",
        20,
    )

    assert result == [
        {
            "dish_id": "d001",
            "dish_name": "Laksa",
            "example_name": "Laksa Special",
            "scope": "global",
            "n_vendors": 42,
        },
        {
            "dish_id": "d002",
            "dish_name": None,
            "example_name": "Seafood Laksa",
            "scope": "chain:c001",
            "n_vendors": 8,
        },
    ]

    params = connection.execute.await_args.args[1]

    assert params == {
        "pattern": "%laksa%",
        "row_limit": 20,
    }


async def test_get_dish(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchone.return_value = (
        "d001",
        "Laksa",
        "Laksa Special",
        "global",
        42,
    )
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(dishes, "run_on", fake_run_on)

    result = await dishes.get_dish(
        object(),
        "d001",
        region_id="r1",
    )

    assert result == {
        "dish_id": "d001",
        "dish_name": "Laksa",
        "example_name": "Laksa Special",
        "scope": "global",
        "n_vendors": 42,
    }


async def test_get_dish_returns_none_when_unknown(monkeypatch) -> None:
    connection = AsyncMock()
    cursor = AsyncMock()

    cursor.fetchone.return_value = None
    connection.execute.return_value = cursor

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(dishes, "run_on", fake_run_on)

    result = await dishes.get_dish(
        object(),
        "missing",
    )

    assert result is None

from unittest.mock import AsyncMock

from restaurantos.gateway import restock as gateway_restock
from restaurantos.region import RestockResult


async def test_restock_region_vendor(monkeypatch) -> None:
    connection = AsyncMock()

    async def fake_restock(
        conn,
        vendor_id,
        epoch,
    ):
        assert conn is connection
        assert vendor_id == "v001"
        assert epoch == 3

        return RestockResult(
            counters_restocked=14,
            products_back_on=3,
        )

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(gateway_restock, "run_on", fake_run_on)

    result = await gateway_restock.restock_region_vendor(
        object(),
        "v001",
        "r1",
        3,
        fake_restock,
    )

    assert result == RestockResult(
        counters_restocked=14,
        products_back_on=3,
    )

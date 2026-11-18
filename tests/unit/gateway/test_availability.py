from unittest.mock import AsyncMock

from restaurantos.gateway import availability
from restaurantos.region import ProductState


async def test_set_region_availability(monkeypatch) -> None:
    connection = AsyncMock()

    async def fake_set_availability(
        conn,
        vendor_id,
        product_id,
        available,
        epoch,
    ):
        assert conn is connection
        assert vendor_id == "v001"
        assert product_id == "p001"
        assert available is False
        assert epoch == 3

        return ProductState(
            available=False,
            reason="manual",
        )

    async def fake_run_on(pool, work, **kwargs):
        assert kwargs["region_id"] == "r1"
        return await work(connection)

    monkeypatch.setattr(availability, "run_on", fake_run_on)

    result = await availability.set_region_availability(
        object(),
        "v001",
        "p001",
        False,
        "r1",
        3,
        fake_set_availability,
    )

    assert result == ProductState(
        available=False,
        reason="manual",
    )

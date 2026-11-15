from unittest.mock import AsyncMock, Mock

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import Settings
from restaurantos.gateway import pools


async def test_region_pool_is_reused(monkeypatch) -> None:
    pool = AsyncMock()

    make_pool = Mock(return_value=pool)
    node_conninfo = Mock(return_value="test-conninfo")

    monkeypatch.setattr(pools, "make_pool", make_pool)
    monkeypatch.setattr(pools, "node_conninfo", node_conninfo)

    manager = pools.RegionPools(
        Cluster(regions=1),
        Settings(network="host"),
    )

    first = await manager.get("r1-primary")
    second = await manager.get("r1-primary")

    assert first is pool
    assert second is pool
    assert make_pool.call_count == 1
    pool.open.assert_awaited_once()


async def test_region_pools_close_all(monkeypatch) -> None:
    first_pool = AsyncMock()
    second_pool = AsyncMock()

    make_pool = Mock(side_effect=[first_pool, second_pool])

    monkeypatch.setattr(pools, "make_pool", make_pool)
    monkeypatch.setattr(pools, "node_conninfo", Mock(return_value="test-conninfo"))

    manager = pools.RegionPools(
        Cluster(regions=1),
        Settings(network="host"),
    )

    await manager.get("r1-primary")
    await manager.get("r1-standby")
    await manager.close()

    first_pool.close.assert_awaited_once()
    second_pool.close.assert_awaited_once()

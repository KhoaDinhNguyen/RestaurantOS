from unittest.mock import AsyncMock

import pytest

from restaurantos.common.config import Settings
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.gateway import edge


async def test_edge_primary_refresh(monkeypatch) -> None:
    cluster = object()
    settings = Settings(_env_file=None)

    find_primary = AsyncMock(return_value="r1-primary")
    monkeypatch.setattr(edge, "find_local_primary", find_primary)

    primary = edge.EdgePrimary(
        cluster,
        "r1",
        settings,
    )

    result = await primary.refresh()

    assert result == "r1-primary"
    assert primary.current == "r1-primary"

    find_primary.assert_awaited_once_with(
        cluster,
        "r1",
        None,
        settings,
    )


async def test_edge_primary_passes_current_node(monkeypatch) -> None:
    cluster = object()
    settings = Settings(_env_file=None)

    find_primary = AsyncMock(
        side_effect=[
            "r1-primary",
            "r1-standby",
        ]
    )
    monkeypatch.setattr(edge, "find_local_primary", find_primary)

    primary = edge.EdgePrimary(
        cluster,
        "r1",
        settings,
    )

    await primary.refresh()
    await primary.refresh()

    assert primary.current == "r1-standby"

    assert find_primary.await_args_list[1].args == (
        cluster,
        "r1",
        "r1-primary",
        settings,
    )


async def test_edge_primary_gets_current_pool() -> None:
    cluster = object()
    settings = Settings(_env_file=None)

    primary = edge.EdgePrimary(
        cluster,
        "r1",
        settings,
    )
    primary.current = "r1-primary"

    pools = AsyncMock()
    expected_pool = object()
    pools.get.return_value = expected_pool

    result = await primary.get_pool(pools)

    assert result is expected_pool
    pools.get.assert_awaited_once_with("r1-primary")


async def test_edge_primary_without_current_node_is_unreachable() -> None:
    cluster = object()
    settings = Settings(_env_file=None)

    primary = edge.EdgePrimary(
        cluster,
        "r1",
        settings,
    )

    pools = AsyncMock()

    with pytest.raises(RegionUnreachableError) as exc_info:
        await primary.get_pool(pools)

    assert exc_info.value.region_id == "r1"
    pools.get.assert_not_awaited()


async def test_run_edge_refreshes_after_region_unreachable() -> None:
    primary = AsyncMock()
    primary.current = "r1-primary"

    pools = AsyncMock()
    failed_pool = object()
    pools.get.return_value = failed_pool

    async def work(pool):
        assert pool is failed_pool
        raise RegionUnreachableError(region_id="r1")

    with pytest.raises(RegionUnreachableError):
        await edge.run_edge(
            primary,
            pools,
            work,
        )

    pools.get.assert_awaited_once_with("r1-primary")
    pools.drop_if_same.assert_awaited_once_with(
        "r1-primary",
        failed_pool,
    )
    primary.refresh.assert_awaited_once_with()


async def test_run_edge_refreshes_after_stale_epoch() -> None:
    from restaurantos.common.errors import StaleEpochError

    primary = AsyncMock()
    primary.current = "r1-primary"

    pools = AsyncMock()
    failed_pool = object()
    pools.get.return_value = failed_pool

    async def work(pool):
        assert pool is failed_pool
        raise StaleEpochError()

    with pytest.raises(StaleEpochError):
        await edge.run_edge(
            primary,
            pools,
            work,
        )

    pools.get.assert_awaited_once_with("r1-primary")
    pools.drop_if_same.assert_awaited_once_with(
        "r1-primary",
        failed_pool,
    )
    primary.refresh.assert_awaited_once_with()


async def test_run_edge_does_not_refresh_after_success() -> None:
    primary = AsyncMock()
    primary.current = "r1-primary"

    pools = AsyncMock()
    expected_pool = object()
    pools.get.return_value = expected_pool

    async def work(pool):
        assert pool is expected_pool
        return "ok"

    result = await edge.run_edge(
        primary,
        pools,
        work,
    )

    assert result == "ok"
    pools.get.assert_awaited_once_with("r1-primary")
    pools.drop_if_same.assert_not_awaited()
    primary.refresh.assert_not_awaited()

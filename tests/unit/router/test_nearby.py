from unittest.mock import AsyncMock

from restaurantos.common.errors import RegionUnreachableError
from restaurantos.gateway.directory import DirectoryCache, RegionRoute
from restaurantos.gateway.router import nearby
from restaurantos.gateway.router.nearby import (
    cells_by_region,
    nearby_cells,
)


def route(region_id: str, node: str) -> RegionRoute:
    return RegionRoute(
        region_id=region_id,
        epoch=1,
        primary_node=node,
    )


def test_nearby_cells_returns_center_and_eight_neighbours() -> None:
    cells = nearby_cells("w21z7")

    assert len(cells) == 9
    assert len({cell.geohash for cell in cells}) == 9

    center = next(cell for cell in cells if cell.geohash == "w21z7")
    assert center.distance_m == 0.0

    assert all(cell.distance_m >= 0 for cell in cells)


def test_cells_by_region_groups_known_cells() -> None:
    cells = nearby_cells("w21z7")

    r1 = route("r1", "r1-primary")
    r2 = route("r2", "r2-primary")

    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={
            "w21z7": r1,
            "w21z6": r1,
            "w21ze": r2,
        },
        region_routes={
            "r1": r1,
            "r2": r2,
        },
    )

    grouped = cells_by_region(cells, directory)

    assert set(grouped) == {"r1", "r2"}

    r1_route, r1_cells = grouped["r1"]
    r2_route, r2_cells = grouped["r2"]

    assert r1_route == r1
    assert r2_route == r2

    assert {cell.geohash for cell in r1_cells} == {
        "w21z7",
        "w21z6",
    }
    assert {cell.geohash for cell in r2_cells} == {
        "w21ze",
    }


def test_cells_by_region_skips_unknown_cells() -> None:
    cells = nearby_cells("w21z7")

    r1 = route("r1", "r1-primary")

    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={
            "w21z6": r1,
        },
        region_routes={
            "r1": r1,
        },
    )

    grouped = cells_by_region(cells, directory)

    assert set(grouped) == {"r1"}

    _, routed_cells = grouped["r1"]
    assert [cell.geohash for cell in routed_cells] == ["w21z6"]


async def test_nearby_live_merges_regions_and_applies_global_limit(
    monkeypatch,
) -> None:
    r1 = route("r1", "r1-primary")
    r2 = route("r2", "r2-primary")

    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={
            "w21z7": r1,
            "w21z6": r1,
            "w21ze": r2,
        },
        region_routes={
            "r1": r1,
            "r2": r2,
        },
    )

    pools = AsyncMock()

    async def fake_query_region(
        pool,
        region_id,
        cells,
        dish_id,
        limit,
    ):
        if region_id == "r1":
            return [
                {
                    "vendor_id": "v3",
                    "product_id": "p1",
                    "distance_m": 0.0,
                },
                {
                    "vendor_id": "v1",
                    "product_id": "p2",
                    "distance_m": 4885.0,
                },
            ]

        return [
            {
                "vendor_id": "v2",
                "product_id": "p1",
                "distance_m": 1000.0,
            },
            {
                "vendor_id": "v4",
                "product_id": "p1",
                "distance_m": 6000.0,
            },
        ]

    monkeypatch.setattr(
        nearby,
        "query_region",
        fake_query_region,
    )

    result = await nearby.nearby_live(
        "w21z7",
        "dish1",
        3,
        directory,
        pools,
    )

    assert [(row["distance_m"], row["vendor_id"], row["product_id"]) for row in result.results] == [
        (0.0, "v3", "p1"),
        (1000.0, "v2", "p1"),
        (4885.0, "v1", "p2"),
    ]

    assert result.meta.mode == "live"
    assert result.meta.nodes_contacted == [
        "r1-primary",
        "r2-primary",
    ]
    assert result.meta.missing_regions == []
    assert result.meta.rows_transferred == 4


async def test_nearby_live_returns_partial_results_when_region_is_missing(
    monkeypatch,
) -> None:
    r1 = route("r1", "r1-primary")
    r2 = route("r2", "r2-primary")

    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={
            "w21z7": r1,
            "w21ze": r2,
        },
        region_routes={
            "r1": r1,
            "r2": r2,
        },
    )

    pools = AsyncMock()

    async def fake_query_region(
        pool,
        region_id,
        cells,
        dish_id,
        limit,
    ):
        if region_id == "r2":
            raise RegionUnreachableError(region_id="r2")

        return [
            {
                "vendor_id": "v1",
                "product_id": "p1",
                "distance_m": 0.0,
            }
        ]

    monkeypatch.setattr(
        nearby,
        "query_region",
        fake_query_region,
    )

    result = await nearby.nearby_live(
        "w21z7",
        "dish1",
        10,
        directory,
        pools,
    )

    assert len(result.results) == 1
    assert result.meta.missing_regions == ["r2"]
    assert result.meta.nodes_contacted == [
        "r1-primary",
        "r2-primary",
    ]
    assert result.meta.rows_transferred == 1

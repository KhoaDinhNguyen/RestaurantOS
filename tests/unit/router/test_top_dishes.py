from unittest.mock import AsyncMock

from restaurantos.common.errors import RegionUnreachableError
from restaurantos.gateway.directory import DirectoryCache, RegionRoute
from restaurantos.gateway.router import top_dishes
from restaurantos.gateway.router.top_dishes import merge_top_dishes
from restaurantos.gateway.router.window import QueryWindow


def test_merge_adds_every_metric() -> None:
    result = merge_top_dishes(
        [
            [
                {
                    "dish_id": "d1",
                    "dish_name": "Dish 1",
                    "example_name": "D1",
                    "units": 10,
                    "quantity": 5,
                    "order_lines": 3,
                    "sales_units": 100,
                }
            ],
            [
                {
                    "dish_id": "d1",
                    "dish_name": "Dish 1",
                    "example_name": "D1",
                    "units": 7,
                    "quantity": 4,
                    "order_lines": 2,
                    "sales_units": 80,
                }
            ],
        ],
        rank_by="units",
        limit=10,
    )

    assert result == [
        {
            "dish_id": "d1",
            "dish_name": "Dish 1",
            "example_name": "D1",
            "units": 17,
            "quantity": 9,
            "order_lines": 5,
            "sales_units": 180,
        }
    ]


def test_global_top_n_is_not_local_top_n() -> None:
    result = merge_top_dishes(
        [
            [
                {
                    "dish_id": "local-a",
                    "dish_name": "A",
                    "example_name": "A",
                    "units": 10,
                    "quantity": 10,
                    "order_lines": 10,
                    "sales_units": 10,
                },
                {
                    "dish_id": "spread",
                    "dish_name": "Spread",
                    "example_name": "Spread",
                    "units": 9,
                    "quantity": 9,
                    "order_lines": 9,
                    "sales_units": 9,
                },
            ],
            [
                {
                    "dish_id": "local-b",
                    "dish_name": "B",
                    "example_name": "B",
                    "units": 10,
                    "quantity": 10,
                    "order_lines": 10,
                    "sales_units": 10,
                },
                {
                    "dish_id": "spread",
                    "dish_name": "Spread",
                    "example_name": "Spread",
                    "units": 9,
                    "quantity": 9,
                    "order_lines": 9,
                    "sales_units": 9,
                },
            ],
        ],
        rank_by="units",
        limit=1,
    )

    assert result[0]["dish_id"] == "spread"
    assert result[0]["units"] == 18


def test_rank_by_changes_order() -> None:
    rows = [
        [
            {
                "dish_id": "d1",
                "dish_name": "D1",
                "example_name": "D1",
                "units": 20,
                "quantity": 2,
                "order_lines": 1,
                "sales_units": 20,
            },
            {
                "dish_id": "d2",
                "dish_name": "D2",
                "example_name": "D2",
                "units": 10,
                "quantity": 5,
                "order_lines": 3,
                "sales_units": 50,
            },
        ]
    ]

    assert (
        merge_top_dishes(
            rows,
            rank_by="units",
            limit=10,
        )[0]["dish_id"]
        == "d1"
    )

    assert (
        merge_top_dishes(
            rows,
            rank_by="quantity",
            limit=10,
        )[0]["dish_id"]
        == "d2"
    )

    assert (
        merge_top_dishes(
            rows,
            rank_by="order_lines",
            limit=10,
        )[0]["dish_id"]
        == "d2"
    )

    assert (
        merge_top_dishes(
            rows,
            rank_by="sales_units",
            limit=10,
        )[0]["dish_id"]
        == "d2"
    )


def test_rank_tie_breaks_by_dish_id() -> None:
    rows = [
        [
            {
                "dish_id": "d2",
                "dish_name": "D2",
                "example_name": "D2",
                "units": 5,
                "quantity": 5,
                "order_lines": 1,
                "sales_units": 5,
            },
            {
                "dish_id": "d1",
                "dish_name": "D1",
                "example_name": "D1",
                "units": 5,
                "quantity": 5,
                "order_lines": 1,
                "sales_units": 5,
            },
        ]
    ]

    result = merge_top_dishes(
        rows,
        rank_by="units",
        limit=10,
    )

    assert [row["dish_id"] for row in result] == ["d1", "d2"]


async def test_live_scatter_merges_all_regions(monkeypatch) -> None:
    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={
            "r1": RegionRoute("r1", 1, "r1-primary"),
            "r2": RegionRoute("r2", 1, "r2-primary"),
        },
    )
    pools = AsyncMock()

    async def fake_query(
        pool,
        window,
        *,
        region_id=None,
        hq=False,
    ):
        value = 9 if region_id == "r1" else 8

        return [
            {
                "dish_id": "spread",
                "dish_name": "Spread",
                "example_name": "Spread",
                "units": value,
                "quantity": value,
                "order_lines": 1,
                "sales_units": value,
            }
        ]

    monkeypatch.setattr(
        top_dishes,
        "query_top_dishes_node",
        fake_query,
    )

    result = await top_dishes.top_dishes_live(
        QueryWindow(14, 14),
        rank_by="units",
        limit=10,
        directory=directory,
        pools=pools,
    )

    assert result.results[0]["units"] == 17
    assert result.meta.nodes_contacted == [
        "r1-primary",
        "r2-primary",
    ]
    assert result.meta.missing_regions == []
    assert result.meta.rows_transferred == 2


async def test_live_scatter_keeps_partial_results(
    monkeypatch,
) -> None:
    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={
            "r1": RegionRoute("r1", 1, "r1-primary"),
            "r2": RegionRoute("r2", 1, "r2-primary"),
        },
    )
    pools = AsyncMock()

    async def fake_query(
        pool,
        window,
        *,
        region_id=None,
        hq=False,
    ):
        if region_id == "r2":
            raise RegionUnreachableError(region_id="r2")

        return [
            {
                "dish_id": "d1",
                "dish_name": "D1",
                "example_name": "D1",
                "units": 5,
                "quantity": 5,
                "order_lines": 1,
                "sales_units": 5,
            }
        ]

    monkeypatch.setattr(
        top_dishes,
        "query_top_dishes_node",
        fake_query,
    )

    result = await top_dishes.top_dishes_live(
        QueryWindow(14, 14),
        rank_by="units",
        limit=10,
        directory=directory,
        pools=pools,
    )

    assert result.results[0]["dish_id"] == "d1"
    assert result.meta.missing_regions == ["r2"]
    assert result.meta.rows_transferred == 1

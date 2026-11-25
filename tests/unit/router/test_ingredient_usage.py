from unittest.mock import AsyncMock

from restaurantos.common.errors import RegionUnreachableError
from restaurantos.gateway.directory import DirectoryCache, RegionRoute
from restaurantos.gateway.router import ingredient_usage
from restaurantos.gateway.router.ingredient_usage import (
    join_lines_with_recipes,
    merge_ingredient_usage,
)
from restaurantos.gateway.router.window import QueryWindow


def test_merge_ingredient_usage_adds_regions() -> None:
    result = merge_ingredient_usage(
        [
            [
                {
                    "ingredient_id": "rice",
                    "amount": 1000,
                    "unit": "gram",
                },
                {
                    "ingredient_id": "beef",
                    "amount": 8,
                    "unit": "kg",
                },
            ],
            [
                {
                    "ingredient_id": "rice",
                    "amount": 500,
                    "unit": "gram",
                }
            ],
        ]
    )

    assert result == [
        {
            "ingredient_id": "rice",
            "amount": 1500,
            "unit": "gram",
        },
        {
            "ingredient_id": "beef",
            "amount": 8,
            "unit": "kg",
        },
    ]


def test_merge_groups_by_ingredient_and_unit() -> None:
    result = merge_ingredient_usage(
        [
            [
                {
                    "ingredient_id": "salt",
                    "amount": 5,
                    "unit": "gram",
                },
                {
                    "ingredient_id": "salt",
                    "amount": 2,
                    "unit": "portion",
                },
            ]
        ]
    )

    assert len(result) == 2
    assert {(row["ingredient_id"], row["unit"]) for row in result} == {
        ("salt", "gram"),
        ("salt", "portion"),
    }


def test_strategy_b_router_join() -> None:
    result = join_lines_with_recipes(
        [
            {
                "vendor_id": "v1",
                "product_id": "p_rice",
                "quantity": 3,
            }
        ],
        [
            {
                "vendor_id": "v1",
                "product_id": "p_rice",
                "multiplier": 2,
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            },
            {
                "vendor_id": "v1",
                "product_id": "p_rice",
                "multiplier": 2,
                "ingredient_id": "beef",
                "amount": 2,
                "unit": "kg",
            },
        ],
    )

    assert result == [
        {
            "ingredient_id": "rice",
            "amount": 1500,
            "unit": "gram",
        },
        {
            "ingredient_id": "beef",
            "amount": 12,
            "unit": "kg",
        },
    ]


async def test_strategy_b_live_ships_lines_and_joins_hq_recipes(
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
    hq_pool = object()

    async def fake_lines(
        pool,
        window,
        *,
        region_id=None,
    ):
        if region_id == "r1":
            return [
                {
                    "vendor_id": "v1",
                    "product_id": "p_rice_set",
                    "quantity": 3,
                }
            ]

        return [
            {
                "vendor_id": "v2",
                "product_id": "p_rice_single",
                "quantity": 2,
            }
        ]

    async def fake_product_recipes(pool, lines):
        assert pool is hq_pool
        assert {(row["vendor_id"], row["product_id"], row["quantity"]) for row in lines} == {
            ("v1", "p_rice_set", 3),
            ("v2", "p_rice_single", 2),
        }

        return [
            {
                "vendor_id": "v1",
                "product_id": "p_rice_set",
                "multiplier": 2,
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            },
            {
                "vendor_id": "v2",
                "product_id": "p_rice_single",
                "multiplier": 1,
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            },
        ]

    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_b_lines",
        fake_lines,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_product_recipes",
        fake_product_recipes,
    )

    result = await ingredient_usage.ingredient_usage_b_live(
        QueryWindow(14, 14),
        directory=directory,
        pools=pools,
        hq_pool=hq_pool,
    )

    # r1: 3 * 2 * 250 = 1500
    # r2: 2 * 1 * 250 = 500
    assert result.results == [
        {
            "ingredient_id": "rice",
            "amount": 2000,
            "unit": "gram",
        }
    ]

    assert result.meta.nodes_contacted == [
        "r1-primary",
        "r2-primary",
    ]
    assert result.meta.missing_regions == []
    assert result.meta.rows_transferred == 2


async def test_strategy_b_live_keeps_partial_results(
    monkeypatch,
) -> None:
    from restaurantos.common.errors import RegionUnreachableError

    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={
            "r1": RegionRoute("r1", 1, "r1-primary"),
            "r2": RegionRoute("r2", 1, "r2-primary"),
        },
    )

    pools = AsyncMock()
    hq_pool = object()

    async def fake_lines(
        pool,
        window,
        *,
        region_id=None,
    ):
        if region_id == "r2":
            raise RegionUnreachableError(region_id="r2")

        return [
            {
                "vendor_id": "v1",
                "product_id": "p_rice",
                "quantity": 3,
            }
        ]

    async def fake_product_recipes(pool, lines):
        assert pool is hq_pool
        return [
            {
                "vendor_id": "v1",
                "product_id": "p_rice",
                "multiplier": 2,
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            }
        ]

    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_b_lines",
        fake_lines,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_product_recipes",
        fake_product_recipes,
    )

    result = await ingredient_usage.ingredient_usage_b_live(
        QueryWindow(14, 14),
        directory=directory,
        pools=pools,
        hq_pool=hq_pool,
    )

    assert result.results == [
        {
            "ingredient_id": "rice",
            "amount": 1500,
            "unit": "gram",
        }
    ]
    assert result.meta.missing_regions == ["r2"]
    assert result.meta.rows_transferred == 1


async def test_strategy_c_live_performs_two_round_semijoin(
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
    hq_pool = object()

    async def fake_dishes(
        pool,
        window,
        *,
        region_id=None,
    ):
        if region_id == "r1":
            return ["d_rice"]
        return ["d_rice", "d_beef"]

    async def fake_recipes(pool, dish_ids):
        assert pool is hq_pool
        assert dish_ids == ["d_beef", "d_rice"]

        return [
            {
                "dish_id": "d_rice",
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            },
            {
                "dish_id": "d_beef",
                "ingredient_id": "beef",
                "amount": 2,
                "unit": "kg",
            },
        ]

    seen_recipes: dict[str, list[str]] = {}

    async def fake_aggregate(
        pool,
        window,
        recipes,
        *,
        region_id=None,
        on_submit=None,
    ):
        if on_submit is not None:
            on_submit()

        seen_recipes[region_id] = sorted(row["dish_id"] for row in recipes)

        if region_id == "r1":
            return [
                {
                    "ingredient_id": "rice",
                    "amount": 1500,
                    "unit": "gram",
                }
            ]

        return [
            {
                "ingredient_id": "rice",
                "amount": 500,
                "unit": "gram",
            },
            {
                "ingredient_id": "beef",
                "amount": 4,
                "unit": "kg",
            },
        ]

    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_c_dishes",
        fake_dishes,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_recipes",
        fake_recipes,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_c_aggregate",
        fake_aggregate,
    )

    result = await ingredient_usage.ingredient_usage_c_live(
        QueryWindow(14, 14),
        directory=directory,
        pools=pools,
        hq_pool=hq_pool,
    )

    assert seen_recipes == {
        "r1": ["d_rice"],
        "r2": ["d_beef", "d_rice"],
    }

    assert result.results == [
        {
            "ingredient_id": "rice",
            "amount": 2000,
            "unit": "gram",
        },
        {
            "ingredient_id": "beef",
            "amount": 4,
            "unit": "kg",
        },
    ]

    # Round 1: 1 + 2 dish-id rows = 3
    # Round 2 down: 1 + 2 recipe rows = 3
    # Round 2 up: 1 + 2 aggregate rows = 3
    assert result.meta.rows_transferred == 9
    assert result.meta.missing_regions == []


async def test_strategy_c_does_not_count_recipes_before_submission(
    monkeypatch,
) -> None:
    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={
            "r1": RegionRoute("r1", 1, "r1-primary"),
        },
    )

    pools = AsyncMock()
    hq_pool = object()

    # First pools.get() succeeds for round 1.
    # Second pools.get() fails before round 2 can submit recipes.
    pools.get.side_effect = [
        object(),
        RegionUnreachableError("unreachable", region_id="r1"),
    ]

    async def fake_dishes(
        pool,
        window,
        *,
        region_id=None,
    ):
        return ["d_rice"]

    async def fake_recipes(pool, dish_ids):
        return [
            {
                "dish_id": "d_rice",
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            }
        ]

    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_c_dishes",
        fake_dishes,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_recipes",
        fake_recipes,
    )

    result = await ingredient_usage.ingredient_usage_c_live(
        QueryWindow(14, 14),
        directory=directory,
        pools=pools,
        hq_pool=hq_pool,
    )

    # Only the Round-1 dish ID crossed the link.
    assert result.meta.rows_transferred == 1
    assert result.meta.missing_regions == ["r1"]


async def test_strategy_c_counts_recipes_after_submission_failure(
    monkeypatch,
) -> None:
    directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={
            "r1": RegionRoute("r1", 1, "r1-primary"),
        },
    )

    pools = AsyncMock()
    hq_pool = object()

    async def fake_dishes(
        pool,
        window,
        *,
        region_id=None,
    ):
        return ["d_rice"]

    async def fake_recipes(pool, dish_ids):
        return [
            {
                "dish_id": "d_rice",
                "ingredient_id": "rice",
                "amount": 250,
                "unit": "gram",
            }
        ]

    async def fake_aggregate(
        pool,
        window,
        recipes,
        *,
        region_id=None,
        on_submit=None,
    ):
        assert on_submit is not None
        on_submit()

        raise RegionUnreachableError(
            "unreachable",
            region_id=region_id,
        )

    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_c_dishes",
        fake_dishes,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_recipes",
        fake_recipes,
    )
    monkeypatch.setattr(
        ingredient_usage,
        "query_strategy_c_aggregate",
        fake_aggregate,
    )

    result = await ingredient_usage.ingredient_usage_c_live(
        QueryWindow(14, 14),
        directory=directory,
        pools=pools,
        hq_pool=hq_pool,
    )

    # Round 1: one dish ID.
    # Round 2 down: one recipe row was submitted.
    # No aggregate rows returned.
    assert result.meta.rows_transferred == 2
    assert result.meta.missing_regions == ["r1"]

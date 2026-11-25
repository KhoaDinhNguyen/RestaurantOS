from datetime import UTC, datetime
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from restaurantos.common.config import Settings
from restaurantos.common.models import QueryMeta
from restaurantos.gateway import app as gateway_app
from restaurantos.gateway.directory import DirectoryCache
from restaurantos.gateway.router.ingredient_usage import IngredientUsageResult
from restaurantos.gateway.router.nearby import NearbyResult


def gateway_settings() -> Settings:
    return Settings(
        _env_file=None,
        mode="gateway",
        network="host",
    )


def test_nearby_route_returns_expected_shape(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        gateway_settings,
    )

    async def fake_get_dish(pool, dish_id, region_id=None):
        assert dish_id == "d1"
        return {
            "dish_id": "d1",
            "dish_name": "laksa",
            "example_name": "Laksa",
            "scope": "global",
            "n_vendors": 2,
        }

    async def fake_nearby_live(
        geohash,
        dish_id,
        limit,
        directory,
        pools,
    ):
        assert geohash == "w21z7"
        assert dish_id == "d1"
        assert limit == 10

        return NearbyResult(
            results=[
                {
                    "vendor_id": "v1",
                    "chain_id": None,
                    "primary_cuisine": "noodles",
                    "geohash": "w21z7",
                    "distance_m": 0.0,
                    "product_id": "p1",
                    "name": "Laksa",
                    "price_units": 30,
                    "multiplier": 1,
                    "region_id": "r1",
                }
            ],
            meta=QueryMeta(
                mode="live",
                nodes_contacted=["r1-primary"],
                missing_regions=[],
                rows_transferred=1,
                data_as_of=datetime.now(UTC),
                elapsed_ms=1.5,
            ),
        )

    monkeypatch.setattr(gateway_app, "get_dish", fake_get_dish)
    monkeypatch.setattr(gateway_app, "nearby_live", fake_nearby_live)

    app = gateway_app.create_app()

    app.state.hq_pool = object()
    app.state.directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={},
    )
    app.state.region_pools = AsyncMock()

    client = TestClient(app)

    response = client.get(
        "/api/v1/query/nearby",
        params={
            "geohash": "w21z7",
            "dish": "d1",
            "limit": 10,
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["geohash"] == "w21z7"
    assert body["dish_id"] == "d1"
    assert body["dish_name"] == "laksa"
    assert body["example_name"] == "Laksa"
    assert body["limit"] == 10

    assert len(body["results"]) == 1
    assert body["results"][0]["vendor_id"] == "v1"
    assert body["results"][0]["region_id"] == "r1"

    assert body["meta"]["mode"] == "live"
    assert body["meta"]["nodes_contacted"] == ["r1-primary"]
    assert body["meta"]["rows_transferred"] == 1


def test_nearby_route_unknown_dish_returns_404(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        gateway_settings,
    )

    async def fake_get_dish(pool, dish_id, region_id=None):
        return None

    monkeypatch.setattr(gateway_app, "get_dish", fake_get_dish)

    app = gateway_app.create_app()
    app.state.hq_pool = object()

    client = TestClient(app)

    response = client.get(
        "/api/v1/query/nearby",
        params={
            "geohash": "w21z7",
            "dish": "missing",
        },
    )

    assert response.status_code == 404
    assert response.json() == {"error": "not_found"}


def test_nearby_route_rejects_invalid_geohash(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        gateway_settings,
    )

    app = gateway_app.create_app()
    client = TestClient(app)

    for geohash in ("a21z7", "w21i7", "w21l7", "w21o7"):
        response = client.get(
            "/api/v1/query/nearby",
            params={
                "geohash": geohash,
                "dish": "d1",
            },
        )

        assert response.status_code == 422


def test_ingredient_usage_live_strategy_c(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        gateway_settings,
    )

    c_query = AsyncMock(
        return_value=IngredientUsageResult(
            results=[
                {
                    "ingredient_id": "rice",
                    "amount": 1500,
                    "unit": "gram",
                }
            ],
            meta=QueryMeta(
                mode="live",
                nodes_contacted=["r1-primary"],
                missing_regions=[],
                rows_transferred=3,
                data_as_of=datetime.now(UTC),
                elapsed_ms=1.0,
            ),
        )
    )

    monkeypatch.setattr(
        gateway_app,
        "ingredient_usage_c_live",
        c_query,
    )

    app = gateway_app.create_app()
    app.state.directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={},
    )
    app.state.region_pools = AsyncMock()
    app.state.hq_pool = object()

    client = TestClient(app)

    response = client.get(
        "/api/v1/query/ingredient-usage",
        params={
            "day": 14,
            "strategy": "C",
            "mode": "live",
        },
    )

    assert response.status_code == 200
    assert response.json()["results"][0]["amount"] == 1500
    assert response.json()["meta"]["mode"] == "live"

    c_query.assert_awaited_once()


def test_ingredient_usage_hq_ignores_strategy(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        gateway_settings,
    )

    single = AsyncMock(
        return_value=IngredientUsageResult(
            results=[],
            meta=QueryMeta(
                mode="hq",
                nodes_contacted=["hq-primary"],
                missing_regions=[],
                rows_transferred=0,
                data_as_of=datetime.now(UTC),
                elapsed_ms=1.0,
            ),
        )
    )
    c_query = AsyncMock()

    monkeypatch.setattr(
        gateway_app,
        "ingredient_usage_single",
        single,
    )
    monkeypatch.setattr(
        gateway_app,
        "ingredient_usage_c_live",
        c_query,
    )

    app = gateway_app.create_app()
    app.state.hq_pool = object()

    client = TestClient(app)

    response = client.get(
        "/api/v1/query/ingredient-usage",
        params={
            "day": 14,
            "strategy": "C",
            "mode": "hq",
        },
    )

    assert response.status_code == 200
    assert response.json()["meta"]["mode"] == "hq"
    assert response.json()["strategy"] is None

    single.assert_awaited_once()
    c_query.assert_not_awaited()


def test_ingredient_usage_requires_query_window(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        gateway_settings,
    )

    app = gateway_app.create_app()
    client = TestClient(app)

    response = client.get(
        "/api/v1/query/ingredient-usage",
        params={
            "strategy": "A",
            "mode": "live",
        },
    )

    assert response.status_code == 422

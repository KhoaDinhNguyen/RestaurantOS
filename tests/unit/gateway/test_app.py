from fastapi.testclient import TestClient

from restaurantos.gateway.app import create_app


def test_health() -> None:
    client = TestClient(create_app())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "mode": "gateway",
    }


def test_regions(monkeypatch) -> None:
    async def fake_get_regions(pool):
        return [
            {
                "region_id": "r1",
                "epoch": 1,
                "primary_node": "r1-primary",
                "standby_node": "r1-standby",
                "menu_version": 1,
                "updated_at": None,
                "geohashes": ["w21z7"],
            }
        ]

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_regions",
        fake_get_regions,
    )

    app = create_app()
    app.state.hq_pool = object()

    client = TestClient(app)
    response = client.get("/api/v1/regions")

    assert response.status_code == 200
    assert response.json() == [
        {
            "region_id": "r1",
            "epoch": 1,
            "primary_node": "r1-primary",
            "standby_node": "r1-standby",
            "menu_version": 1,
            "updated_at": None,
            "geohashes": ["w21z7"],
        }
    ]


def test_region_unreachable_returns_503() -> None:
    from restaurantos.common.errors import RegionUnreachableError

    app = create_app()

    @app.get("/test/unreachable")
    async def unreachable():
        raise RegionUnreachableError(region_id="r1")

    client = TestClient(app)
    response = client.get("/test/unreachable")

    assert response.status_code == 503
    assert response.json() == {
        "error": "region_unreachable",
        "region_id": "r1",
    }


def test_unknown_vendor_returns_404() -> None:
    from restaurantos.common.errors import UnknownVendorError

    app = create_app()

    @app.get("/test/not-found")
    async def not_found():
        raise UnknownVendorError()

    client = TestClient(app)
    response = client.get("/test/not-found")

    assert response.status_code == 404
    assert response.json() == {"error": "not_found"}

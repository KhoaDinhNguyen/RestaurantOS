from unittest.mock import AsyncMock

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


def test_vendor_menu(monkeypatch) -> None:
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    async def fake_get_vendor_menu(pool, vendor_id, region_id):
        assert vendor_id == "v001"
        assert region_id == "r1"

        return [
            {
                "vendor_id": "v001",
                "product_id": "p003",
                "name": "Menu item 003",
                "price_units": 2,
                "menu_version": 1,
                "reason": None,
                "available": True,
            }
        ]

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_vendor_menu",
        fake_get_vendor_menu,
    )

    app = create_app()

    app.state.directory = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
        region_routes={},
    )

    region_pools = AsyncMock()
    region_pools.get.return_value = object()
    app.state.region_pools = region_pools

    client = TestClient(app)
    response = client.get("/api/v1/vendors/v001/menu")

    assert response.status_code == 200
    assert response.json()[0]["product_id"] == "p003"

    region_pools.get.assert_awaited_once_with("r1-primary")


def test_internal_probe(monkeypatch) -> None:
    from restaurantos.common.localnode import NodeView

    async def fake_probe_node(node, conninfo, settings):
        return NodeView(
            node=node,
            reachable=True,
            in_recovery=False,
            epoch=2,
        )

    monkeypatch.setattr(
        "restaurantos.gateway.app.probe_node",
        fake_probe_node,
    )

    client = TestClient(create_app())
    response = client.get("/internal/probe/r1-primary")

    assert response.status_code == 200
    assert response.json() == {
        "node_id": "r1-primary",
        "reachable": True,
        "writable": True,
        "in_recovery": False,
        "epoch": 2,
    }


def test_create_order_accepted(monkeypatch) -> None:
    from tests.unit.gateway.fakes import place_order_accepted

    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    async def fake_place_region_order(
        pool,
        order,
        region_id,
        epoch,
        received_at,
        place_order,
    ):
        return await place_order(
            None,
            order,
            epoch,
            received_at=received_at,
        )

    monkeypatch.setattr(
        "restaurantos.gateway.app.place_region_order",
        fake_place_region_order,
    )

    app = create_app()

    app.state.directory = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
        region_routes={},
    )

    region_pools = AsyncMock()
    region_pools.get.return_value = object()
    app.state.region_pools = region_pools
    app.state.place_order = place_order_accepted

    client = TestClient(app)

    response = client.post(
        "/api/v1/orders",
        json={
            "channel": "dhrd",
            "external_order_id": "123",
            "vendor_id": "v001",
            "customer_id": "c001",
            "customer_geohash": "w21z7",
            "order_day": 14,
            "order_time": "12:03:29",
            "items": [
                {
                    "product_id": "p003",
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 201

    body = response.json()

    assert body["outcome"] == "accepted"
    assert body["duplicate"] is False
    assert body["queued"] is False
    assert body["region_id"] == "r1"
    assert body["status"] == "received"
    assert body["received_at"] is not None
    assert body["committed_at"] is not None
    assert body["acked_at"] is not None

    region_pools.get.assert_awaited_once_with("r1-primary")


def test_create_order_parks_when_region_unreachable(monkeypatch) -> None:
    from restaurantos.common.errors import RegionUnreachableError
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    async def fake_place_region_order(*args, **kwargs):
        raise RegionUnreachableError(region_id="r1")

    async def fake_park_order(
        pool,
        order,
        region_id,
        received_at,
    ):
        assert region_id == "r1"
        assert order.external_order_id == "123"

    monkeypatch.setattr(
        "restaurantos.gateway.app.place_region_order",
        fake_place_region_order,
    )
    monkeypatch.setattr(
        "restaurantos.gateway.app.park_order",
        fake_park_order,
    )

    app = create_app()

    app.state.directory = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
        region_routes={},
    )

    region_pools = AsyncMock()
    region_pools.get.return_value = object()

    app.state.region_pools = region_pools
    app.state.hq_pool = object()
    app.state.place_order = AsyncMock()

    client = TestClient(app)

    response = client.post(
        "/api/v1/orders",
        json={
            "channel": "dhrd",
            "external_order_id": "123",
            "vendor_id": "v001",
            "customer_id": "c001",
            "customer_geohash": "w21z7",
            "order_day": 14,
            "order_time": "12:03:29",
            "items": [
                {
                    "product_id": "p003",
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 202

    body = response.json()

    assert body["queued"] is True
    assert body["region_id"] == "r1"
    assert body["outcome"] is None
    assert body["received_at"] is not None
    assert body["acked_at"] is not None
    assert body["committed_at"] is None


def test_create_order_refreshes_after_stale_epoch(monkeypatch) -> None:
    from restaurantos.common.errors import StaleEpochError
    from restaurantos.common.models import OrderOutcome, OrderResult, OrderStatus
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    calls = 0

    async def fake_place_region_order(
        pool,
        order,
        region_id,
        epoch,
        received_at,
        place_order,
    ):
        nonlocal calls
        calls += 1

        if calls == 1:
            assert epoch == 1
            raise StaleEpochError()

        assert epoch == 2
        assert region_id == "r1"

        return OrderResult(
            outcome=OrderOutcome.ACCEPTED,
            duplicate=False,
            order_day=14,
            status=OrderStatus.RECEIVED,
            menu_version=1,
            committed_at=received_at,
        )

    async def fake_refresh(pool):
        app.state.directory.cell_routes["w21z7"] = RegionRoute(
            region_id="r1",
            epoch=2,
            primary_node="r1-standby",
        )

    monkeypatch.setattr(
        "restaurantos.gateway.app.place_region_order",
        fake_place_region_order,
    )

    app = create_app()

    app.state.directory = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
        region_routes={},
    )
    app.state.directory.refresh = fake_refresh

    region_pools = AsyncMock()
    region_pools.get.side_effect = [
        object(),
        object(),
    ]

    app.state.region_pools = region_pools
    app.state.hq_pool = object()
    app.state.place_order = AsyncMock()

    client = TestClient(app)

    response = client.post(
        "/api/v1/orders",
        json={
            "channel": "dhrd",
            "external_order_id": "123",
            "vendor_id": "v001",
            "customer_id": "c001",
            "customer_geohash": "w21z7",
            "order_day": 14,
            "order_time": "12:03:29",
            "items": [
                {
                    "product_id": "p003",
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 201
    assert calls == 2

    assert region_pools.get.await_args_list[0].args == ("r1-primary",)
    assert region_pools.get.await_args_list[1].args == ("r1-standby",)


def test_order_status_routes_to_region_primary(monkeypatch) -> None:
    from uuid import uuid4

    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    order_id = uuid4()

    async def fake_get_order_status(pool, vendor_id, requested_order_id, region_id):
        assert vendor_id == "v001"
        assert requested_order_id == order_id
        assert region_id == "r1"

        return {
            "order_id": order_id,
            "order_day": 14,
            "vendor_id": "v001",
            "status": "accepted",
            "status_changed_at": None,
            "menu_version": 1,
            "received_at": None,
            "committed_at": None,
        }

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_order_status",
        fake_get_order_status,
    )

    app = create_app()
    app.state.directory = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
        region_routes={},
    )

    region_pools = AsyncMock()
    region_pools.get.return_value = object()
    app.state.region_pools = region_pools

    client = TestClient(app)
    response = client.get(f"/api/v1/vendors/v001/orders/{order_id}")

    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    region_pools.get.assert_awaited_once_with("r1-primary")


def test_create_order_parks_when_retry_after_stale_epoch_is_unreachable(
    monkeypatch,
) -> None:
    from restaurantos.common.errors import RegionUnreachableError, StaleEpochError
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    calls = 0
    parked = False

    async def fake_place_region_order(*args, **kwargs):
        nonlocal calls
        calls += 1

        if calls == 1:
            raise StaleEpochError()

        raise RegionUnreachableError(region_id="r1")

    async def fake_refresh(pool):
        app.state.directory.cell_routes["w21z7"] = RegionRoute(
            region_id="r1",
            epoch=2,
            primary_node="r1-standby",
        )

    async def fake_park_order(
        pool,
        order,
        region_id,
        received_at,
    ):
        nonlocal parked
        parked = True

        assert region_id == "r1"
        assert order.external_order_id == "123"

    monkeypatch.setattr(
        "restaurantos.gateway.app.place_region_order",
        fake_place_region_order,
    )
    monkeypatch.setattr(
        "restaurantos.gateway.app.park_order",
        fake_park_order,
    )

    app = create_app()

    app.state.directory = DirectoryCache(
        vendor_cells={"v001": "w21z7"},
        cell_routes={
            "w21z7": RegionRoute(
                region_id="r1",
                epoch=1,
                primary_node="r1-primary",
            )
        },
        region_routes={},
    )
    app.state.directory.refresh = fake_refresh

    region_pools = AsyncMock()
    region_pools.get.side_effect = [object(), object()]

    app.state.region_pools = region_pools
    app.state.hq_pool = object()
    app.state.place_order = AsyncMock()

    client = TestClient(app)

    response = client.post(
        "/api/v1/orders",
        json={
            "channel": "dhrd",
            "external_order_id": "123",
            "vendor_id": "v001",
            "customer_id": "c001",
            "customer_geohash": "w21z7",
            "order_day": 14,
            "order_time": "12:03:29",
            "items": [
                {
                    "product_id": "p003",
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 202
    assert response.json()["queued"] is True
    assert calls == 2
    assert parked is True

    assert region_pools.get.await_args_list[0].args == ("r1-primary",)
    assert region_pools.get.await_args_list[1].args == ("r1-standby",)


def test_gateway_only_routes_not_registered_in_edge_mode(monkeypatch) -> None:
    from restaurantos.common.config import Settings

    edge_settings = Settings(
        mode="edge",
        region_id="r1",
        network="host",
    )

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_settings",
        lambda: edge_settings,
    )

    app = create_app()
    client = TestClient(app)

    assert client.get("/health").status_code == 200
    assert client.get("/health").json()["mode"] == "edge"

    assert client.get("/api/v1/regions").status_code == 404
    assert client.get("/api/v1/vendors/v001/menu").status_code == 404

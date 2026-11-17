import asyncio
from unittest.mock import AsyncMock

import pytest
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
                "dish_id": "d001",
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

    expected_order_id = uuid4()

    async def fake_get_order_status(
        pool,
        vendor_id,
        order_id,
        region_id,
        *,
        day=None,
    ):
        assert vendor_id == "v001"
        assert order_id == expected_order_id
        assert region_id == "r1"
        assert day is None

        return {
            "order_id": order_id,
            "order_day": 14,
            "order_time": "12:03:29",
            "vendor_id": "v001",
            "customer_id": "c001",
            "channel": "dhrd",
            "status": "accepted",
            "status_changed_at": None,
            "menu_version": 1,
            "received_at": None,
            "committed_at": None,
            "items": [],
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
    response = client.get(f"/api/v1/vendors/v001/orders/{expected_order_id}")

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


def test_hq_only_routes_not_registered_in_edge_mode(monkeypatch) -> None:
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
    assert client.post("/api/v1/orders", json={}).status_code == 404


async def test_periodic_directory_refresh(monkeypatch) -> None:
    from restaurantos.gateway import app as gateway_app

    directory = AsyncMock()
    hq_pool = object()

    sleeps = 0

    async def fake_sleep(delay):
        nonlocal sleeps
        sleeps += 1

        assert delay == 5.0

        if sleeps > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(gateway_app.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await gateway_app.refresh_directory_periodically(
            directory,
            hq_pool,
            5.0,
        )

    directory.refresh.assert_awaited_once_with(hq_pool)


async def test_periodic_edge_primary_refresh(monkeypatch) -> None:
    from restaurantos.gateway import app as gateway_app

    primary = AsyncMock()
    sleeps = 0

    async def fake_sleep(delay):
        nonlocal sleeps
        sleeps += 1

        assert delay == 1.0

        if sleeps > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(gateway_app.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await gateway_app.refresh_edge_primary_periodically(
            primary,
            1.0,
        )

    primary.refresh.assert_awaited_once_with()


def test_edge_mode_finds_primary_at_startup(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    primary = AsyncMock()
    primary.refresh.return_value = "r1-primary"

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "EdgePrimary",
        lambda *args, **kwargs: primary,
    )

    app = gateway_app.create_app()

    with TestClient(app):
        pass

    primary.refresh.assert_awaited()


def test_edge_vendor_menu_uses_local_primary(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    async def fake_get_vendor_menu(pool, vendor_id, region_id):
        assert pool == "edge-pool"
        assert vendor_id == "v001"
        assert region_id == "r1"

        return [
            {
                "vendor_id": "v001",
                "product_id": "p003",
                "name": "Menu item 003",
                "price_units": 2,
                "menu_version": 1,
                "dish_id": "d001",
                "reason": None,
                "available": True,
            }
        ]

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "get_vendor_menu",
        fake_get_vendor_menu,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.return_value = "edge-pool"

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)
    response = client.get("/api/v1/vendors/v001/menu")

    assert response.status_code == 200
    assert response.json()[0]["product_id"] == "p003"

    edge_primary.get_pool.assert_awaited_once_with(app.state.region_pools)


def test_edge_vendor_menu_returns_503_without_primary(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.common.errors import RegionUnreachableError
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.side_effect = RegionUnreachableError(region_id="r1")

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)
    response = client.get("/api/v1/vendors/v001/menu")

    assert response.status_code == 503
    assert response.json() == {
        "error": "region_unreachable",
        "region_id": "r1",
    }


def test_edge_summary_rejects_other_region(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )

    app = gateway_app.create_app()
    client = TestClient(app)

    response = client.get("/api/v1/regions/r2/summary?day=14")

    assert response.status_code == 404
    assert response.json() == {"error": "not_found"}


def test_edge_summary_uses_local_primary(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    async def fake_get_region_summary(pool, region_id, day):
        assert pool == "edge-pool"
        assert region_id == "r1"
        assert day == 14

        return {
            "region_id": "r1",
            "day": 14,
            "epoch": 1,
            "menu_version": 1,
            "vendors": 1,
            "orders": {
                "total": 3,
                "received": 0,
                "accepted": 3,
                "preparing": 0,
                "ready": 0,
                "picked_up": 0,
                "canceled": 0,
            },
            "refused": 0,
            "sales_units": 24,
            "products_unavailable": 0,
            "outbox_backlog": 0,
            "data_as_of": None,
        }

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "get_region_summary",
        fake_get_region_summary,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.return_value = "edge-pool"

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)
    response = client.get("/api/v1/regions/r1/summary?day=14")

    assert response.status_code == 200
    assert response.json()["region_id"] == "r1"
    assert response.json()["orders"]["total"] == 3

    edge_primary.get_pool.assert_awaited_once_with(app.state.region_pools)


def test_edge_menu_refreshes_primary_after_database_error(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.common.errors import RegionUnreachableError
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    async def fake_get_vendor_menu(pool, vendor_id, region_id):
        raise RegionUnreachableError(region_id=region_id)

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "get_vendor_menu",
        fake_get_vendor_menu,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.return_value = object()
    edge_primary.refresh.return_value = "r1-standby"

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)
    response = client.get("/api/v1/vendors/v001/menu")

    assert response.status_code == 503
    assert response.json() == {
        "error": "region_unreachable",
        "region_id": "r1",
    }

    edge_primary.refresh.assert_awaited_once_with()


def test_order_status_accepts_optional_day(monkeypatch) -> None:
    from uuid import uuid4

    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    order_id = uuid4()

    async def fake_get_order_status(
        pool,
        vendor_id,
        requested_order_id,
        region_id,
        *,
        day=None,
    ):
        assert vendor_id == "v001"
        assert requested_order_id == order_id
        assert region_id == "r1"
        assert day == 14

        return {
            "order_id": requested_order_id,
            "order_day": 14,
            "order_time": "12:03:29",
            "vendor_id": "v001",
            "customer_id": "c001",
            "channel": "dhrd",
            "status": "accepted",
            "status_changed_at": None,
            "menu_version": 1,
            "received_at": None,
            "committed_at": None,
            "items": [],
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

    response = client.get(f"/api/v1/vendors/v001/orders/{order_id}?day=14")

    assert response.status_code == 200


def test_vendor_orders_accepts_multiple_statuses(monkeypatch) -> None:
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    async def fake_get_vendor_orders(
        pool,
        vendor_id,
        day,
        statuses,
        region_id,
    ):
        assert vendor_id == "v001"
        assert day == 14
        assert statuses == ["received", "accepted", "preparing"]
        assert region_id == "r1"

        return []

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_vendor_orders",
        fake_get_vendor_orders,
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

    response = client.get("/api/v1/vendors/v001/orders?day=14&status=received,accepted,preparing")

    assert response.status_code == 200
    assert response.json() == []


def test_vendor_orders_rejects_unknown_status() -> None:
    client = TestClient(create_app())

    response = client.get("/api/v1/vendors/v001/orders?day=14&status=received,banana")

    assert response.status_code == 422


def test_vendor_search_routes_by_geohash(monkeypatch) -> None:
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    async def fake_search_vendors(
        pool,
        geohash,
        query,
        region_id,
    ):
        assert geohash == "w21z7"
        assert query == "noodle"
        assert region_id == "r1"

        return [
            {
                "vendor_id": "v001",
                "chain_id": None,
                "geohash": "w21z7",
                "primary_cuisine": "noodles",
            }
        ]

    monkeypatch.setattr(
        "restaurantos.gateway.app.search_vendors",
        fake_search_vendors,
    )

    app = create_app()
    app.state.directory = DirectoryCache(
        vendor_cells={},
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

    response = client.get("/api/v1/vendors?geohash=w21z7&q=noodle")

    assert response.status_code == 200
    assert response.json()[0]["vendor_id"] == "v001"
    region_pools.get.assert_awaited_once_with("r1-primary")


def test_vendor_search_unknown_cell_returns_empty() -> None:
    from restaurantos.gateway.directory import DirectoryCache

    app = create_app()
    app.state.directory = DirectoryCache(
        vendor_cells={},
        cell_routes={},
        region_routes={},
    )
    app.state.region_pools = AsyncMock()

    client = TestClient(app)

    response = client.get("/api/v1/vendors?geohash=zzzzz")

    assert response.status_code == 200
    assert response.json() == []
    app.state.region_pools.get.assert_not_awaited()


def test_vendor_search_requires_geohash() -> None:
    client = TestClient(create_app())

    response = client.get("/api/v1/vendors")

    assert response.status_code == 422


def test_edge_vendor_search_uses_local_region(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    async def fake_search_vendors(
        pool,
        geohash,
        query,
        region_id,
    ):
        assert pool == "edge-pool"
        assert geohash == "w21z7"
        assert query == "noodle"
        assert region_id == "r1"

        return [
            {
                "vendor_id": "v001",
                "chain_id": None,
                "geohash": "w21z7",
                "primary_cuisine": "noodles",
            }
        ]

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "search_vendors",
        fake_search_vendors,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.return_value = "edge-pool"

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)

    response = client.get("/api/v1/vendors?geohash=w21z7&q=noodle")

    assert response.status_code == 200
    assert response.json()[0]["vendor_id"] == "v001"

    edge_primary.get_pool.assert_awaited_once_with(app.state.region_pools)


def test_vendor_stock_routes_to_region_primary(monkeypatch) -> None:
    from restaurantos.gateway.directory import DirectoryCache, RegionRoute

    async def fake_get_vendor_stock(
        pool,
        vendor_id,
        region_id,
    ):
        assert vendor_id == "v001"
        assert region_id == "r1"

        return [
            {
                "item_kind": "ingredient",
                "item_id": "garlic",
                "name": "garlic",
                "unit": "portion",
                "on_hand": 12,
                "par_level": 35,
                "updated_at": None,
            }
        ]

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_vendor_stock",
        fake_get_vendor_stock,
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

    response = client.get("/api/v1/vendors/v001/stock")

    assert response.status_code == 200
    assert response.json()[0]["item_id"] == "garlic"
    assert response.json()[0]["on_hand"] == 12

    region_pools.get.assert_awaited_once_with("r1-primary")


def test_edge_vendor_stock_uses_local_primary(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    async def fake_get_vendor_stock(
        pool,
        vendor_id,
        region_id,
    ):
        assert pool == "edge-pool"
        assert vendor_id == "v001"
        assert region_id == "r1"

        return []

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "get_vendor_stock",
        fake_get_vendor_stock,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.return_value = "edge-pool"

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)

    response = client.get("/api/v1/vendors/v001/stock")

    assert response.status_code == 200
    assert response.json() == []

    edge_primary.get_pool.assert_awaited_once_with(app.state.region_pools)


def test_hq_unreachable_returns_503(monkeypatch) -> None:
    from restaurantos.common.errors import RegionUnreachableError

    async def fake_get_regions(pool):
        raise RegionUnreachableError()

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_regions",
        fake_get_regions,
    )

    app = create_app()
    app.state.hq_pool = object()

    client = TestClient(app)
    response = client.get("/api/v1/regions")

    assert response.status_code == 503
    assert response.json() == {
        "error": "hq_unreachable",
    }


def test_dish_search_requires_query() -> None:
    client = TestClient(create_app())

    response = client.get("/api/v1/dishes")

    assert response.status_code == 422


@pytest.mark.parametrize("limit", [0, 101])
def test_dish_search_rejects_invalid_limit(limit) -> None:
    client = TestClient(create_app())

    response = client.get(f"/api/v1/dishes?q=laksa&limit={limit}")

    assert response.status_code == 422


def test_dish_search_reads_hq(monkeypatch) -> None:
    async def fake_search_dishes(
        pool,
        query,
        limit,
        region_id=None,
    ):
        assert pool == "hq-pool"
        assert query == "laksa"
        assert limit == 20
        assert region_id is None

        return [
            {
                "dish_id": "d001",
                "dish_name": "Laksa",
                "example_name": "Laksa Special",
                "scope": "global",
                "n_vendors": 42,
            }
        ]

    monkeypatch.setattr(
        "restaurantos.gateway.app.search_dishes",
        fake_search_dishes,
    )

    app = create_app()
    app.state.hq_pool = "hq-pool"

    client = TestClient(app)
    response = client.get("/api/v1/dishes?q=laksa")

    assert response.status_code == 200
    assert response.json()[0]["dish_id"] == "d001"


def test_dish_lookup_returns_404(monkeypatch) -> None:
    async def fake_get_dish(
        pool,
        dish_id,
        region_id=None,
    ):
        return None

    monkeypatch.setattr(
        "restaurantos.gateway.app.get_dish",
        fake_get_dish,
    )

    app = create_app()
    app.state.hq_pool = object()

    client = TestClient(app)
    response = client.get("/api/v1/dishes/missing")

    assert response.status_code == 404
    assert response.json() == {"error": "not_found"}


def test_edge_dish_search_uses_local_primary(monkeypatch) -> None:
    from restaurantos.common.config import Settings
    from restaurantos.gateway import app as gateway_app

    edge_settings = Settings(
        _env_file=None,
        mode="edge",
        region_id="r1",
        network="host",
    )

    async def fake_search_dishes(
        pool,
        query,
        limit,
        region_id=None,
    ):
        assert pool == "edge-pool"
        assert query == "laksa"
        assert limit == 20
        assert region_id == "r1"

        return [
            {
                "dish_id": "d001",
                "dish_name": "Laksa",
                "example_name": "Laksa Special",
                "scope": "global",
                "n_vendors": 42,
            }
        ]

    monkeypatch.setattr(
        gateway_app,
        "get_settings",
        lambda: edge_settings,
    )
    monkeypatch.setattr(
        gateway_app,
        "search_dishes",
        fake_search_dishes,
    )

    app = gateway_app.create_app()

    edge_primary = AsyncMock()
    edge_primary.get_pool.return_value = "edge-pool"

    app.state.edge_primary = edge_primary
    app.state.region_pools = AsyncMock()

    client = TestClient(app)

    response = client.get("/api/v1/dishes?q=laksa")

    assert response.status_code == 200
    assert response.json()[0]["dish_id"] == "d001"

    edge_primary.get_pool.assert_awaited_once_with(app.state.region_pools)


def test_publish_menu_products_returns_202(monkeypatch) -> None:
    from restaurantos.gateway.menu_publish import (
        MenuPublishResponse,
        PublishedProduct,
    )

    async def fake_publish_menu(pool, body):
        assert pool == "hq-pool"
        assert body.note == "new product"
        assert len(body.products) == 1

        product = body.products[0]

        assert product.vendor_id == "v001"
        assert product.name == "Laksa"
        assert product.price_units == 30
        assert product.dish_id == "d001"
        assert product.multiplier == 1

        return MenuPublishResponse(
            menu_version=2,
            products=[
                PublishedProduct(
                    vendor_id="v001",
                    product_id="abc123def456",
                    region_id="r1",
                    created=True,
                )
            ],
        )

    monkeypatch.setattr(
        "restaurantos.gateway.app.publish_menu",
        fake_publish_menu,
    )

    app = create_app()
    app.state.hq_pool = "hq-pool"

    client = TestClient(app)

    response = client.post(
        "/api/v1/menu/products",
        json={
            "note": "new product",
            "products": [
                {
                    "vendor_id": "v001",
                    "name": "Laksa",
                    "price_units": 30,
                    "dish_id": "d001",
                    "multiplier": 1,
                }
            ],
        },
    )

    assert response.status_code == 202
    assert response.json() == {
        "menu_version": 2,
        "products": [
            {
                "vendor_id": "v001",
                "product_id": "abc123def456",
                "region_id": "r1",
                "created": True,
            }
        ],
    }


def test_publish_menu_products_product_exists_returns_409(
    monkeypatch,
) -> None:
    from restaurantos.gateway.errors import ProductExistsError

    async def fake_publish_menu(pool, body):
        raise ProductExistsError("v001", "abc123def456")

    monkeypatch.setattr(
        "restaurantos.gateway.app.publish_menu",
        fake_publish_menu,
    )

    app = create_app()
    app.state.hq_pool = object()

    client = TestClient(app)

    response = client.post(
        "/api/v1/menu/products",
        json={
            "products": [
                {
                    "vendor_id": "v001",
                    "name": "Laksa",
                    "price_units": 30,
                    "dish_id": "d001",
                    "multiplier": 1,
                }
            ]
        },
    )

    assert response.status_code == 409
    assert response.json() == {
        "error": "product_exists",
        "vendor_id": "v001",
        "product_id": "abc123def456",
    }


def test_publish_menu_products_dish_link_fixed_returns_409(
    monkeypatch,
) -> None:
    from restaurantos.gateway.errors import DishLinkFixedError

    async def fake_publish_menu(pool, body):
        raise DishLinkFixedError()

    monkeypatch.setattr(
        "restaurantos.gateway.app.publish_menu",
        fake_publish_menu,
    )

    app = create_app()
    app.state.hq_pool = object()

    client = TestClient(app)

    response = client.post(
        "/api/v1/menu/products",
        json={
            "products": [
                {
                    "vendor_id": "v001",
                    "product_id": "p001",
                    "name": "Laksa",
                    "price_units": 30,
                    "dish_id": "different-dish",
                    "multiplier": 1,
                }
            ]
        },
    )

    assert response.status_code == 409
    assert response.json() == {
        "error": "dish_link_fixed",
    }

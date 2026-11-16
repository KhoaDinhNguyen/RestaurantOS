"""FastAPI application for the RestaurantOS gateway and edge API."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster
from restaurantos.common.config import get_settings
from restaurantos.common.db import make_pool, node_conninfo, probe_node
from restaurantos.common.errors import (
    RegionUnreachableError,
    StaleEpochError,
    UnknownVendorError,
)
from restaurantos.common.models import OrderRequest, OrderResponse
from restaurantos.gateway.directory import DirectoryCache, get_regions
from restaurantos.gateway.menu import get_vendor_menu
from restaurantos.gateway.orders import (
    get_order_status,
    get_vendor_orders,
    order_response,
    order_status_code,
    park_order,
    place_region_order,
    queued_order_response,
)
from restaurantos.gateway.pools import RegionPools
from restaurantos.gateway.summary import get_region_summary


def create_app() -> FastAPI:
    """Create the gateway or edge FastAPI application."""
    settings = get_settings()
    cluster = load_cluster(settings.cluster_file)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        region_pools = RegionPools(cluster, settings)
        app.state.region_pools = region_pools

        if settings.mode == "gateway":
            hq_pool = make_pool(
                node_conninfo(cluster, HQ_PRIMARY, settings=settings),
                settings,
            )
            await hq_pool.open()
            app.state.hq_pool = hq_pool

            directory = DirectoryCache(
                vendor_cells={},
                cell_routes={},
                region_routes={},
            )
            await directory.refresh(hq_pool)
            app.state.directory = directory

        try:
            yield
        finally:
            if settings.mode == "gateway":
                await app.state.hq_pool.close()

            await region_pools.close()

    app = FastAPI(
        title="RestaurantOS Gateway",
        version="0.1.0",
        description=(
            "RestaurantOS HTTP gateway."
            if settings.mode == "gateway"
            else f"RestaurantOS edge API for {settings.region_id}."
        ),
        lifespan=lifespan,
    )

    gateway = APIRouter(prefix="/api/v1")

    @app.exception_handler(RegionUnreachableError)
    async def region_unreachable_handler(
        request: Request,
        exc: RegionUnreachableError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "error": "region_unreachable",
                "region_id": exc.region_id,
            },
        )

    @app.exception_handler(UnknownVendorError)
    async def unknown_vendor_handler(
        request: Request,
        exc: UnknownVendorError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found"},
        )

    def get_place_order():
        place_order = getattr(app.state, "place_order", None)

        if place_order is None:
            raise RuntimeError("restaurantos.region.place_order is not available yet")

        return place_order

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "mode": settings.mode}

    @gateway.get("/regions")
    async def regions(request: Request) -> list[dict[str, object]]:
        return await get_regions(request.app.state.hq_pool)

    @gateway.get("/vendors/{vendor_id}/menu")
    async def vendor_menu(
        vendor_id: str,
        request: Request,
    ) -> list[dict[str, object]]:
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_vendor_menu(
            pool,
            vendor_id,
            route.region_id,
        )

    @app.get("/internal/probe/{node_id}")
    async def internal_probe(node_id: str) -> dict[str, object]:
        if node_id not in cluster.nodes:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found"},
            )

        view = await probe_node(
            node_id,
            node_conninfo(
                cluster,
                node_id,
                settings=settings,
            ),
            settings,
        )

        return {
            "node_id": view.node,
            "reachable": view.reachable,
            "writable": view.writable,
            "in_recovery": view.in_recovery,
            "epoch": view.epoch,
        }

    @gateway.get("/vendors/{vendor_id}/orders/{order_id}")
    async def order_status(
        vendor_id: str,
        order_id: UUID,
        request: Request,
    ) -> dict[str, object]:
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_order_status(
            pool,
            vendor_id,
            order_id,
            route.region_id,
        )

    @gateway.get("/vendors/{vendor_id}/orders")
    async def vendor_orders(
        vendor_id: str,
        day: int,
        request: Request,
        status: str | None = None,
    ) -> list[dict[str, object]]:
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_vendor_orders(
            pool,
            vendor_id,
            day,
            status,
            route.region_id,
        )

    @gateway.get("/regions/{region_id}/summary")
    async def region_summary(
        region_id: str,
        day: int,
        request: Request,
    ) -> dict[str, object]:
        if region_id not in cluster.region_ids:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found"},
            )

        route = request.app.state.directory.region_routes.get(region_id)

        if route is None:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found"},
            )

        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_region_summary(
            pool,
            region_id,
            day,
        )

    @gateway.post(
        "/orders",
        response_model=OrderResponse,
        status_code=201,
        responses={
            200: {
                "model": OrderResponse,
                "description": "Duplicate order; same result as the original request.",
            },
            202: {
                "model": OrderResponse,
                "description": "Region unreachable; order parked in the HQ holding queue.",
            },
            409: {
                "model": OrderResponse,
                "description": "Order refused.",
            },
        },
    )
    async def create_order(
        order: OrderRequest,
        request: Request,
    ) -> OrderResponse:
        received_at = datetime.now(UTC)

        route = request.app.state.directory.route_vendor(order.vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)
        place_order = get_place_order()

        try:
            try:
                result = await place_region_order(
                    pool,
                    order,
                    route.region_id,
                    route.epoch,
                    received_at,
                    place_order,
                )
            except StaleEpochError:
                await request.app.state.directory.refresh(request.app.state.hq_pool)

                route = request.app.state.directory.route_vendor(order.vendor_id)
                pool = await request.app.state.region_pools.get(route.primary_node)

                result = await place_region_order(
                    pool,
                    order,
                    route.region_id,
                    route.epoch,
                    received_at,
                    place_order,
                )
        except RegionUnreachableError:
            await park_order(
                request.app.state.hq_pool,
                order,
                route.region_id,
                received_at,
            )

            response = queued_order_response(
                route.region_id,
                received_at,
            )

            return JSONResponse(
                status_code=202,
                content=response.model_dump(mode="json"),
            )

        response = order_response(
            result,
            route.region_id,
            received_at,
            datetime.now(UTC),
        )

        return JSONResponse(
            status_code=order_status_code(result),
            content=response.model_dump(mode="json"),
        )

    if settings.mode == "gateway":
        app.include_router(gateway)

    return app


app = create_app()

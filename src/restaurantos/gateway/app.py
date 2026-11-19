"""FastAPI application for the RestaurantOS gateway and edge API."""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster
from restaurantos.common.config import get_settings
from restaurantos.common.db import make_pool, node_conninfo, probe_node
from restaurantos.common.errors import (
    RegionUnreachableError,
    StaleEpochError,
    UnknownVendorError,
)
from restaurantos.common.models import (
    OrderRequest,
    OrderResponse,
    OrderStatus,
    StatusChangeRequest,
    StatusResult,
)
from restaurantos.gateway.availability import set_region_availability
from restaurantos.gateway.config import get_gateway_settings
from restaurantos.gateway.directory import DirectoryCache, get_regions
from restaurantos.gateway.dishes import get_dish, search_dishes
from restaurantos.gateway.edge import EdgePrimary, run_edge
from restaurantos.gateway.errors import (
    DishLinkFixedError,
    HQUnreachableError,
    MenuNotFoundError,
    MenuValidationError,
    ProductExistsError,
)
from restaurantos.gateway.menu import get_vendor_menu
from restaurantos.gateway.menu_publish import (
    MenuPublishRequest,
    MenuPublishResponse,
    publish_menu,
)
from restaurantos.gateway.orders import (
    change_region_order_status,
    get_order_status,
    get_vendor_orders,
    order_response,
    order_status_code,
    park_order,
    place_region_order,
    queued_order_response,
    read_region_epoch,
)
from restaurantos.gateway.pools import RegionPools
from restaurantos.gateway.restock import restock_region_vendor
from restaurantos.gateway.stock import get_vendor_stock
from restaurantos.gateway.summary import get_region_summary
from restaurantos.gateway.vendors import search_vendors
from restaurantos.region import (
    change_status,
    place_order,
    restock,
    set_availability,
)


async def refresh_directory_periodically(
    directory: DirectoryCache,
    hq_pool,
    interval_s: float,
) -> None:
    """Refresh the Gateway shard directory on its configured interval."""
    while True:
        await asyncio.sleep(interval_s)

        try:
            await directory.refresh(hq_pool)
        except RegionUnreachableError:
            continue


async def refresh_edge_primary_periodically(
    primary: EdgePrimary,
    interval_s: float,
) -> None:
    """Refresh the edge API's local primary on its configured interval."""
    while True:
        await asyncio.sleep(interval_s)
        await primary.refresh()


def parse_order_statuses(status: str | None) -> list[str] | None:
    """Parse the kitchen-board comma-separated status filter."""
    if status is None:
        return None

    values = status.split(",")
    allowed = {item.value for item in OrderStatus}

    if not values or any(value not in allowed for value in values):
        raise HTTPException(
            status_code=422,
            detail="status must contain only: " + ", ".join(item.value for item in OrderStatus),
        )

    return values


async def run_hq(work):
    """Run HQ work and translate database unavailability."""
    try:
        return await work()
    except RegionUnreachableError as exc:
        raise HQUnreachableError() from exc


class AvailabilityRequest(BaseModel):
    """Manual product availability change."""

    available: bool


class RestockRequest(BaseModel):
    """Optional vendor restock request."""

    items: list[dict[str, object]] | None = None


def create_app() -> FastAPI:
    """Create the gateway or edge FastAPI application."""
    settings = get_settings()
    cluster = load_cluster(settings.cluster_file)
    gateway_settings = get_gateway_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        region_pools = RegionPools(
            cluster,
            settings,
            direct=settings.mode == "edge",
        )
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

            directory_refresh_task = asyncio.create_task(
                refresh_directory_periodically(
                    directory,
                    hq_pool,
                    gateway_settings.directory_refresh_s,
                )
            )
        else:
            if settings.region_id is None:
                raise RuntimeError("ROS_REGION_ID is required in edge mode")

            edge_primary = EdgePrimary(
                cluster,
                settings.region_id,
                settings,
            )
            await edge_primary.refresh()
            app.state.edge_primary = edge_primary

            edge_primary_task = asyncio.create_task(
                refresh_edge_primary_periodically(
                    edge_primary,
                    gateway_settings.local_check_s,
                )
            )

        try:
            yield
        finally:
            if settings.mode == "gateway":
                directory_refresh_task.cancel()

                with suppress(asyncio.CancelledError):
                    await directory_refresh_task

                await app.state.hq_pool.close()
            else:
                edge_primary_task.cancel()

                with suppress(asyncio.CancelledError):
                    await edge_primary_task

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
    edge = APIRouter(prefix="/api/v1")

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

    @app.exception_handler(HQUnreachableError)
    async def hq_unreachable_handler(
        request: Request,
        exc: HQUnreachableError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"error": "hq_unreachable"},
        )

    @app.exception_handler(MenuNotFoundError)
    async def menu_not_found_handler(
        request: Request,
        exc: MenuNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found"},
        )

    @app.exception_handler(MenuValidationError)
    async def menu_validation_handler(
        request: Request,
        exc: MenuValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"error": "invalid_request"},
        )

    @app.exception_handler(ProductExistsError)
    async def product_exists_handler(
        request: Request,
        exc: ProductExistsError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={
                "error": "product_exists",
                "vendor_id": exc.vendor_id,
                "product_id": exc.product_id,
            },
        )

    @app.exception_handler(DishLinkFixedError)
    async def dish_link_fixed_handler(
        request: Request,
        exc: DishLinkFixedError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": "dish_link_fixed"},
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "mode": settings.mode}

    @gateway.get("/regions")
    async def regions(request: Request) -> list[dict[str, object]]:
        async def work():
            return await get_regions(request.app.state.hq_pool)

        return await run_hq(work)

    @gateway.get("/vendors")
    async def vendor_search(
        geohash: str,
        request: Request,
        q: str | None = None,
    ) -> list[dict[str, object]]:
        route = request.app.state.directory.cell_routes.get(geohash)

        if route is None:
            return []

        pool = await request.app.state.region_pools.get(route.primary_node)

        return await search_vendors(
            pool,
            geohash,
            q,
            route.region_id,
        )

    @edge.get("/vendors")
    async def edge_vendor_search(
        geohash: str,
        request: Request,
        q: str | None = None,
    ) -> list[dict[str, object]]:
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        # The edge has no HQ directory. Its local vendors table is its ownership
        # boundary, so a cell with no local vendors simply produces an empty list.
        async def work(pool):
            return await search_vendors(
                pool,
                geohash,
                q,
                region_id,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

    @gateway.get("/dishes")
    async def dish_search(
        q: str,
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
    ) -> list[dict[str, object]]:
        async def work():
            return await search_dishes(
                request.app.state.hq_pool,
                q,
                limit,
            )

        return await run_hq(work)

    @gateway.get("/dishes/{dish_id}")
    async def dish_lookup(
        dish_id: str,
        request: Request,
    ):
        async def work():
            return await get_dish(
                request.app.state.hq_pool,
                dish_id,
            )

        dish = await run_hq(work)

        if dish is None:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found"},
            )

        return dish

    @edge.get("/dishes")
    async def edge_dish_search(
        q: str,
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
    ) -> list[dict[str, object]]:

        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            return await search_dishes(
                pool,
                q,
                limit,
                region_id=region_id,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

    @edge.get("/dishes/{dish_id}")
    async def edge_dish_lookup(
        dish_id: str,
        request: Request,
    ):
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            return await get_dish(
                pool,
                dish_id,
                region_id=region_id,
            )

        dish = await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

        if dish is None:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found"},
            )

        return dish

    @gateway.get("/vendors/{vendor_id}/stock")
    async def vendor_stock(
        vendor_id: str,
        request: Request,
    ) -> list[dict[str, object]]:
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_vendor_stock(
            pool,
            vendor_id,
            route.region_id,
        )

    @edge.get("/vendors/{vendor_id}/stock")
    async def edge_vendor_stock(
        vendor_id: str,
        request: Request,
    ) -> list[dict[str, object]]:
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            return await get_vendor_stock(
                pool,
                vendor_id,
                region_id,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

    @gateway.post("/vendors/{vendor_id}/restock")
    async def restock_vendor(
        vendor_id: str,
        request: Request,
        body: RestockRequest | None = None,
    ) -> dict[str, object]:
        if body is not None and body.items is not None:
            return JSONResponse(
                status_code=501,
                content={"error": "not_implemented"},
            )
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        try:
            result = await restock_region_vendor(
                pool,
                vendor_id,
                route.region_id,
                route.epoch,
                restock,
            )
        except StaleEpochError:
            await request.app.state.directory.refresh(request.app.state.hq_pool)

            route = request.app.state.directory.route_vendor(vendor_id)
            pool = await request.app.state.region_pools.get(route.primary_node)

            result = await restock_region_vendor(
                pool,
                vendor_id,
                route.region_id,
                route.epoch,
                restock,
            )

        return {
            "vendor_id": vendor_id,
            "region_id": route.region_id,
            "counters_restocked": result.counters_restocked,
            "products_back_on": result.products_back_on,
        }

    @edge.post("/vendors/{vendor_id}/restock")
    async def edge_restock_vendor(
        vendor_id: str,
        request: Request,
        body: RestockRequest | None = None,
    ) -> dict[str, object]:
        if body is not None and body.items is not None:
            return JSONResponse(
                status_code=501,
                content={"error": "not_implemented"},
            )
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            epoch = await read_region_epoch(
                pool,
                region_id,
            )

            result = await restock_region_vendor(
                pool,
                vendor_id,
                region_id,
                epoch,
                restock,
            )

            return {
                "vendor_id": vendor_id,
                "region_id": region_id,
                "counters_restocked": result.counters_restocked,
                "products_back_on": result.products_back_on,
            }

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

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

    @edge.get("/vendors/{vendor_id}/menu")
    async def edge_vendor_menu(
        vendor_id: str,
        request: Request,
    ) -> list[dict[str, object]]:
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            return await get_vendor_menu(
                pool,
                vendor_id,
                region_id,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
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
        day: int | None = None,
    ) -> dict[str, object]:
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_order_status(
            pool,
            vendor_id,
            order_id,
            route.region_id,
            day=day,
        )

    @gateway.post(
        "/vendors/{vendor_id}/orders/{order_id}/status",
        response_model=StatusResult,
        responses={
            404: {"description": "Vendor or order not found."},
            409: {"description": "Status change conflicts with the current state."},
        },
    )
    async def change_order_status(
        vendor_id: str,
        order_id: UUID,
        body: StatusChangeRequest,
        request: Request,
    ):
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        try:
            result = await change_region_order_status(
                pool,
                vendor_id,
                order_id,
                body,
                route.region_id,
                route.epoch,
                change_status,
            )
        except StaleEpochError:
            await request.app.state.directory.refresh(request.app.state.hq_pool)

            route = request.app.state.directory.route_vendor(vendor_id)
            pool = await request.app.state.region_pools.get(route.primary_node)

            result = await change_region_order_status(
                pool,
                vendor_id,
                order_id,
                body,
                route.region_id,
                route.epoch,
                change_status,
            )

        if result.conflict:
            return JSONResponse(
                status_code=409,
                content=result.model_dump(mode="json"),
            )

        return result

    @edge.get("/vendors/{vendor_id}/orders/{order_id}")
    async def edge_order_status(
        vendor_id: str,
        order_id: UUID,
        request: Request,
        day: int | None = None,
    ) -> dict[str, object]:
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            return await get_order_status(
                pool,
                vendor_id,
                order_id,
                region_id,
                day=day,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

    @edge.post(
        "/vendors/{vendor_id}/orders/{order_id}/status",
        response_model=StatusResult,
        responses={
            404: {"description": "Vendor or order not found."},
            409: {"description": "Status change conflicts with the current state."},
        },
    )
    async def edge_change_order_status(
        vendor_id: str,
        order_id: UUID,
        body: StatusChangeRequest,
        request: Request,
    ):
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            epoch = await read_region_epoch(
                pool,
                region_id,
            )

            result = await change_region_order_status(
                pool,
                vendor_id,
                order_id,
                body,
                region_id,
                epoch,
                change_status,
            )

            if result.conflict:
                return JSONResponse(
                    status_code=409,
                    content=result.model_dump(mode="json"),
                )

            return result

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

    @gateway.get("/vendors/{vendor_id}/orders")
    async def vendor_orders(
        vendor_id: str,
        day: int,
        request: Request,
        status: str | None = None,
    ) -> list[dict[str, object]]:
        statuses = parse_order_statuses(status)

        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        return await get_vendor_orders(
            pool,
            vendor_id,
            day,
            statuses,
            route.region_id,
        )

    @edge.get("/vendors/{vendor_id}/orders")
    async def edge_vendor_orders(
        vendor_id: str,
        day: int,
        request: Request,
        status: str | None = None,
    ) -> list[dict[str, object]]:
        statuses = parse_order_statuses(status)

        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            return await get_vendor_orders(
                pool,
                vendor_id,
                day,
                statuses,
                region_id,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
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

    @edge.get("/regions/{region_id}/summary")
    async def edge_region_summary(
        region_id: str,
        request: Request,
        day: int,
    ) -> dict[str, object]:
        if region_id != settings.region_id:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found"},
            )

        async def work(pool):
            return await get_region_summary(
                pool,
                region_id,
                day,
            )

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
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

    @edge.post(
        "/orders",
        response_model=OrderResponse,
        status_code=201,
        responses={
            200: {
                "model": OrderResponse,
                "description": "Duplicate order; same result as the original request.",
            },
            404: {
                "description": "Vendor does not belong to this region.",
                "content": {
                    "application/json": {
                        "example": {"error": "not_found"},
                    }
                },
            },
            409: {
                "model": OrderResponse,
                "description": "Order refused.",
            },
            503: {
                "description": "Local region unavailable.",
            },
        },
    )
    async def edge_create_order(
        order: OrderRequest,
        request: Request,
    ) -> OrderResponse:
        received_at = datetime.now(UTC)

        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            epoch = await read_region_epoch(
                pool,
                region_id,
            )

            return await place_region_order(
                pool,
                order,
                region_id,
                epoch,
                received_at,
                place_order,
            )

        try:
            result = await run_edge(
                request.app.state.edge_primary,
                request.app.state.region_pools,
                work,
            )
        except StaleEpochError:
            try:
                result = await run_edge(
                    request.app.state.edge_primary,
                    request.app.state.region_pools,
                    work,
                )
            except StaleEpochError as exc:
                raise RegionUnreachableError(
                    region_id=region_id,
                ) from exc

        response = order_response(
            result,
            region_id,
            received_at,
            datetime.now(UTC),
        )

        return JSONResponse(
            status_code=order_status_code(result),
            content=response.model_dump(mode="json"),
        )

    @gateway.post(
        "/menu/products",
        response_model=MenuPublishResponse,
        status_code=202,
        responses={
            404: {
                "description": "Vendor, product, or dish not found.",
                "content": {
                    "application/json": {
                        "example": {"error": "not_found"},
                    }
                },
            },
            409: {
                "description": "Product already exists or its dish link cannot change.",
                "content": {
                    "application/json": {
                        "examples": {
                            "product_exists": {
                                "value": {
                                    "error": "product_exists",
                                    "vendor_id": "v001",
                                    "product_id": "p001",
                                }
                            },
                            "dish_link_fixed": {"value": {"error": "dish_link_fixed"}},
                        }
                    }
                },
            },
        },
    )
    async def publish_menu_products(
        body: MenuPublishRequest,
        request: Request,
    ) -> MenuPublishResponse:
        async def work():
            return await publish_menu(
                request.app.state.hq_pool,
                body,
            )

        return await run_hq(work)

    @gateway.put(
        "/vendors/{vendor_id}/products/{product_id}/availability",
    )
    async def update_product_availability(
        vendor_id: str,
        product_id: str,
        body: AvailabilityRequest,
        request: Request,
    ) -> dict[str, object]:
        route = request.app.state.directory.route_vendor(vendor_id)
        pool = await request.app.state.region_pools.get(route.primary_node)

        try:
            state = await set_region_availability(
                pool,
                vendor_id,
                product_id,
                body.available,
                route.region_id,
                route.epoch,
                set_availability,
            )
        except StaleEpochError:
            await request.app.state.directory.refresh(request.app.state.hq_pool)

            route = request.app.state.directory.route_vendor(vendor_id)
            pool = await request.app.state.region_pools.get(route.primary_node)

            state = await set_region_availability(
                pool,
                vendor_id,
                product_id,
                body.available,
                route.region_id,
                route.epoch,
                set_availability,
            )

        return {
            "vendor_id": vendor_id,
            "product_id": product_id,
            "available": state.available,
            "reason": state.reason,
        }

    @edge.put(
        "/vendors/{vendor_id}/products/{product_id}/availability",
    )
    async def edge_update_product_availability(
        vendor_id: str,
        product_id: str,
        body: AvailabilityRequest,
        request: Request,
    ) -> dict[str, object]:
        region_id = settings.region_id
        if region_id is None:
            raise RuntimeError("ROS_REGION_ID is required in edge mode")

        async def work(pool):
            epoch = await read_region_epoch(
                pool,
                region_id,
            )

            state = await set_region_availability(
                pool,
                vendor_id,
                product_id,
                body.available,
                region_id,
                epoch,
                set_availability,
            )

            return {
                "vendor_id": vendor_id,
                "product_id": product_id,
                "available": state.available,
                "reason": state.reason,
            }

        return await run_edge(
            request.app.state.edge_primary,
            request.app.state.region_pools,
            work,
        )

    if settings.mode == "gateway":
        app.include_router(gateway)
    else:
        app.include_router(edge)

    return app


app = create_app()

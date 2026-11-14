"""FastAPI application for the RestaurantOS gateway and edge API."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from restaurantos.common.cluster import HQ_PRIMARY, load_cluster
from restaurantos.common.config import get_settings
from restaurantos.common.db import make_pool, node_conninfo
from restaurantos.common.errors import RegionUnreachableError, UnknownVendorError
from restaurantos.gateway.directory import get_regions


def create_app() -> FastAPI:
    """Create the gateway or edge FastAPI application."""
    settings = get_settings()
    cluster = load_cluster(settings.cluster_file)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.mode == "gateway":
            hq_pool = make_pool(
                node_conninfo(cluster, HQ_PRIMARY, settings=settings),
                settings,
            )
            await hq_pool.open()
            app.state.hq_pool = hq_pool

        yield

        if settings.mode == "gateway":
            await app.state.hq_pool.close()

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

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "mode": settings.mode}

    @app.get("/api/v1/regions")
    async def regions(request: Request):
        return await get_regions(request.app.state.hq_pool)

    return app


app = create_app()

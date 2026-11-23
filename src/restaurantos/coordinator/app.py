"""Coordinator admin reads from architecture Section 6.2."""

from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from restaurantos.common.cluster import load_cluster
from restaurantos.common.config import get_settings
from restaurantos.common.errors import RegionUnreachableError
from restaurantos.coordinator.config import CoordinatorSettings
from restaurantos.coordinator.models import ClusterEvent, ClusterStatus, ExceptionEntry
from restaurantos.coordinator.service import Coordinator


def create_app(coordinator: Coordinator | None = None) -> FastAPI:
    if coordinator is None:
        settings = get_settings()
        coordinator = Coordinator(
            load_cluster(settings.cluster_file), settings, CoordinatorSettings()
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            await coordinator.start()
            yield
        finally:
            await coordinator.close()

    app = FastAPI(
        title="RestaurantOS coordinator",
        description="Node health monitoring and admin reads. Promotion is a follow-up increment.",
        lifespan=lifespan,
    )

    @app.exception_handler(RegionUnreachableError)
    async def unavailable(request, exc: RegionUnreachableError):
        return JSONResponse(
            status_code=503,
            content={"error": "region_unreachable", "region_id": exc.region_id},
        )

    @app.get("/admin/cluster", response_model=ClusterStatus)
    async def cluster() -> ClusterStatus:
        return coordinator.snapshot()

    @app.get("/admin/events", response_model=list[ClusterEvent])
    async def events(
        since: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> list[ClusterEvent]:
        return await coordinator.events(since, limit)

    @app.get("/admin/exceptions", response_model=list[ExceptionEntry])
    async def exceptions(
        since: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> list[ExceptionEntry]:
        return await coordinator.exceptions(since, limit)

    return app


app = create_app()

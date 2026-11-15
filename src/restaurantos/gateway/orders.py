"""Order routing and holding-queue operations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.models import (
    OrderOutcome,
    OrderRequest,
    OrderResponse,
    OrderResult,
)
from restaurantos.common.sqlfiles import load_query

PlaceOrder = Callable[..., Awaitable[OrderResult]]


def order_response(
    result: OrderResult,
    region_id: str,
    received_at: datetime,
    acked_at: datetime,
) -> OrderResponse:
    """Build the public response for a completed regional order."""

    return OrderResponse(
        **result.model_dump(),
        region_id=region_id,
        received_at=received_at,
        acked_at=acked_at,
    )


def queued_order_response(
    region_id: str,
    received_at: datetime,
) -> OrderResponse:
    """Build the public response for an order parked at HQ."""

    return OrderResponse(
        outcome=None,
        queued=True,
        region_id=region_id,
        received_at=received_at,
        acked_at=datetime.now(UTC),
    )


def order_status_code(result: OrderResult) -> int:
    """HTTP status for a completed regional order."""

    if result.outcome == OrderOutcome.REFUSED:
        return 409

    if result.duplicate:
        return 200

    return 201


async def park_order(
    pool: AsyncConnectionPool,
    request: OrderRequest,
    region_id: str,
    received_at: datetime,
) -> None:
    """Store an unreachable order in HQ's durable holding queue."""

    async def work(conn) -> None:
        await conn.execute(
            load_query("gateway/park_order"),
            {
                "region_id": region_id,
                "channel": request.channel,
                "external_order_id": request.external_order_id,
                "request": Jsonb(request.model_dump(mode="json")),
                "received_at": received_at,
            },
        )

    await run_on(pool, work)


async def place_region_order(
    pool: AsyncConnectionPool,
    request: OrderRequest,
    region_id: str,
    epoch: int,
    received_at: datetime,
    place_order: PlaceOrder,
) -> OrderResult:
    """Run WP1's order transaction on the owning region."""

    async def work(conn: psycopg.AsyncConnection) -> OrderResult:
        return await place_order(
            conn,
            request,
            epoch,
            received_at=received_at,
        )

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

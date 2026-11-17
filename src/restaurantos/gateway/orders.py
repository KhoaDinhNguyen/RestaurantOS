"""Order routing and holding-queue operations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.errors import UnknownVendorError
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


async def get_order_items(
    conn: psycopg.AsyncConnection,
    order_day: int,
    order_id: UUID,
) -> list[dict[str, object]]:
    """Read an order's lines in display order."""
    cursor = await conn.execute(
        load_query("gateway/get_order_items"),
        {
            "order_day": order_day,
            "order_id": order_id,
        },
    )
    rows = await cursor.fetchall()

    return [
        {
            "line_no": row[0],
            "product_id": row[1],
            "name": row[2],
            "quantity": row[3],
            "price_units": row[4],
        }
        for row in rows
    ]


async def get_order_status(
    pool: AsyncConnectionPool,
    vendor_id: str,
    order_id: UUID,
    region_id: str,
    day: int | None = None,
) -> dict[str, object]:
    """Read an order's current status from its owning region."""

    async def work(conn: psycopg.AsyncConnection) -> dict[str, object]:
        cursor = await conn.execute(
            load_query("gateway/get_order_status"),
            {
                "vendor_id": vendor_id,
                "order_id": order_id,
                "day": day,
            },
        )
        row = await cursor.fetchone()

        if row is None:
            raise UnknownVendorError(f"unknown order {order_id}")

        items = await get_order_items(
            conn,
            row[1],
            row[0],
        )

        return {
            "order_id": row[0],
            "order_day": row[1],
            "order_time": row[2],
            "vendor_id": row[3],
            "customer_id": row[4],
            "channel": row[5],
            "status": row[6],
            "status_changed_at": row[7],
            "menu_version": row[8],
            "received_at": row[9],
            "committed_at": row[10],
            "items": items,
        }

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def get_vendor_orders(
    pool: AsyncConnectionPool,
    vendor_id: str,
    day: int,
    statuses: list[str] | None,
    region_id: str,
) -> list[dict[str, object]]:
    """Read one vendor's kitchen board from its owning region."""

    async def work(conn: psycopg.AsyncConnection) -> list[dict[str, object]]:
        cursor = await conn.execute(
            load_query("gateway/get_vendor_orders"),
            {
                "vendor_id": vendor_id,
                "day": day,
                "statuses": statuses,
            },
        )
        rows = await cursor.fetchall()

        result = []

        for row in rows:
            items = await get_order_items(
                conn,
                row[1],
                row[0],
            )

            result.append(
                {
                    "order_id": row[0],
                    "order_day": row[1],
                    "order_time": row[2],
                    "vendor_id": row[3],
                    "customer_id": row[4],
                    "channel": row[5],
                    "status": row[6],
                    "status_changed_at": row[7],
                    "menu_version": row[8],
                    "received_at": row[9],
                    "committed_at": row[10],
                    "items": items,
                }
            )

        return result

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

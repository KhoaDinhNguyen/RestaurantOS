"""Order routing and holding-queue operations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.errors import RegionUnreachableError, UnknownVendorError
from restaurantos.common.models import (
    OrderOutcome,
    OrderRequest,
    OrderResponse,
    OrderResult,
    StatusChangeRequest,
    StatusResult,
)
from restaurantos.common.sqlfiles import load_query

PlaceOrder = Callable[..., Awaitable[OrderResult]]
ChangeStatus = Callable[..., Awaitable[StatusResult]]


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


async def read_region_epoch(
    pool: AsyncConnectionPool,
    region_id: str,
) -> int:
    """Read the epoch advertised by a regional node."""

    async def work(conn: psycopg.AsyncConnection) -> int:
        cursor = await conn.execute(load_query("gateway/read_epoch"))
        row = await cursor.fetchone()

        if row is None:
            raise RegionUnreachableError(
                "no local epoch",
                region_id=region_id,
            )

        return row[0]

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )


async def change_region_order_status(
    pool: AsyncConnectionPool,
    vendor_id: str,
    order_id: UUID,
    request: StatusChangeRequest,
    region_id: str,
    epoch: int,
    change_status: ChangeStatus,
) -> StatusResult:
    """Run WP1's status-change transaction on the owning region."""

    async def work(conn: psycopg.AsyncConnection) -> StatusResult:
        return await change_status(
            conn,
            vendor_id,
            order_id,
            request.event_id,
            request.to_status,
            epoch,
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

        if not rows:
            return []

        order_ids = [row[0] for row in rows]

        items_cursor = await conn.execute(
            load_query("gateway/get_order_items_bulk"),
            {
                "vendor_id": vendor_id,
                "order_day": day,
                "order_ids": order_ids,
            },
        )
        item_rows = await items_cursor.fetchall()

        items_by_order: dict[tuple[object, object], list[dict[str, object]]] = {}

        for item in item_rows:
            key = (item[0], item[1])
            items_by_order.setdefault(key, []).append(
                {
                    "line_no": item[2],
                    "product_id": item[3],
                    "name": item[4],
                    "quantity": item[5],
                    "price_units": item[6],
                }
            )

        result = []

        for row in rows:
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
                    "items": items_by_order.get((row[1], row[0]), []),
                }
            )

        return result

    return await run_on(
        pool,
        work,
        region_id=region_id,
    )

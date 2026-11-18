"""Changing an order's status: one local transaction on the region primary.

The steps are numbered as in docs/work-packages/wp1-region-write-path.md,
"change_status". The state machine is common.models.ALLOWED_TRANSITIONS, and
the first committed change wins (architecture Section 5.2).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from restaurantos.common.errors import UnknownVendorError
from restaurantos.common.models import (
    EventType,
    ExceptionLoggedPayload,
    OrderStatus,
    StatusChangedPayload,
    StatusResult,
    can_transition,
)
from restaurantos.common.sqlfiles import load_query
from restaurantos.region.orders import assert_epoch

type Row = dict[str, Any]
type Cursor = psycopg.AsyncCursor[Row]


async def change_status(
    conn: psycopg.AsyncConnection,
    vendor_id: str,
    order_id: UUID,
    event_id: UUID,
    to_status: OrderStatus,
    epoch: int,
) -> StatusResult:
    """Move one order to `to_status`, or log the move as a conflict.

    Safe to repeat with the same event_id. Returning commits; raising rolls
    everything back. Raises UnknownVendorError when this vendor has no such
    order on this node (the gateway answers 404, as for an unknown vendor).
    A stale epoch raises psycopg's ROS01 error, which run_on turns into
    StaleEpochError.
    """
    to_status = OrderStatus(to_status)

    async with conn.transaction():
        cur = conn.cursor(row_factory=dict_row)

        # 1. The epoch fence, before anything is read or written.
        await assert_epoch(cur, epoch)

        # 2. Record the event; if it was processed before, answer with the
        #    order as it is now and change nothing.
        await cur.execute(
            load_query("region/claim_event"),
            {"event_id": event_id, "order_id": order_id},
        )
        first_time = await cur.fetchone() is not None

        order = await _lock_order(cur, vendor_id, order_id)
        current = OrderStatus(order["status"])

        if not first_time:
            return StatusResult(order_id=order_id, status=current, applied=False, duplicate=True)

        # 3. Check the move against the state machine.
        if can_transition(current, to_status):
            # 4. Allowed: change the order and tell HQ.
            await _apply(cur, order_id, order, current, to_status)
            return StatusResult(order_id=order_id, status=to_status, applied=True)

        # 5. Not allowed: the order stays as it is; log both versions.
        await _log_conflict(cur, order_id, event_id, order, current, to_status)
        return StatusResult(order_id=order_id, status=current, applied=False, conflict=True)


async def _lock_order(cur: Cursor, vendor_id: str, order_id: UUID) -> Row:
    await cur.execute(
        load_query("region/lock_order"),
        {"vendor_id": vendor_id, "order_id": order_id},
    )
    order = await cur.fetchone()
    if order is None:
        raise UnknownVendorError(f"vendor {vendor_id} has no order {order_id} on this node")
    return order


async def _apply(
    cur: Cursor,
    order_id: UUID,
    order: Row,
    current: OrderStatus,
    to_status: OrderStatus,
) -> None:
    await cur.execute(
        load_query("region/update_order_status"),
        {"order_day": order["order_day"], "order_id": order_id, "to_status": to_status.value},
    )
    updated = await cur.fetchone()
    if updated is None:
        raise RuntimeError(f"order {order_id} vanished while locked")

    payload = StatusChangedPayload(
        order_id=order_id,
        order_day=order["order_day"],
        from_status=current,
        to_status=to_status,
        changed_at=updated["status_changed_at"],
    )
    await _outbox(cur, EventType.ORDER_STATUS_CHANGED, order["order_day"], payload)


async def _log_conflict(
    cur: Cursor,
    order_id: UUID,
    event_id: UUID,
    order: Row,
    current: OrderStatus,
    to_status: OrderStatus,
) -> None:
    await cur.execute(
        load_query("region/insert_exception"),
        {
            "event_id": event_id,
            "order_day": order["order_day"],
            "order_id": order_id,
            "current_status": current.value,
            "current_changed_at": order["status_changed_at"],
            "attempted_status": to_status.value,
        },
    )
    logged = await cur.fetchone()
    if logged is None:
        raise RuntimeError("INSERT ... RETURNING gave no row")

    payload = ExceptionLoggedPayload(
        order_id=order_id,
        order_day=order["order_day"],
        kind=logged["kind"],
        current_status=current,
        current_changed_at=order["status_changed_at"],
        attempted_status=to_status,
        attempted_at=logged["attempted_at"],
        detail=logged["detail"],
    )
    await _outbox(cur, EventType.EXCEPTION_LOGGED, order["order_day"], payload)


async def _outbox(
    cur: Cursor,
    event_type: EventType,
    order_day: int,
    payload: StatusChangedPayload | ExceptionLoggedPayload,
) -> None:
    await cur.execute(
        load_query("region/insert_outbox"),
        {
            "event_type": event_type.value,
            "order_day": order_day,
            "payload": Jsonb(payload.model_dump(mode="json")),
        },
    )

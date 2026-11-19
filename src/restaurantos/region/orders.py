"""Placing an order: one local transaction on the region primary.

The steps are numbered as in docs/work-packages/wp1-region-write-path.md.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from restaurantos.common.errors import UnknownVendorError
from restaurantos.common.models import (
    EventType,
    OrderCreatedPayload,
    OrderItemRow,
    OrderOutcome,
    OrderRequest,
    OrderResult,
    OrderStatus,
    RefusalReason,
)
from restaurantos.common.sqlfiles import load_query

type Row = dict[str, Any]
type Cursor = psycopg.AsyncCursor[Row]
type StockKey = tuple[str, str]  # (item_kind, item_id)


async def place_order(
    conn: psycopg.AsyncConnection,
    req: OrderRequest,
    epoch: int,
    *,
    received_at: datetime | None = None,
) -> OrderResult:
    """Place one order, or refuse it whole. Safe to repeat with the same order.

    Returning commits; raising rolls everything back, the inbox row included.
    Raises UnknownVendorError when the vendor is not on this node. A stale
    epoch raises psycopg's ROS01 error, which run_on turns into StaleEpochError.
    """
    async with conn.transaction():
        cur = conn.cursor(row_factory=dict_row)

        # 1. The epoch fence, before anything is read or written.
        await assert_epoch(cur, epoch)

        # Reject a vendor that does not belong to this region before checking
        # the idempotency key. A foreign vendor must never reuse the stored
        # outcome of a local order with the same channel/external_order_id.
        await _require_vendor(cur, req.vendor_id)

        # 2. Claim the order in the inbox; if it was seen before, answer as then.

        # 2. Claim the order in the inbox; if it was seen before, answer as then.
        order_id = uuid4()
        await cur.execute(
            load_query("region/inbox_claim"),
            {
                "channel": req.channel,
                "external_order_id": req.external_order_id,
                "order_day": req.order_day,
                "order_id": order_id,
            },
        )
        if await cur.fetchone() is None:
            return await _stored_outcome(cur, req)

        # 3. Every product, looked up for this vendor.
        products = await _look_up_products(cur, req)
        reason = _check_products(req, products)
        if reason is not None:
            return await _refuse(cur, req, reason)

        # 4. What the order consumes, per stock counter.
        needs = await _needs(cur, req, products)

        if needs:
            # 5. Lock the counters in a fixed order.
            on_hand = await _lock_stock(cur, req.vendor_id, needs)
            # 6. One short counter refuses the whole order. No row means zero.
            if any(on_hand.get(key, 0) < need for key, need in needs.items()):
                return await _refuse(cur, req, RefusalReason.OUT_OF_STOCK)
            # 7. Take the stock; mark products that can no longer make one unit.
            await _take_stock(cur, req.vendor_id, needs)

        # 8. The order and its lines; each line keeps today's price, and the
        #    order the menu version those prices were read from.
        menu_version = products[req.items[0].product_id]["menu_version"]
        lines = [
            OrderItemRow(
                line_no=line_no,
                product_id=item.product_id,
                quantity=item.quantity,
                price_units=products[item.product_id]["price_units"],
            )
            for line_no, item in enumerate(req.items, start=1)
        ]
        order = await _insert_order(cur, req, order_id, epoch, menu_version, received_at, lines)

        # 9. The event for HQ, committed together with the order.
        status = OrderStatus(order["status"])
        payload = OrderCreatedPayload(
            order_id=order_id,
            order_day=req.order_day,
            vendor_id=req.vendor_id,
            channel=req.channel,
            external_order_id=req.external_order_id,
            customer_id=req.customer_id,
            customer_geohash=req.customer_geohash,
            order_time=req.order_time,
            status=status,
            menu_version=order["menu_version"],
            received_at=received_at,
            committed_at=order["committed_at"],
            items=lines,
        )
        await cur.execute(
            load_query("region/insert_outbox"),
            {
                "event_type": EventType.ORDER_CREATED.value,
                "order_day": req.order_day,
                "payload": Jsonb(payload.model_dump(mode="json")),
            },
        )

        return OrderResult(
            outcome=OrderOutcome.ACCEPTED,
            order_id=order_id,
            order_day=req.order_day,
            status=status,
            menu_version=order["menu_version"],
            committed_at=order["committed_at"],
        )


async def assert_epoch(cur: Cursor, epoch: int) -> None:
    """Step 1 of every WP1 function. Move it to a shared module once a second one needs it."""
    await cur.execute(load_query("region/assert_epoch"), {"epoch": epoch})


# --- step 2: idempotency ---------------------------------------------------


async def _stored_outcome(cur: Cursor, req: OrderRequest) -> OrderResult:
    """The answer the first attempt got. Reads only; writes nothing."""
    await cur.execute(
        load_query("region/inbox_get"),
        {"channel": req.channel, "external_order_id": req.external_order_id},
    )
    inbox = await cur.fetchone()
    if inbox is None:
        raise RuntimeError("inbox conflict without an inbox row")

    if inbox["outcome"] == OrderOutcome.REFUSED:
        return OrderResult(
            outcome=OrderOutcome.REFUSED,
            duplicate=True,
            order_day=inbox["order_day"],
            refusal_reason=RefusalReason(inbox["refusal_reason"]),
        )

    await cur.execute(
        load_query("region/get_order"),
        {"order_day": inbox["order_day"], "order_id": inbox["order_id"]},
    )
    order = await cur.fetchone()
    if order is None:
        raise RuntimeError(f"accepted inbox row without order {inbox['order_id']}")

    return OrderResult(
        outcome=OrderOutcome.ACCEPTED,
        duplicate=True,
        order_id=inbox["order_id"],
        order_day=inbox["order_day"],
        # The same answer as the first time (architecture Section 6.1: a 200
        # duplicate has the first body). Every order starts as received; the
        # current status is what GET /vendors/{id}/orders/{order_id} is for.
        status=OrderStatus.RECEIVED,
        menu_version=order["menu_version"],
        committed_at=order["committed_at"],
    )


# --- step 3: products ------------------------------------------------------


async def _look_up_products(cur: Cursor, req: OrderRequest) -> dict[str, Row]:
    await cur.execute(
        load_query("region/order_products"),
        {
            "vendor_id": req.vendor_id,
            "product_ids": sorted({item.product_id for item in req.items}),
        },
    )
    return {row["product_id"]: row for row in await cur.fetchall()}


def _check_products(req: OrderRequest, products: dict[str, Row]) -> RefusalReason | None:
    """The step-3 refusal, or None. Unknown is checked before unavailable. No I/O."""
    for item in req.items:
        row = products.get(item.product_id)
        if row is None or row["dish_id"] is None:
            return RefusalReason.UNKNOWN_PRODUCT
    for item in req.items:
        if not products[item.product_id]["available"]:
            return RefusalReason.UNAVAILABLE
    return None


async def _require_vendor(cur: Cursor, vendor_id: str) -> None:
    """Raise when the vendor is not on this node.

    Only called when a product is unknown: products has a foreign key to
    vendors, so a missing vendor always shows up as unknown products first.
    """
    await cur.execute(load_query("region/vendor_exists"), {"vendor_id": vendor_id})
    if await cur.fetchone() is None:
        raise UnknownVendorError(f"vendor {vendor_id} is not on this node")


async def _refuse(cur: Cursor, req: OrderRequest, reason: RefusalReason) -> OrderResult:
    """Turn the inbox row into a refusal. Nothing else is written."""
    await cur.execute(
        load_query("region/inbox_refuse"),
        {
            "channel": req.channel,
            "external_order_id": req.external_order_id,
            "reason": reason.value,
        },
    )
    return OrderResult(
        outcome=OrderOutcome.REFUSED,
        order_day=req.order_day,
        refusal_reason=reason,
    )


# --- step 4: what the order consumes --------------------------------------


async def _needs(cur: Cursor, req: OrderRequest, products: dict[str, Row]) -> dict[StockKey, int]:
    """Total need per counter; amount and multiplier always come from the tables."""
    needs: dict[StockKey, int] = defaultdict(int)
    dish_units: dict[str, int] = defaultdict(int)  # per_ingredient dish -> units ordered

    for item in req.items:
        product = products[item.product_id]
        units = product["multiplier"] * item.quantity
        match product["stock_mode"]:
            case "per_item":
                needs[("dish", product["dish_id"])] += units
            case "per_ingredient":
                dish_units[product["dish_id"]] += units
            case "none":
                pass
            case other:
                raise ValueError(f"unknown stock_mode {other!r}")

    if dish_units:
        await cur.execute(load_query("region/recipes"), {"dish_ids": sorted(dish_units)})
        for row in await cur.fetchall():
            needs[("ingredient", row["ingredient_id"])] += (
                row["amount"] * dish_units[row["dish_id"]]
            )

    return dict(needs)


# --- steps 5 to 7: stock ---------------------------------------------------


async def _lock_stock(
    cur: Cursor, vendor_id: str, needs: dict[StockKey, int]
) -> dict[StockKey, int]:
    keys = sorted(needs)
    await cur.execute(
        load_query("region/lock_stock"),
        {
            "vendor_id": vendor_id,
            "item_kinds": [kind for kind, _ in keys],
            "item_ids": [item_id for _, item_id in keys],
        },
    )
    return {(row["item_kind"], row["item_id"]): row["on_hand"] for row in await cur.fetchall()}


async def _take_stock(cur: Cursor, vendor_id: str, needs: dict[StockKey, int]) -> None:
    """Take the stock, then mark the products that can no longer make one unit.

    Only the counters this order took from are checked: they are the ones that
    went down, and step 5 already holds them, so the marking takes no new locks.
    Every one of them has a row, because step 6 refused an order with a missing one.
    """
    keys = sorted(needs)
    await cur.execute(
        load_query("region/take_stock"),
        {
            "vendor_id": vendor_id,
            "item_kinds": [kind for kind, _ in keys],
            "item_ids": [item_id for _, item_id in keys],
            "needs": [needs[key] for key in keys],
        },
    )
    await cur.execute(
        load_query("region/mark_out_of_stock"),
        {
            "vendor_id": vendor_id,
            "touched_dishes": [item_id for kind, item_id in keys if kind == "dish"],
            "touched_ingredients": [item_id for kind, item_id in keys if kind == "ingredient"],
        },
    )


# --- step 8: the order -----------------------------------------------------


async def _insert_order(
    cur: Cursor,
    req: OrderRequest,
    order_id: UUID,
    epoch: int,
    menu_version: int,
    received_at: datetime | None,
    lines: list[OrderItemRow],
) -> Row:
    await cur.execute(
        load_query("region/insert_order"),
        {
            "order_day": req.order_day,
            "order_id": order_id,
            "vendor_id": req.vendor_id,
            "channel": req.channel,
            "external_order_id": req.external_order_id,
            "customer_id": req.customer_id,
            "customer_geohash": req.customer_geohash,
            "order_time": req.order_time,
            "menu_version": menu_version,
            "epoch": epoch,
            "received_at": received_at,
        },
    )
    order = await cur.fetchone()
    if order is None:
        raise RuntimeError("INSERT ... RETURNING gave no row")

    await cur.execute(
        load_query("region/insert_order_items"),
        {
            "order_day": req.order_day,
            "order_id": order_id,
            "vendor_id": req.vendor_id,
            "line_nos": [line.line_no for line in lines],
            "product_ids": [line.product_id for line in lines],
            "quantities": [line.quantity for line in lines],
            "prices": [line.price_units for line in lines],
        },
    )
    return order

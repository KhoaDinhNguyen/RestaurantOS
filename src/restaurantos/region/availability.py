"""The manual sold-out switch: one local transaction on the region primary.

WP1 brief, "set_availability"; architecture Section 6.1, PUT
/vendors/{vendor_id}/products/{product_id}/availability. The switch only
touches the `manual` mark. The `stock` mark is set by orders and cleared by
restocks, with one exception: switching on writes a `stock` mark when one unit
cannot be made, because a product that ran out while it was switched off
never got one (the `manual` row was in the way).
"""

from __future__ import annotations

from typing import Any, Literal, NamedTuple

import psycopg
from psycopg.rows import dict_row

from restaurantos.common.errors import UnknownVendorError
from restaurantos.common.sqlfiles import load_query
from restaurantos.region.orders import assert_epoch

type Row = dict[str, Any]
type Cursor = psycopg.AsyncCursor[Row]


class ProductState(NamedTuple):
    """A product's availability after the switch (architecture Section 5.2)."""

    available: bool
    reason: Literal["stock", "manual"] | None
    """Why the product is off; None when it is on."""


async def set_availability(
    conn: psycopg.AsyncConnection,
    vendor_id: str,
    product_id: str,
    available: bool,
    epoch: int,
) -> ProductState:
    """Switch a product off by hand, or take the manual switch off again.

    Sending the same value twice is harmless. Raises UnknownVendorError when
    this vendor does not sell the product (the gateway answers 404). A stale
    epoch raises psycopg's ROS01 error, which run_on turns into StaleEpochError.
    """
    params = {"vendor_id": vendor_id, "product_id": product_id}

    async with conn.transaction():
        cur = conn.cursor(row_factory=dict_row)

        # 1. The epoch fence, before anything is read or written.
        await assert_epoch(cur, epoch)

        # 2. The product must be this vendor's.
        await cur.execute(load_query("region/product_linked"), params)
        if await cur.fetchone() is None:
            raise UnknownVendorError(f"vendor {vendor_id} does not sell {product_id}")

        # 3. The switch.
        if available:
            await _switch_on(cur, params)
        else:
            await cur.execute(load_query("region/set_manual_mark"), params)

        # 4. The product's state now.
        await cur.execute(load_query("region/product_state"), params)
        mark = await cur.fetchone()

    if mark is None or mark["available"]:
        return ProductState(available=True, reason=None)
    return ProductState(available=False, reason=mark["reason"])


async def _switch_on(cur: Cursor, params: Row) -> None:
    """Remove the manual mark; mark `stock` if one unit cannot be made.

    The stock check runs before the delete. The result is the same, and it
    takes the locks in the order place_order and restock take them, counters
    first and the availability row second, so the three cannot deadlock.
    """
    await cur.execute(load_query("region/can_make_one"), params)
    row = await cur.fetchone()
    can_make = row is not None and bool(row["ok"])

    await cur.execute(load_query("region/clear_manual_mark"), params)
    if not can_make:
        await cur.execute(load_query("region/set_stock_mark"), params)

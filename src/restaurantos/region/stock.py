"""Restocking a vendor: one local transaction on the region primary.

Only the no-body restock is built (architecture Section 6.1, POST
/vendors/{vendor_id}/restock): every counter goes up to its par level and
never down, and every `stock` mark of the vendor is cleared. The body form,
adding chosen amounts, is optional to build and is not built.
"""

from __future__ import annotations

from typing import Any, NamedTuple

import psycopg
from psycopg.rows import dict_row

from restaurantos.common.errors import UnknownVendorError
from restaurantos.common.sqlfiles import load_query
from restaurantos.region.orders import assert_epoch

type Row = dict[str, Any]
type Cursor = psycopg.AsyncCursor[Row]


class RestockResult(NamedTuple):
    """What a restock did (architecture Section 5.2)."""

    counters_restocked: int
    """Counters whose on_hand went up."""

    products_back_on: int
    """`stock` marks deleted."""


async def restock(
    conn: psycopg.AsyncConnection,
    vendor_id: str,
    epoch: int,
    *,
    items: list[dict[str, Any]] | None = None,
) -> RestockResult:
    """Top every counter of the vendor up to par and clear its `stock` marks.

    Safe to send twice: the second call changes nothing and returns (0, 0).
    Raises UnknownVendorError when the vendor is not on this node. A stale
    epoch raises psycopg's ROS01 error, which run_on turns into StaleEpochError.
    """
    if items is not None:
        raise NotImplementedError("restock with items is optional to build and is not built")

    async with conn.transaction():
        cur = conn.cursor(row_factory=dict_row)

        # 1. The epoch fence, before anything is read or written.
        await assert_epoch(cur, epoch)

        await cur.execute(load_query("region/vendor_exists"), {"vendor_id": vendor_id})
        if await cur.fetchone() is None:
            raise UnknownVendorError(f"vendor {vendor_id} is not on this node")

        params = {"vendor_id": vendor_id}

        # 2. Lock the vendor's counters in the same order place_order does.
        await cur.execute(load_query("region/lock_vendor_stock"), params)

        # 3. Up to par, never down.
        await cur.execute(load_query("region/top_up_stock"), params)
        counters_restocked = cur.rowcount

        # 4. Then clear the stock marks; manual marks stay.
        await cur.execute(load_query("region/clear_stock_marks"), params)
        products_back_on = cur.rowcount

    return RestockResult(counters_restocked, products_back_on)

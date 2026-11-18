"""Applying a menu version from HQ: one local transaction on the region primary.

WP1 brief, "apply_menu_version"; architecture Sections 5.2 and 6.1
(POST /menu/products). The caller is the menu publisher in hq-worker (WP4).
It sends this region's whole HQ slice under the newest version that touches
it, so a version at or below the node's own is skipped.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.rows import dict_row

from restaurantos.common.errors import RestaurantOSError
from restaurantos.common.sqlfiles import load_query
from restaurantos.region.orders import assert_epoch
from restaurantos.sim.config import get_sim_settings

type Row = dict[str, Any]
type Cursor = psycopg.AsyncCursor[Row]
type Key = tuple[str, str]


class DishLinkChangedError(RestaurantOSError):
    """The slice gives an existing product a different dish or multiplier.

    A product's dish link never changes (architecture Section 6.1), so the
    whole version is refused and nothing of it is applied.
    """


async def apply_menu_version(
    conn: psycopg.AsyncConnection,
    version: int,
    rows: Sequence[Mapping[str, Any]],
    epoch: int,
) -> None:
    """Bring this node's HQ products to `version`, atomically.

    `rows` is the slice the menu publisher reads at HQ: one mapping per
    product with vendor_id, product_id, name, unit_price, price_units,
    menu_version, dish_id, multiplier and multiplier_ambiguous.

    Returning commits; raising rolls everything back. A stale epoch raises
    psycopg's ROS01 error, which run_on turns into StaleEpochError.
    """
    # F, the bootstrap floor (docs/replay-stock-parameters.md). Read from the
    # setting bootstrap uses, ROS_SIM_STOCK_FLOOR, never written as a literal.
    floor = get_sim_settings().stock_floor

    async with conn.transaction():
        cur = conn.cursor(row_factory=dict_row)

        # 1. The epoch fence, before anything is read or written.
        await assert_epoch(cur, epoch)

        # 2. Skip a version this node already has.
        await cur.execute(load_query("region/lock_node_state"))
        state = await cur.fetchone()
        if state is None:
            raise RuntimeError("node_state has no row")
        if version <= state["menu_version"]:
            return

        # 3. Which products exist already. A different dish link refuses the
        #    whole version; products created by the region are left alone.
        existing = await _existing_products(cur, rows)
        _refuse_changed_dish_links(rows, existing)
        hq_rows = [r for r in rows if existing.get(_key(r), {"origin": "hq"})["origin"] == "hq"]
        added = [r for r in hq_rows if _key(r) not in existing]

        # 4. Upsert the products and their dish links.
        if hq_rows:
            await _upsert(cur, hq_rows)

        # 5 and 6. Counters and stock marks, for added products only. A
        #    changed product keeps its counters and marks: what it needs is
        #    the same, because its dish link is the same.
        if added:
            keys = {
                "vendor_ids": [r["vendor_id"] for r in added],
                "product_ids": [r["product_id"] for r in added],
            }
            await cur.execute(load_query("region/add_counters"), {**keys, "floor": floor})
            await cur.execute(load_query("region/mark_added_unavailable"), keys)

        # 7. The node now has this version.
        await cur.execute(load_query("region/set_menu_version"), {"version": version})


def _key(row: Mapping[str, Any]) -> Key:
    return row["vendor_id"], row["product_id"]


async def _existing_products(cur: Cursor, rows: Sequence[Mapping[str, Any]]) -> dict[Key, Row]:
    await cur.execute(
        load_query("region/existing_products"),
        {
            "vendor_ids": [r["vendor_id"] for r in rows],
            "product_ids": [r["product_id"] for r in rows],
        },
    )
    return {_key(row): row for row in await cur.fetchall()}


def _refuse_changed_dish_links(
    rows: Sequence[Mapping[str, Any]], existing: Mapping[Key, Row]
) -> None:
    for row in rows:
        stored = existing.get(_key(row))
        if stored is None or stored["origin"] != "hq":
            continue
        if (stored["dish_id"], stored["multiplier"]) != (row["dish_id"], row["multiplier"]):
            raise DishLinkChangedError(
                f"product {row['vendor_id']}/{row['product_id']} is linked to"
                f" {stored['dish_id']} x {stored['multiplier']}; the slice says"
                f" {row['dish_id']} x {row['multiplier']}"
            )


async def _upsert(cur: Cursor, rows: Sequence[Mapping[str, Any]]) -> None:
    keys = {
        "vendor_ids": [r["vendor_id"] for r in rows],
        "product_ids": [r["product_id"] for r in rows],
    }
    await cur.execute(
        load_query("region/upsert_products"),
        {
            **keys,
            "names": [r["name"] for r in rows],
            "unit_prices": [r["unit_price"] for r in rows],
            "price_units": [r["price_units"] for r in rows],
            "menu_versions": [r["menu_version"] for r in rows],
        },
    )
    await cur.execute(
        load_query("region/upsert_product_dish"),
        {
            **keys,
            "dish_ids": [r["dish_id"] for r in rows],
            "multipliers": [r["multiplier"] for r in rows],
            "ambiguous": [r.get("multiplier_ambiguous", False) for r in rows],
        },
    )

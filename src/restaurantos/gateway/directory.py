"""Shard-directory access for the gateway."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg_pool import AsyncConnectionPool

from restaurantos.common.db import run_on
from restaurantos.common.sqlfiles import load_query


@dataclass(frozen=True)
class RegionRoute:
    """Routing information for one region."""

    region_id: str
    epoch: int
    primary_node: str


@dataclass
class DirectoryCache:
    """In-memory vendor and shard routing directory."""

    vendor_cells: dict[str, str]
    cell_routes: dict[str, RegionRoute]

    def route_vendor(self, vendor_id: str) -> RegionRoute:
        """Return the region route that owns a vendor."""
        from restaurantos.common.errors import UnknownVendorError

        cell = self.vendor_cells.get(vendor_id)
        if cell is None:
            raise UnknownVendorError(f"unknown vendor {vendor_id}")

        route = self.cell_routes.get(cell)
        if route is None:
            raise UnknownVendorError(f"no region owns vendor {vendor_id}")

        return route

    async def refresh(self, pool: AsyncConnectionPool) -> None:
        """Refresh vendor and region routing information from HQ."""

        async def work(conn) -> tuple[dict[str, str], dict[str, RegionRoute]]:
            vendor_cursor = await conn.execute(load_query("gateway/get_vendor_cells"))
            vendor_rows = await vendor_cursor.fetchall()

            region_cursor = await conn.execute(load_query("gateway/get_regions"))
            region_rows = await region_cursor.fetchall()

            vendor_cells = {row[0]: row[1] for row in vendor_rows}

            cell_routes: dict[str, RegionRoute] = {}

            for row in region_rows:
                region_id = row[0]
                epoch = row[1]
                primary_node = row[2]
                geohashes = row[6]

                route = RegionRoute(
                    region_id=region_id,
                    epoch=epoch,
                    primary_node=primary_node,
                )

                for cell in geohashes:
                    cell_routes[cell] = route

            return vendor_cells, cell_routes

        vendor_cells, cell_routes = await run_on(pool, work)

        self.vendor_cells = vendor_cells
        self.cell_routes = cell_routes


async def get_regions(pool: AsyncConnectionPool) -> list[dict[str, Any]]:
    """Read the shard directory and region state from HQ."""

    async def work(conn) -> list[dict[str, Any]]:
        cursor = await conn.execute(load_query("gateway/get_regions"))
        rows = await cursor.fetchall()

        return [
            {
                "region_id": row[0],
                "epoch": row[1],
                "primary_node": row[2],
                "standby_node": row[3],
                "menu_version": row[4],
                "updated_at": row[5],
                "geohashes": row[6],
            }
            for row in rows
        ]

    return await run_on(pool, work)

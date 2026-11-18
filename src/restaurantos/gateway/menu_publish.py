"""HQ menu product publishing."""

from __future__ import annotations

from decimal import Decimal
from hashlib import sha1

import psycopg
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

from restaurantos.common.db import run_on
from restaurantos.common.sqlfiles import load_query
from restaurantos.gateway.errors import (
    DishLinkFixedError,
    MenuNotFoundError,
    MenuValidationError,
    ProductExistsError,
)


class MenuProductInput(BaseModel):
    """One product to create or change at HQ."""

    vendor_id: str
    name: str
    price_units: int = Field(ge=0)
    dish_id: str | None = None
    multiplier: int | None = Field(default=None, ge=1)
    product_id: str | None = None


class MenuPublishRequest(BaseModel):
    """One atomic HQ menu-version request."""

    note: str | None = None
    products: list[MenuProductInput] = Field(min_length=1)


class PublishedProduct(BaseModel):
    """Result for one product in a published version."""

    vendor_id: str
    product_id: str
    region_id: str
    created: bool


class MenuPublishResponse(BaseModel):
    """HQ result after storing one new menu version."""

    menu_version: int
    products: list[PublishedProduct]


def generated_product_id(vendor_id: str, name: str) -> str:
    """Return the deterministic 12-hex product id required by the API."""
    value = f"{vendor_id}|{name}".encode()
    return sha1(value).hexdigest()[:12]


async def publish_menu(
    pool: AsyncConnectionPool,
    request: MenuPublishRequest,
) -> MenuPublishResponse:
    """Publish one atomic menu version at HQ."""

    async def work(conn: psycopg.AsyncConnection) -> MenuPublishResponse:
        async with conn.transaction():
            await conn.execute("LOCK TABLE menu_versions IN EXCLUSIVE MODE")

            version_cursor = await conn.execute(load_query("gateway/next_menu_version"))
            version = (await version_cursor.fetchone())[0]

            await conn.execute(
                load_query("gateway/insert_menu_version"),
                {
                    "version": version,
                    "note": request.note,
                },
            )

            published: list[PublishedProduct] = []

            for item in request.products:
                vendor_cursor = await conn.execute(
                    load_query("gateway/get_menu_vendor"),
                    {"vendor_id": item.vendor_id},
                )
                vendor_row = await vendor_cursor.fetchone()

                if vendor_row is None:
                    raise MenuNotFoundError()

                region_id = vendor_row[1]

                product_id = item.product_id or generated_product_id(
                    item.vendor_id,
                    item.name,
                )

                product_cursor = await conn.execute(
                    load_query("gateway/get_menu_product"),
                    {
                        "vendor_id": item.vendor_id,
                        "product_id": product_id,
                    },
                )
                existing = await product_cursor.fetchone()

                if existing is None:
                    if item.product_id is not None:
                        raise MenuNotFoundError()

                    if item.dish_id is None or item.multiplier is None:
                        raise MenuValidationError()

                    dish_cursor = await conn.execute(
                        load_query("gateway/dish_exists"),
                        {"dish_id": item.dish_id},
                    )

                    if await dish_cursor.fetchone() is None:
                        raise MenuNotFoundError()

                    await conn.execute(
                        load_query("gateway/insert_menu_product"),
                        {
                            "vendor_id": item.vendor_id,
                            "product_id": product_id,
                            "name": item.name,
                            "unit_price": Decimal(item.price_units) / Decimal(2500),
                            "price_units": item.price_units,
                            "menu_version": version,
                        },
                    )

                    await conn.execute(
                        load_query("gateway/insert_product_dish"),
                        {
                            "vendor_id": item.vendor_id,
                            "product_id": product_id,
                            "dish_id": item.dish_id,
                            "multiplier": item.multiplier,
                        },
                    )

                    created = True
                else:
                    existing_name = existing[0]
                    existing_dish_id = existing[2]
                    existing_multiplier = existing[3]

                    if item.product_id is None and existing_name != item.name:
                        raise ProductExistsError(
                            item.vendor_id,
                            product_id,
                        )

                    if item.dish_id is not None and item.dish_id != existing_dish_id:
                        raise DishLinkFixedError()

                    if item.multiplier is not None and item.multiplier != existing_multiplier:
                        raise DishLinkFixedError()

                    await conn.execute(
                        load_query("gateway/update_menu_product"),
                        {
                            "vendor_id": item.vendor_id,
                            "product_id": product_id,
                            "name": item.name,
                            "unit_price": Decimal(item.price_units) / Decimal(2500),
                            "price_units": item.price_units,
                            "menu_version": version,
                        },
                    )

                    created = False

                published.append(
                    PublishedProduct(
                        vendor_id=item.vendor_id,
                        product_id=product_id,
                        region_id=region_id,
                        created=created,
                    )
                )

            return MenuPublishResponse(
                menu_version=version,
                products=published,
            )

    return await run_on(pool, work)

from hashlib import sha1
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from restaurantos.gateway.errors import (
    DishLinkFixedError,
    MenuNotFoundError,
    ProductExistsError,
)
from restaurantos.gateway.menu_publish import (
    MenuProductInput,
    MenuPublishRequest,
    generated_product_id,
    publish_menu,
)


def test_generated_product_id_is_first_12_sha1_hex() -> None:
    expected = sha1(b"v001|Laksa").hexdigest()[:12]

    assert generated_product_id("v001", "Laksa") == expected
    assert len(expected) == 12


def test_generated_product_id_is_deterministic() -> None:
    first = generated_product_id("v001", "Laksa")
    second = generated_product_id("v001", "Laksa")

    assert first == second


def test_menu_publish_requires_products() -> None:
    with pytest.raises(ValidationError):
        MenuPublishRequest(products=[])


def test_menu_product_rejects_negative_price() -> None:
    with pytest.raises(ValidationError):
        MenuProductInput(
            vendor_id="v001",
            name="Laksa",
            price_units=-1,
            dish_id="d001",
            multiplier=1,
        )


def test_menu_product_rejects_invalid_multiplier() -> None:
    with pytest.raises(ValidationError):
        MenuProductInput(
            vendor_id="v001",
            name="Laksa",
            price_units=30,
            dish_id="d001",
            multiplier=0,
        )


async def test_publish_menu_creates_product(monkeypatch) -> None:
    connection = AsyncMock()

    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (2,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = None

    dish_cursor = AsyncMock()
    dish_cursor.fetchone.return_value = (1,)

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,
        product_cursor,
        dish_cursor,
        AsyncMock(),  # INSERT product
        AsyncMock(),  # INSERT product_dish
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        note="add laksa",
        products=[
            MenuProductInput(
                vendor_id="v001",
                name="Laksa",
                price_units=30,
                dish_id="d001",
                multiplier=1,
            )
        ],
    )

    result = await publish_menu(
        object(),
        request,
    )

    expected_id = generated_product_id("v001", "Laksa")

    assert result.menu_version == 2
    assert len(result.products) == 1

    published = result.products[0]

    assert published.vendor_id == "v001"
    assert published.product_id == expected_id
    assert published.region_id == "r1"
    assert published.created is True


async def test_publish_menu_updates_existing_product(monkeypatch) -> None:
    connection = AsyncMock()
    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (3,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = (
        "Old Laksa",
        25,
        "d001",
        1,
    )

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,
        product_cursor,
        AsyncMock(),  # UPDATE product
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        note="rename laksa",
        products=[
            MenuProductInput(
                vendor_id="v001",
                product_id="p001",
                name="New Laksa",
                price_units=30,
                dish_id="d001",
                multiplier=1,
            )
        ],
    )

    result = await publish_menu(object(), request)

    assert result.menu_version == 3
    assert result.products[0].product_id == "p001"
    assert result.products[0].region_id == "r1"
    assert result.products[0].created is False


async def test_publish_menu_update_may_omit_dish_link(monkeypatch) -> None:
    connection = AsyncMock()
    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (3,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = (
        "Laksa",
        25,
        "d001",
        2,
    )

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,
        product_cursor,
        AsyncMock(),  # UPDATE product
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        products=[
            MenuProductInput(
                vendor_id="v001",
                product_id="p001",
                name="Laksa Set",
                price_units=35,
            )
        ],
    )

    result = await publish_menu(object(), request)

    assert result.products[0].created is False


async def test_publish_menu_rejects_changed_dish_link(monkeypatch) -> None:
    connection = AsyncMock()
    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (3,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = (
        "Laksa",
        30,
        "d001",
        1,
    )

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,
        product_cursor,
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        products=[
            MenuProductInput(
                vendor_id="v001",
                product_id="p001",
                name="Laksa",
                price_units=30,
                dish_id="d999",
                multiplier=1,
            )
        ],
    )

    with pytest.raises(DishLinkFixedError):
        await publish_menu(object(), request)


async def test_publish_menu_generated_id_is_idempotent(monkeypatch) -> None:
    connection = AsyncMock()
    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (3,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = (
        "Laksa",
        30,
        "d001",
        1,
    )

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,
        product_cursor,
        AsyncMock(),  # UPDATE product
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        products=[
            MenuProductInput(
                vendor_id="v001",
                name="Laksa",
                price_units=35,
                dish_id="d001",
                multiplier=1,
            )
        ],
    )

    result = await publish_menu(object(), request)

    assert result.products[0].product_id == generated_product_id(
        "v001",
        "Laksa",
    )
    assert result.products[0].created is False


async def test_publish_menu_rejects_generated_id_name_collision(
    monkeypatch,
) -> None:
    connection = AsyncMock()
    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (3,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = (
        "Different Product",
        20,
        "d001",
        1,
    )

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,
        product_cursor,
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        products=[
            MenuProductInput(
                vendor_id="v001",
                name="Laksa",
                price_units=30,
                dish_id="d001",
                multiplier=1,
            )
        ],
    )

    with pytest.raises(ProductExistsError) as exc_info:
        await publish_menu(object(), request)

    assert exc_info.value.vendor_id == "v001"
    assert exc_info.value.product_id == generated_product_id(
        "v001",
        "Laksa",
    )


async def test_publish_menu_rejects_unknown_explicit_product(
    monkeypatch,
) -> None:
    connection = AsyncMock()
    transaction = AsyncMock()
    connection.transaction = Mock(return_value=transaction)

    version_cursor = AsyncMock()
    version_cursor.fetchone.return_value = (3,)

    vendor_cursor = AsyncMock()
    vendor_cursor.fetchone.return_value = ("v001", "r1")

    product_cursor = AsyncMock()
    product_cursor.fetchone.return_value = None

    connection.execute.side_effect = [
        AsyncMock(),  # LOCK
        version_cursor,  # SELECT next version
        AsyncMock(),  # INSERT menu_versions
        vendor_cursor,  # vendor lookup
        product_cursor,  # explicit product lookup
    ]

    async def fake_run_on(pool, work, **kwargs):
        return await work(connection)

    monkeypatch.setattr(
        "restaurantos.gateway.menu_publish.run_on",
        fake_run_on,
    )

    request = MenuPublishRequest(
        products=[
            MenuProductInput(
                vendor_id="v001",
                product_id="missing",
                name="Laksa",
                price_units=30,
            )
        ],
    )

    with pytest.raises(MenuNotFoundError):
        await publish_menu(object(), request)

    assert connection.execute.await_count == 5

    product_call = connection.execute.await_args_list[4]
    assert product_call.args[1] == {
        "vendor_id": "v001",
        "product_id": "missing",
    }

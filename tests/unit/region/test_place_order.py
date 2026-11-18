"""The step-3 refusal rules of place_order. No database."""

from datetime import time

from restaurantos.common.models import OrderItemIn, OrderRequest, RefusalReason
from restaurantos.region.orders import _check_products


def request(*product_ids: str) -> OrderRequest:
    return OrderRequest(
        channel="test",
        external_order_id="o1",
        vendor_id="v1",
        order_day=14,
        order_time=time(12, 0),
        items=[OrderItemIn(product_id=p, quantity=1) for p in product_ids],
    )


def product(*, dish_id: str | None = "d1", available: bool = True) -> dict:
    return {"dish_id": dish_id, "available": available}


def test_known_products_that_are_on_pass():
    assert _check_products(request("a", "b"), {"a": product(), "b": product()}) is None


def test_a_product_the_vendor_does_not_have_is_unknown():
    assert _check_products(request("a", "x"), {"a": product()}) is RefusalReason.UNKNOWN_PRODUCT


def test_a_product_without_a_dish_link_is_unknown():
    products = {"a": product(dish_id=None)}
    assert _check_products(request("a"), products) is RefusalReason.UNKNOWN_PRODUCT


def test_a_switched_off_product_is_unavailable():
    products = {"a": product(), "b": product(available=False)}
    assert _check_products(request("a", "b"), products) is RefusalReason.UNAVAILABLE


def test_unknown_is_reported_before_unavailable():
    # The switched-off product comes first in the order, the unknown one second.
    products = {"a": product(available=False)}
    assert _check_products(request("a", "x"), products) is RefusalReason.UNKNOWN_PRODUCT

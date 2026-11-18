"""The region write path (WP1).

The only code that writes a region's operational tables. Every function runs
one local transaction on a connection its caller opened, and checks the
caller's epoch first.
"""

from restaurantos.region.availability import ProductState, set_availability
from restaurantos.region.menu import DishLinkChangedError, apply_menu_version
from restaurantos.region.orders import place_order
from restaurantos.region.status import change_status
from restaurantos.region.stock import RestockResult, restock

__all__ = [
    "DishLinkChangedError",
    "ProductState",
    "RestockResult",
    "apply_menu_version",
    "change_status",
    "place_order",
    "restock",
    "set_availability",
]

"""The region write path (WP1).

The only code that writes a region's operational tables. Every function runs
one local transaction on a connection its caller opened, and checks the
caller's epoch first.
"""

from restaurantos.region.orders import place_order

__all__ = ["place_order"]

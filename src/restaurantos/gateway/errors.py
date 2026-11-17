"""Gateway-specific errors."""


class HQUnreachableError(Exception):
    """The HQ database could not be reached in time."""


class ProductExistsError(Exception):
    """A generated product id already belongs to another product name."""

    def __init__(self, vendor_id: str, product_id: str) -> None:
        self.vendor_id = vendor_id
        self.product_id = product_id


class DishLinkFixedError(Exception):
    """An existing product's dish link cannot be changed."""


class MenuNotFoundError(Exception):
    """A vendor, product, or dish referenced by a menu publish is unknown."""


class MenuValidationError(Exception):
    """A menu publish is invalid for the requested operation."""

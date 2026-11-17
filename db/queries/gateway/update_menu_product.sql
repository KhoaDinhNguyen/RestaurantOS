UPDATE products
SET
    name = %(name)s,
    unit_price = %(unit_price)s,
    price_units = %(price_units)s,
    menu_version = %(menu_version)s
WHERE
    vendor_id = %(vendor_id)s  -- noqa: RF02
    AND product_id = %(product_id)s  -- noqa: RF02
    AND origin = 'hq';

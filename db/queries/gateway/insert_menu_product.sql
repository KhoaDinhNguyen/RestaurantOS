INSERT INTO products (
    vendor_id,
    product_id,
    name,
    unit_price,
    price_units,
    menu_version,
    origin
)
VALUES (
    %(vendor_id)s,
    %(product_id)s,
    %(name)s,
    %(unit_price)s,
    %(price_units)s,
    %(menu_version)s,
    'hq'
);

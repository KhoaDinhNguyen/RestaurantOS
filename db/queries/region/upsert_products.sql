-- apply_menu_version step 4. Upsert, not delete and insert, so the
-- product_availability rows of existing products survive. A row with
-- origin = 'region' is never touched (the WHERE on the update).
INSERT INTO products (
    vendor_id, product_id, name, unit_price, price_units, menu_version, origin
)
SELECT
    r.vendor_id,
    r.product_id,
    r.name,
    r.unit_price,
    r.price_units,
    r.menu_version,
    'hq' AS origin
FROM
    unnest(
        %(vendor_ids)s::text[],
        %(product_ids)s::text[],
        %(names)s::text[],
        %(unit_prices)s::numeric[],
        %(price_units)s::integer[],
        %(menu_versions)s::integer[]
    ) AS r (vendor_id, product_id, name, unit_price, price_units, menu_version)
ON CONFLICT (vendor_id, product_id) DO UPDATE
    SET
        name = excluded.name,
        unit_price = excluded.unit_price,
        price_units = excluded.price_units,
        menu_version = excluded.menu_version
    WHERE products.origin = 'hq';

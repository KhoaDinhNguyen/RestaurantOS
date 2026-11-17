WITH region_cells AS (
    SELECT geohash
    FROM shard_directory
    WHERE region_id = %(region_id)s
)

SELECT
    p.vendor_id,
    p.product_id,
    p.name,
    p.unit_price,
    p.price_units,
    p.menu_version,
    p.origin,
    pd.dish_id,
    pd.multiplier,
    pd.multiplier_ambiguous,
    max(p.menu_version) OVER () AS publish_version
FROM products AS p
INNER JOIN product_dish AS pd
    ON p.vendor_id = pd.vendor_id AND p.product_id = pd.product_id
INNER JOIN vendors AS v ON p.vendor_id = v.vendor_id
INNER JOIN region_cells AS rc ON v.geohash = rc.geohash
WHERE p.origin = 'hq'
ORDER BY p.vendor_id, p.product_id;

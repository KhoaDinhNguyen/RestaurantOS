SELECT
    p.vendor_id,
    p.product_id,
    p.name,
    p.price_units,
    p.menu_version,
    pd.dish_id,
    pa.reason,
    COALESCE(pa.available, true) AS available
FROM products AS p
INNER JOIN product_dish AS pd
    ON
        p.vendor_id = pd.vendor_id
        AND p.product_id = pd.product_id
LEFT JOIN product_availability AS pa
    ON
        p.vendor_id = pa.vendor_id
        AND p.product_id = pa.product_id
WHERE p.vendor_id = %(vendor_id)s  -- noqa: RF02
ORDER BY p.product_id;

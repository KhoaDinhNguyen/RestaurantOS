SELECT
    p.name,
    p.price_units,
    pd.dish_id,
    pd.multiplier
FROM products AS p
INNER JOIN product_dish AS pd
    ON
        p.vendor_id = pd.vendor_id
        AND p.product_id = pd.product_id
WHERE
    p.vendor_id = %(vendor_id)s  -- noqa: RF02
    AND p.product_id = %(product_id)s;  -- noqa: RF02

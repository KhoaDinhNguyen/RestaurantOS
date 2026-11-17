-- Every product of the order, for this vendor only; every join is on
-- a primary key. dish_id is NULL when the product has no product_dish row
-- (treated as unknown). available is false when a product_availability row
-- switches the product off.
SELECT
    p.product_id,
    p.price_units,
    pd.dish_id,
    pd.multiplier,
    d.stock_mode,
    COALESCE(pa.available, true) AS available
FROM products AS p
LEFT JOIN product_dish AS pd
    ON p.vendor_id = pd.vendor_id AND p.product_id = pd.product_id
LEFT JOIN dishes AS d ON pd.dish_id = d.dish_id
LEFT JOIN product_availability AS pa
    ON p.vendor_id = pa.vendor_id AND p.product_id = pa.product_id
WHERE
    p.vendor_id = %(vendor_id)s  -- noqa: RF02
    AND p.product_id = ANY(%(product_ids)s::text[]);  -- noqa: RF02

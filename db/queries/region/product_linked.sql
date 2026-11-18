-- set_availability step 2. Does this vendor sell this product? A product
-- with no dish link counts as unknown, as in place_order step 3.
-- Both joins are on primary keys.
SELECT 1
FROM products AS p
INNER JOIN product_dish AS pd
    ON p.vendor_id = pd.vendor_id AND p.product_id = pd.product_id
WHERE
    p.vendor_id = %(vendor_id)s  -- noqa: RF02
    AND p.product_id = %(product_id)s;  -- noqa: RF02

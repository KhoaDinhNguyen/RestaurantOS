--  Switches off this vendor's products that use a counter that just
-- reached zero. Scans only this vendor's products, never the region's menu.
INSERT INTO product_availability (vendor_id, product_id, available, reason)
SELECT
    pd.vendor_id,
    pd.product_id,
    false AS available,
    'stock' AS reason
FROM product_dish AS pd
INNER JOIN dishes AS d ON pd.dish_id = d.dish_id
WHERE
    pd.vendor_id = %(vendor_id)s  -- noqa: RF02
    AND (
        (d.stock_mode = 'per_item' AND pd.dish_id = ANY(%(empty_dishes)s::text[]))  -- noqa: RF02
        OR (
            d.stock_mode = 'per_ingredient'
            AND EXISTS (
                SELECT 1 FROM dish_ingredients AS di
                WHERE
                    di.dish_id = pd.dish_id
                    AND di.ingredient_id = ANY(%(empty_ingredients)s::text[])  -- noqa: RF02
            )
        )
    )
ON CONFLICT (vendor_id, product_id) DO NOTHING;

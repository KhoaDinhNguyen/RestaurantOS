-- place_order step 7. Switches off this vendor's products that can no longer
-- make one unit because a counter this order just took from is now below what
-- one unit needs (amount x multiplier per recipe row, or multiplier for
-- per_item). The same rule as can_make_one, but only for the counters this
-- order touched. This transaction already holds them FOR UPDATE, so it takes
-- no new locks. Rows go in (vendor_id, product_id) order so two orders marking
-- the same products cannot deadlock on the insert. A manual mark is never
-- overwritten.
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
        (
            d.stock_mode = 'per_item'
            AND pd.dish_id = ANY(%(touched_dishes)s::text[])  -- noqa: RF02
            AND EXISTS (
                SELECT 1
                FROM stock AS s
                WHERE
                    s.vendor_id = pd.vendor_id
                    AND s.item_kind = 'dish'
                    AND s.item_id = pd.dish_id
                    AND s.on_hand < pd.multiplier
            )
        )
        OR (
            d.stock_mode = 'per_ingredient'
            AND EXISTS (
                SELECT 1
                FROM dish_ingredients AS di
                INNER JOIN stock AS s
                    ON
                        s.vendor_id = pd.vendor_id
                        AND s.item_kind = 'ingredient'
                        AND di.ingredient_id = s.item_id
                WHERE
                    di.dish_id = pd.dish_id
                    AND di.ingredient_id = ANY(%(touched_ingredients)s::text[])  -- noqa: RF02
                    AND s.on_hand < di.amount * pd.multiplier
            )
        )
    )
ORDER BY pd.vendor_id, pd.product_id
ON CONFLICT (vendor_id, product_id) DO NOTHING;

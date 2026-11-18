-- apply_menu_version step 4. A dish link never changes (architecture Section
-- 6.1), and the caller has already refused a slice that tries: on a conflict
-- only multiplier_ambiguous is refreshed, never dish_id or multiplier.
INSERT INTO product_dish (vendor_id, product_id, dish_id, multiplier, multiplier_ambiguous)
SELECT
    r.vendor_id,
    r.product_id,
    r.dish_id,
    r.multiplier,
    r.multiplier_ambiguous
FROM
    unnest(
        %(vendor_ids)s::text[],
        %(product_ids)s::text[],
        %(dish_ids)s::text[],
        %(multipliers)s::integer[],
        %(ambiguous)s::boolean[]
    ) AS r (vendor_id, product_id, dish_id, multiplier, multiplier_ambiguous)
ON CONFLICT (vendor_id, product_id) DO UPDATE
    SET multiplier_ambiguous = excluded.multiplier_ambiguous;

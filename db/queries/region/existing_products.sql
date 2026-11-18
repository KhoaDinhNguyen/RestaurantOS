-- apply_menu_version step 3. Which products of the slice this node already
-- has, with their origin and dish link. A product missing here is one the
-- version adds. Every join is on a primary key.
SELECT
    p.vendor_id,
    p.product_id,
    p.origin,
    pd.dish_id,
    pd.multiplier
FROM
    unnest(
        %(vendor_ids)s::text[],  -- noqa: RF02
        %(product_ids)s::text[]  -- noqa: RF02
    ) AS r (vendor_id, product_id)
INNER JOIN products AS p
    ON r.vendor_id = p.vendor_id AND r.product_id = p.product_id
LEFT JOIN product_dish AS pd
    ON p.vendor_id = pd.vendor_id AND p.product_id = pd.product_id;

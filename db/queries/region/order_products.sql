-- Every product of the order, for this vendor only; every join is on
-- a primary key. dish_id is NULL when the product has no product_dish row
-- (treated as unknown). available is false when a product_availability row
-- switches the product off. menu_version is the node's version, read in this
-- same statement so that it always matches the prices: apply_menu_version
-- commits the products and the version together, and one statement sees one
-- snapshot. No lock on node_state, so orders never hold up a menu apply or an
-- epoch bump.
SELECT
    p.product_id,
    p.price_units,
    pd.dish_id,
    pd.multiplier,
    d.stock_mode,
    COALESCE(pa.available, true) AS available,
    (SELECT ns.menu_version FROM node_state AS ns) AS menu_version
FROM products AS p
LEFT JOIN product_dish AS pd
    ON p.vendor_id = pd.vendor_id AND p.product_id = pd.product_id
LEFT JOIN dishes AS d ON pd.dish_id = d.dish_id
LEFT JOIN product_availability AS pa
    ON p.vendor_id = pa.vendor_id AND p.product_id = pa.product_id
WHERE
    p.vendor_id = %(vendor_id)s  -- noqa: RF02
    AND p.product_id = ANY(%(product_ids)s::text[]);  -- noqa: RF02

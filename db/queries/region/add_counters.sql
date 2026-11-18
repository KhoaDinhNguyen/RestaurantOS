-- apply_menu_version step 5, for the products the version adds. What one
-- unit of each takes from each counter: amount x multiplier per recipe row
-- (per_ingredient), multiplier (per_item), nothing (none). A missing counter
-- is created with on_hand = 0; every needed counter's par level is raised to
-- at least max(F, the largest need) and on_hand is never touched.
-- Rows are taken in (vendor_id, item_kind, item_id) order, the order
-- place_order and restock lock counters in, so none of them can deadlock.
WITH added AS (
    SELECT
        a.vendor_id,
        a.product_id
    FROM unnest(%(vendor_ids)s::text[], %(product_ids)s::text[]) AS a (vendor_id, product_id)
),

needs AS (
    SELECT
        pd.vendor_id,
        'ingredient' AS item_kind,
        di.ingredient_id AS item_id,
        di.amount * pd.multiplier AS need
    FROM added AS a
    INNER JOIN product_dish AS pd
        ON a.vendor_id = pd.vendor_id AND a.product_id = pd.product_id
    INNER JOIN dishes AS d ON pd.dish_id = d.dish_id
    INNER JOIN dish_ingredients AS di ON pd.dish_id = di.dish_id
    WHERE d.stock_mode = 'per_ingredient'
    UNION ALL
    SELECT
        pd.vendor_id,
        'dish' AS item_kind,
        pd.dish_id AS item_id,
        pd.multiplier AS need
    FROM added AS a
    INNER JOIN product_dish AS pd
        ON a.vendor_id = pd.vendor_id AND a.product_id = pd.product_id
    INNER JOIN dishes AS d ON pd.dish_id = d.dish_id
    WHERE d.stock_mode = 'per_item'
)

INSERT INTO stock (vendor_id, item_kind, item_id, on_hand, par_level)
SELECT
    vendor_id,
    item_kind,
    item_id,
    0 AS on_hand,
    greatest(%(floor)s, max(need)) AS par_level
FROM needs
GROUP BY vendor_id, item_kind, item_id
ORDER BY vendor_id, item_kind, item_id
ON CONFLICT (vendor_id, item_kind, item_id) DO UPDATE
    SET par_level = excluded.par_level
    WHERE stock.par_level < excluded.par_level;

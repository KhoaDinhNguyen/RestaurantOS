WITH ingredient_units AS (
    SELECT
        ingredient_id,
        MIN(unit) AS unit
    FROM dish_ingredients
    GROUP BY ingredient_id
)

SELECT
    s.item_kind,
    s.item_id,
    s.on_hand,
    s.par_level,
    s.updated_at,
    CASE
        WHEN s.item_kind = 'ingredient' THEN s.item_id
        ELSE COALESCE(d.dish_name, d.example_name)
    END AS name,
    CASE
        WHEN s.item_kind = 'ingredient' THEN iu.unit
    END AS unit
FROM stock AS s
LEFT JOIN dishes AS d
    ON
        s.item_kind = 'dish'
        AND s.item_id = d.dish_id
LEFT JOIN ingredient_units AS iu
    ON
        s.item_kind = 'ingredient'
        AND s.item_id = iu.ingredient_id
WHERE s.vendor_id = %(vendor_id)s  -- noqa: RF02
ORDER BY s.item_kind ASC, s.item_id ASC;

WITH shipped_recipes AS (
    SELECT *
    FROM unnest(
        %(dish_ids)s::text[],
        %(ingredient_ids)s::text[],
        %(amounts)s::integer[],
        %(units)s::text[]
    ) AS recipe (dish_id, ingredient_id, amount, unit)
)

SELECT
    recipe.ingredient_id,
    recipe.unit,
    sum(
        oi.quantity
        * pd.multiplier
        * recipe.amount
    ) AS amount
FROM orders AS o
INNER JOIN order_items AS oi
    ON
        o.order_day = oi.order_day
        AND o.order_id = oi.order_id
INNER JOIN product_dish AS pd
    ON
        oi.vendor_id = pd.vendor_id
        AND oi.product_id = pd.product_id
INNER JOIN shipped_recipes AS recipe
    ON pd.dish_id = recipe.dish_id
WHERE
    (
        (
            %(crosses_midnight)s = false  -- noqa: RF02
            AND o.order_day BETWEEN %(from_day)s AND %(to_day)s  -- noqa: RF02
            AND (
                %(has_time_window)s = false  -- noqa: RF02
                OR (
                    o.order_time >= %(from_time)s  -- noqa: RF02
                    AND o.order_time < %(to_time)s  -- noqa: RF02
                )
            )
        )
        OR (
            %(crosses_midnight)s = true  -- noqa: RF02
            AND (
                (
                    o.order_day BETWEEN %(from_day)s AND %(to_day)s  -- noqa: RF02
                    AND o.order_time >= %(from_time)s  -- noqa: RF02
                )
                OR (
                    o.order_day BETWEEN %(from_day)s + 1 AND %(to_day)s + 1  -- noqa: RF02
                    AND o.order_time < %(to_time)s  -- noqa: RF02
                )
            )
        )
    )
GROUP BY
    recipe.ingredient_id,
    recipe.unit
ORDER BY
    amount DESC,
    recipe.ingredient_id ASC,
    recipe.unit ASC;

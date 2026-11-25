SELECT
    di.ingredient_id,
    di.unit,
    sum(oi.quantity * pd.multiplier * di.amount) AS amount
FROM analytics_orders AS o
INNER JOIN analytics_order_items AS oi
    ON
        o.order_day = oi.order_day
        AND o.order_id = oi.order_id
INNER JOIN product_dish AS pd
    ON
        oi.vendor_id = pd.vendor_id
        AND oi.product_id = pd.product_id
INNER JOIN dish_ingredients AS di
    ON pd.dish_id = di.dish_id
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
    di.ingredient_id,
    di.unit
ORDER BY
    amount DESC,
    di.ingredient_id COLLATE "C" ASC,  -- noqa: RF02
    di.unit COLLATE "C" ASC;  -- noqa: RF02

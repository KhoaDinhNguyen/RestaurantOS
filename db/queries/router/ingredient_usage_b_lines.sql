SELECT
    oi.vendor_id,
    oi.product_id,
    oi.quantity
FROM orders AS o
INNER JOIN order_items AS oi
    ON
        o.order_day = oi.order_day
        AND o.order_id = oi.order_id
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
ORDER BY
    oi.vendor_id ASC,
    oi.product_id ASC,
    oi.quantity ASC;

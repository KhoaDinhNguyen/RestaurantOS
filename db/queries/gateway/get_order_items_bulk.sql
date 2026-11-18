SELECT
    oi.order_day,
    oi.order_id,
    oi.line_no,
    oi.product_id,
    p.name,
    oi.quantity,
    oi.price_units
FROM order_items AS oi
INNER JOIN products AS p
    ON
        oi.vendor_id = p.vendor_id
        AND oi.product_id = p.product_id
WHERE
    oi.vendor_id = %(vendor_id)s  -- noqa: RF02
    AND oi.order_day = %(order_day)s  -- noqa: RF02
    AND oi.order_id = ANY(%(order_ids)s)  -- noqa: RF02
ORDER BY
    oi.order_id,
    oi.line_no;

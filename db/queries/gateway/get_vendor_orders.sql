SELECT
    order_id,
    order_day,
    order_time,
    vendor_id,
    customer_id,
    channel,
    status,
    status_changed_at,
    menu_version,
    received_at,
    committed_at
FROM orders
WHERE
    vendor_id = %(vendor_id)s  -- noqa: RF02
    AND order_day = %(day)s
    AND (
        %(statuses)s::text[] IS NULL
        OR status = ANY(%(statuses)s::text[])
    )
ORDER BY order_time, order_id;

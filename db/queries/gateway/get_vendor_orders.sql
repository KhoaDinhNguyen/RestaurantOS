SELECT
    order_id,
    order_day,
    vendor_id,
    status,
    order_time,
    status_changed_at,
    menu_version,
    received_at,
    committed_at
FROM orders
WHERE
    vendor_id = %(vendor_id)s  -- noqa: RF02
    AND order_day = %(day)s
    AND (
        %(status)s::text IS NULL
        OR status = %(status)s::text
    )
ORDER BY order_time, order_id;

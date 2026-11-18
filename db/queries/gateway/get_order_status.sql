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
    AND order_id = %(order_id)s
    AND (
        %(day)s::smallint IS NULL
        OR order_day = %(day)s::smallint
    );

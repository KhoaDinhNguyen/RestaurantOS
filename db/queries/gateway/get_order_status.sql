SELECT
    order_id,
    order_day,
    vendor_id,
    status,
    status_changed_at,
    menu_version,
    received_at,
    committed_at
FROM orders
WHERE
    vendor_id = %(vendor_id)s  -- noqa: RF02
    AND order_id = %(order_id)s;

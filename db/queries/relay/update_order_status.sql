UPDATE analytics_orders
SET
    status = %(to_status)s,
    status_changed_at = %(changed_at)s
WHERE
    order_day = %(order_day)s
    AND order_id = %(order_id)s
    AND region_id = %(region_id)s;

--  the stored order. Both keys, so one partition is read.
SELECT
    menu_version,
    committed_at
FROM orders
WHERE order_day = %(order_day)s AND order_id = %(order_id)s;

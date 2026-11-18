-- change_status step 4. status_changed_at is wall-clock time, for measurement.
UPDATE orders
SET
    status = %(to_status)s,
    status_changed_at = clock_timestamp()
WHERE
    order_day = %(order_day)s
    AND order_id = %(order_id)s
RETURNING status_changed_at;

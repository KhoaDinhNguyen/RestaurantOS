-- change_status step 5. Both versions and their times: the stored status,
-- which stays, and the attempted one, which lost. kind, attempted_at
-- (clock_timestamp) and detail come from the column defaults.
INSERT INTO exception_log (
    event_id,
    order_day,
    order_id,
    current_status,
    current_changed_at,
    attempted_status
)
VALUES (
    %(event_id)s,
    %(order_day)s,
    %(order_id)s,
    %(current_status)s,
    %(current_changed_at)s,
    %(attempted_status)s
)
RETURNING kind, attempted_at, detail;

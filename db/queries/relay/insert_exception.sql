INSERT INTO analytics_exceptions (
    event_id, region_id, order_day, order_id, kind, current_status, current_changed_at,
    attempted_status, attempted_at, detail
)
VALUES (
    %(event_id)s, %(region_id)s, %(order_day)s, %(order_id)s, %(kind)s, %(current_status)s,
    %(current_changed_at)s, %(attempted_status)s, %(attempted_at)s, %(detail)s
);

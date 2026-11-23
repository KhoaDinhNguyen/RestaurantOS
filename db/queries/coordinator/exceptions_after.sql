SELECT
    seq,
    region_id,
    order_day,
    order_id,
    kind,
    current_status,
    current_changed_at,
    attempted_status,
    attempted_at,
    hq_applied_at
FROM analytics_exceptions
WHERE seq > %(since)s
ORDER BY seq
LIMIT %(page_size)s;

SELECT *
FROM (
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
    ORDER BY seq DESC
    LIMIT %(page_size)s
) AS recent
ORDER BY seq;

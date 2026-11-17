UPDATE holding_queue
SET
    delivered_at = clock_timestamp(),
    delivered_outcome = %(outcome)s
WHERE
    holding_id = %(holding_id)s
    AND region_id = %(region_id)s
    AND delivered_at IS NULL;

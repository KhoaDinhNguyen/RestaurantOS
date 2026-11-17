SELECT
    holding_id,
    request,
    received_at
FROM holding_queue
WHERE
    region_id = %(region_id)s
    AND delivered_at IS NULL
ORDER BY holding_id
LIMIT %(batch_size)s;

SELECT
    event_id,
    seq,
    event_type,
    order_day,
    created_at,
    payload
FROM outbox
WHERE relayed_at IS NULL
ORDER BY seq
LIMIT %(batch_size)s;

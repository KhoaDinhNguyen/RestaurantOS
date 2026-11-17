UPDATE outbox
SET relayed_at = clock_timestamp()
WHERE
    event_id = any(%(event_ids)s)
    AND relayed_at IS NULL;

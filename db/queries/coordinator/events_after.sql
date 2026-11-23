SELECT
    cluster_event_id AS event_id,
    occurred_at,
    kind,
    region_id,
    node_id,
    detail
FROM cluster_events
WHERE cluster_event_id > %(since)s
ORDER BY cluster_event_id
LIMIT %(page_size)s;

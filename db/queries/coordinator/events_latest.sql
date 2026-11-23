SELECT *
FROM (
    SELECT
        cluster_event_id AS event_id,
        occurred_at,
        kind,
        region_id,
        node_id,
        detail
    FROM cluster_events
    ORDER BY cluster_event_id DESC
    LIMIT %(page_size)s
) AS recent
ORDER BY event_id;

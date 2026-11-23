SELECT request.detail
FROM cluster_events AS request
WHERE
    request.region_id IS NULL AND request.kind = 'sync_downgrade_requested'
    AND request.detail ->> 'scope' = 'hq'
    AND request.detail ->> 'primary_node' = %(primary_node)s
    AND request.detail ->> 'system_id' = %(system_id)s
    AND request.detail ->> 'timeline' = %(timeline)s
    AND NOT EXISTS (
        SELECT 1 FROM cluster_events AS completed
        WHERE completed.detail ->> 'request_id' = request.detail ->> 'coordinator_event_id'
    )
ORDER BY request.cluster_event_id
LIMIT 1;

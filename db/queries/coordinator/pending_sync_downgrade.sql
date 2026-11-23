SELECT request.detail
FROM cluster_events AS request
WHERE
    request.region_id = %(region_id)s AND request.kind = 'sync_downgrade_requested'
    AND request.detail ->> 'epoch' = %(epoch)s::text
    AND request.detail ->> 'primary_node' = %(primary_node)s
    AND NOT EXISTS (
        SELECT 1 FROM cluster_events AS completed
        WHERE
            completed.kind = 'sync_downgrade'
            AND completed.detail ->> 'request_id' = request.detail ->> 'coordinator_event_id'
    )
ORDER BY request.cluster_event_id
LIMIT 1;

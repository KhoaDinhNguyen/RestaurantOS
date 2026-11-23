SELECT cluster_event_id AS event_id
FROM cluster_events
WHERE detail ->> 'coordinator_event_id' = %(marker)s;

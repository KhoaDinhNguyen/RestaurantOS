INSERT INTO cluster_events (occurred_at, kind, region_id, node_id, detail)
SELECT
    %(occurred_at)s,
    %(kind)s,
    %(region_id)s,
    %(node_id)s,
    %(detail)s
WHERE NOT EXISTS (
    SELECT 1
    FROM cluster_events
    WHERE detail ->> 'coordinator_event_id' = %(event_key)s
);

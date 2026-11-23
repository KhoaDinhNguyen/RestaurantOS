SELECT
    region_id,
    epoch,
    primary_node,
    standby_node
FROM region_state
WHERE region_id = %(region_id)s
FOR UPDATE SKIP LOCKED;

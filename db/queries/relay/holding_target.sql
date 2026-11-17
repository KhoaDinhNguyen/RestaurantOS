SELECT
    primary_node,
    epoch
FROM region_state
WHERE region_id = %(region_id)s;

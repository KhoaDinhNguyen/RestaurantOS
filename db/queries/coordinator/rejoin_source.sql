SELECT
    region_id,
    epoch,
    pg_is_in_recovery() AS in_recovery
FROM node_state;

UPDATE region_state
SET
    epoch = %(epoch)s,
    primary_node = %(new_primary_node)s,
    standby_node = %(new_standby_node)s,
    updated_at = clock_timestamp()
WHERE region_id = %(region_id)s;

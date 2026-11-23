SELECT
    ns.epoch,
    pg_is_in_recovery() AS in_recovery,
    current_setting('synchronous_standby_names') AS standby_names,
    current_setting('synchronous_commit') AS commit_mode,
    (
        SELECT pgconf.source
        FROM pg_settings AS pgconf
        WHERE pgconf.name = 'synchronous_standby_names'
    ) AS setting_source,
    coalesce((
        SELECT bool_or(repl.state = 'streaming' AND repl.sync_state IN ('sync', 'quorum'))
        FROM pg_stat_replication AS repl
        INNER JOIN pg_replication_slots AS slot ON repl.pid = slot.active_pid
        WHERE
            slot.slot_name = %(slot)s AND slot.slot_type = 'physical'
    ), false) AS sync_connected
FROM node_state AS ns
WHERE ns.region_id = %(region_id)s;

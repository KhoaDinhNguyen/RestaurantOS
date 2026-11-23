SELECT
    pg_is_in_recovery() AS in_recovery,
    (pg_control_system()).system_identifier::text AS system_id,
    CASE
        WHEN NOT pg_is_in_recovery()
            THEN substring(pg_walfile_name(pg_current_wal_lsn()) FROM 1 FOR 8)
    END AS timeline,
    current_setting('synchronous_standby_names') AS standby_names,
    current_setting('synchronous_commit') AS commit_mode,
    coalesce((
        SELECT bool_or(repl.state = 'streaming' AND repl.sync_state IN ('sync', 'quorum'))
        FROM pg_stat_replication AS repl
        INNER JOIN pg_replication_slots AS slot ON repl.pid = slot.active_pid
        WHERE slot.slot_name = %(slot)s AND slot.slot_type = 'physical'
    ), false) AS sync_connected;

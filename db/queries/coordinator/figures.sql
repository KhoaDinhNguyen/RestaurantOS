SELECT
    clock_timestamp() AS figures_as_of,
    (
        SELECT count(*)
        FROM outbox
        WHERE relayed_at IS NULL
    ) AS outbox_backlog,
    coalesce((
        SELECT extract(EPOCH FROM clock_timestamp() - created_at)::double precision
        FROM outbox
        WHERE relayed_at IS NULL
        ORDER BY seq
        LIMIT 1
    ), 0.0) AS hq_lag_s,
    (
        SELECT coalesce(sum(n_tup_ins), 0)
        FROM pg_stat_user_tables
        WHERE
            schemaname = current_schema()
            AND (relname LIKE 'orders\_d%' OR relname = 'orders_default')
    ) AS total_orders,
    (
        SELECT coalesce(extract(EPOCH FROM replay_lag)::double precision, 0.0)
        FROM pg_stat_replication
        ORDER BY pid
        LIMIT 1
    ) AS standby_lag_s,
    CASE
        WHEN
            current_setting('synchronous_standby_names') <> ''
            AND current_setting('synchronous_commit') NOT IN ('off', 'local')
            THEN 'sync'
        ELSE 'async'
    END AS replication_mode;

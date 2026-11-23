SELECT
    pg_is_in_recovery() AS in_recovery,
    (pg_control_system()).system_identifier::text AS system_identifier,
    to_regclass('public.region_state') IS NOT NULL AS has_hq_schema,
    CASE
        WHEN NOT pg_is_in_recovery()
            THEN substring(pg_walfile_name(pg_current_wal_lsn()) FROM 1 FOR 8)
    END AS timeline;

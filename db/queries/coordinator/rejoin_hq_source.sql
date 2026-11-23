SELECT
    (pg_control_system()).system_identifier::text AS system_identifier,
    substring(pg_walfile_name(pg_current_wal_lsn()) FROM 1 FOR 8) AS timeline,
    pg_is_in_recovery() AS in_recovery;

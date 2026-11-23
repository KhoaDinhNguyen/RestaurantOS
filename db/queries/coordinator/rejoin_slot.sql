SELECT
    slot_type,
    active,
    wal_status
FROM pg_replication_slots
WHERE slot_name = :'slot';

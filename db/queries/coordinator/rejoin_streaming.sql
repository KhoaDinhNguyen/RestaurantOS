SELECT active
FROM pg_replication_slots
WHERE slot_name = %(slot)s AND slot_type = 'physical';

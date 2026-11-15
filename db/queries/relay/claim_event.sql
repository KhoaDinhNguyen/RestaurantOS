INSERT INTO hq_inbox (region_id, event_id, seq)
VALUES (%(region_id)s, %(event_id)s, %(seq)s)
ON CONFLICT (region_id, event_id) DO NOTHING;

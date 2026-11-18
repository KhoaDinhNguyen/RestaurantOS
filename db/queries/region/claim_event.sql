-- change_status step 2. Records the status event.
-- A row back: first time seen. No row back: this event was processed before.
INSERT INTO processed_events (event_id, order_id)
VALUES (%(event_id)s, %(order_id)s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;

-- insert_outbox.sql
-- The event for HQ, in the same transaction as the change it describes.
INSERT INTO outbox (event_type, order_day, payload)
VALUES (%(event_type)s, %(order_day)s, %(payload)s);

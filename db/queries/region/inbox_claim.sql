-- A row back: the order is new. No row back: this order was seen before.
-- The row is complete (accepted + order_id) because the table's check
-- constraints run before the conflict check.
INSERT INTO inbox (channel, external_order_id, outcome, order_day, order_id)
VALUES (%(channel)s, %(external_order_id)s, 'accepted', %(order_day)s, %(order_id)s)
ON CONFLICT (channel, external_order_id) DO NOTHING
RETURNING order_id;

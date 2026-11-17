-- Refusing: outcome, order_id and refusal_reason change together, because
-- the table checks them as a set. order_day is kept.
UPDATE inbox
SET outcome = 'refused', order_id = NULL, refusal_reason = %(reason)s
WHERE channel = %(channel)s AND external_order_id = %(external_order_id)s;

-- inbox_get.sql
SELECT
    outcome,
    refusal_reason,
    order_day,
    order_id
FROM inbox
WHERE channel = %(channel)s AND external_order_id = %(external_order_id)s;

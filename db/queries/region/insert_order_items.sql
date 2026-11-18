-- insert_order_items.sql
-- All lines in one statement. price_units is copied from products,
-- so a later price change does not rewrite this order.
INSERT INTO order_items (order_day, order_id, line_no, vendor_id, product_id, quantity, price_units)
SELECT
    %(order_day)s::smallint,
    %(order_id)s::uuid,
    i.line_no,
    %(vendor_id)s::text,
    i.product_id,
    i.quantity,
    i.price_units
FROM unnest(
    %(line_nos)s::smallint[], %(product_ids)s::text[],
    %(quantities)s::smallint[], %(prices)s::integer[]
) AS i (line_no, product_id, quantity, price_units);

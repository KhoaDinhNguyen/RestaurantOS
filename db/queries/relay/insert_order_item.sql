INSERT INTO analytics_order_items (
    order_day, order_id, line_no, vendor_id, product_id, quantity, price_units
)
VALUES (
    %(order_day)s, %(order_id)s, %(line_no)s, %(vendor_id)s, %(product_id)s,
    %(quantity)s, %(price_units)s
);

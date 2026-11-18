-- set_availability step 3, switching on when one unit cannot be made.
-- Never overwrites a row that is already there.
INSERT INTO product_availability (vendor_id, product_id, available, reason)
VALUES (%(vendor_id)s, %(product_id)s, false, 'stock')
ON CONFLICT (vendor_id, product_id) DO NOTHING;

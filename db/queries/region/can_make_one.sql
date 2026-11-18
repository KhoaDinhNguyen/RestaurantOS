-- set_availability step 3, switching on. Locks the product's counters FOR SHARE.
SELECT can_make_one(%(vendor_id)s, %(product_id)s) AS ok;

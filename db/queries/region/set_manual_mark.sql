-- set_availability step 3, switching off. Upsert: a manual mark replaces a
-- stock mark if there is one, so a restock no longer switches the product on.
INSERT INTO product_availability (vendor_id, product_id, available, reason)
VALUES (%(vendor_id)s, %(product_id)s, false, 'manual')
ON CONFLICT (vendor_id, product_id) DO UPDATE
    SET
        available = excluded.available,
        reason = excluded.reason,
        updated_at = clock_timestamp();

-- restock step 3. on_hand = max(on_hand, par_level): only counters below par
-- change, so the row count is the number of counters that went up.
-- updated_at is wall-clock time, for measurement.
UPDATE stock
SET
    on_hand = par_level,
    updated_at = clock_timestamp()
WHERE
    vendor_id = %(vendor_id)s
    AND on_hand < par_level;

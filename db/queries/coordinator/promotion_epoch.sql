UPDATE node_state
SET epoch = %(epoch)s
WHERE region_id = %(region_id)s AND epoch IN (%(old_epoch)s, %(epoch)s)
RETURNING epoch;

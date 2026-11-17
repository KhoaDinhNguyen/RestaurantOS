WITH region_cells AS (
    SELECT geohash
    FROM shard_directory
    WHERE region_id = %(region_id)s
)

SELECT max(p.menu_version)
FROM products AS p
INNER JOIN vendors AS v ON p.vendor_id = v.vendor_id
INNER JOIN region_cells AS rc ON v.geohash = rc.geohash
WHERE p.origin = 'hq';

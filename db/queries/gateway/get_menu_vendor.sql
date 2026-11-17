SELECT
    v.vendor_id,
    sd.region_id
FROM vendors AS v
INNER JOIN shard_directory AS sd
    ON v.geohash = sd.geohash
WHERE v.vendor_id = %(vendor_id)s;  -- noqa: RF02

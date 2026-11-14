SELECT
    rs.region_id,
    rs.epoch,
    rs.primary_node,
    rs.standby_node,
    rs.menu_version,
    rs.updated_at,
    COALESCE(
        ARRAY_AGG(sd.geohash ORDER BY sd.geohash)
        FILTER (WHERE sd.geohash IS NOT NULL),
        ARRAY[]::text[]
    ) AS geohashes
FROM region_state AS rs
LEFT JOIN shard_directory AS sd
    ON rs.region_id = sd.region_id
GROUP BY
    rs.region_id,
    rs.epoch,
    rs.primary_node,
    rs.standby_node,
    rs.menu_version,
    rs.updated_at
ORDER BY rs.region_id;

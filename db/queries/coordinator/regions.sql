SELECT
    rs.region_id,
    rs.epoch,
    rs.menu_version,
    rs.primary_node,
    rs.standby_node,
    (
        SELECT count(*)
        FROM holding_queue AS hq
        WHERE hq.region_id = rs.region_id AND hq.delivered_at IS NULL
    ) AS holding_queue
FROM region_state AS rs
ORDER BY rs.region_id;

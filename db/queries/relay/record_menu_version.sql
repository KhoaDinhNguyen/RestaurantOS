UPDATE region_state
SET
    menu_version = greatest(menu_version, %(version)s),
    updated_at = clock_timestamp()
WHERE
    region_id = %(region_id)s
    AND primary_node = %(node)s
    AND epoch = %(epoch)s
    AND menu_version < %(version)s;

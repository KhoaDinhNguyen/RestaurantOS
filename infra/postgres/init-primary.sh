#!/usr/bin/env bash
# Runs once, on the first start of a primary, from /docker-entrypoint-initdb.d.
# Creates the replication role, applies the schema for this node kind and, on
# a region, writes the single node_state row.
#
# Needs: ROS_NODE_KIND (hq | region), ROS_REPL_PASSWORD,
#        ROS_REGION_ID (regions only), the repo's db/ folder at /ros/db.
set -eo pipefail

run_sql() {
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" "$@"
}

run_sql -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${ROS_REPL_PASSWORD}'"
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"

for dir in shared "$ROS_NODE_KIND"; do
    for file in /ros/db/"$dir"/*.sql; do
        echo "init-primary: applying $file"
        run_sql -q -f "$file"
    done
done

if [ "$ROS_NODE_KIND" = "region" ]; then
    run_sql -c "INSERT INTO node_state (region_id, epoch, menu_version) VALUES ('${ROS_REGION_ID}', 1, 1)"
fi

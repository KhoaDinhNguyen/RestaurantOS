#!/usr/bin/env bash
# Runs once, on the first start of a primary, from /docker-entrypoint-initdb.d.
# Creates the replication role, applies the schema for this node kind and, on
# a region, writes the single node_state row.
#
# Needs: ROS_NODE_KIND (hq | region), ROS_REPL_PASSWORD,
#        ROS_REGION_ID (regions only), the repo's db/ folder at /ros/db.
set -eo pipefail

# Stop on a missing variable instead of creating a role with an empty password.
# Checked one by one, not with `set -u`: if this file ever loses its executable
# bit, the image's entrypoint sources it instead of running it, and `set -u`
# would then stay switched on inside the entrypoint itself.
: "${ROS_NODE_KIND:?must be hq or region}"
: "${ROS_REPL_PASSWORD:?the replication password is not set}"
case "$ROS_NODE_KIND" in
    hq) ;;
    region) : "${ROS_REGION_ID:?a region node needs ROS_REGION_ID}" ;;
    *)
        echo "init-primary: ROS_NODE_KIND must be hq or region, got '$ROS_NODE_KIND'" >&2
        exit 1
        ;;
esac

run_sql() {
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" "$@"
}

# Values are passed as psql variables and quoted by psql (:'name'), so a quote
# in a password or an ID cannot break the statement. This needs stdin: psql
# does not expand variables inside -c.
run_sql -v repl_password="$ROS_REPL_PASSWORD" <<'SQL'
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD :'repl_password';
SQL
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"

for dir in shared "$ROS_NODE_KIND"; do
    for file in /ros/db/"$dir"/*.sql; do
        echo "init-primary: applying $file"
        run_sql -q -f "$file"
    done
done

if [ "$ROS_NODE_KIND" = "region" ]; then
    run_sql -v region_id="$ROS_REGION_ID" <<'SQL'
INSERT INTO node_state (region_id, epoch, menu_version) VALUES (:'region_id', 1, 1);
SQL
fi

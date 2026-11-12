#!/usr/bin/env bash
# Entrypoint of a standby container. On the first start it clones the primary
# (through the standby's Toxiproxy proxy, so tests can cut replication), then
# hands over to the image's normal entrypoint.
#
# Needs: ROS_PRIMARY_HOST, ROS_PRIMARY_PORT, ROS_REPL_PASSWORD, ROS_SLOT_NAME.
set -euo pipefail

primary="host=${ROS_PRIMARY_HOST} port=${ROS_PRIMARY_PORT} user=replicator password=${ROS_REPL_PASSWORD}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "standby: waiting for primary at ${ROS_PRIMARY_HOST}:${ROS_PRIMARY_PORT}"
    until pg_isready -q -h "$ROS_PRIMARY_HOST" -p "$ROS_PRIMARY_PORT"; do sleep 1; done

    # The slot keeps WAL on the primary while this standby is cut off.
    psql "$primary dbname=postgres" -v ON_ERROR_STOP=1 -qAt -c \
        "SELECT pg_create_physical_replication_slot('${ROS_SLOT_NAME}')
         WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '${ROS_SLOT_NAME}')"

    mkdir -p "$PGDATA"
    chown -R postgres:postgres "$(dirname "$PGDATA")"
    chmod 700 "$PGDATA"

    echo "standby: cloning primary"
    # -R writes standby.signal and primary_conninfo; fast checkpoint so the
    # clone starts at once instead of waiting for the next scheduled one.
    gosu postgres pg_basebackup -d "$primary" -D "$PGDATA" -Fp -Xs -R \
        -S "$ROS_SLOT_NAME" --checkpoint=fast
fi

exec docker-entrypoint.sh "$@"

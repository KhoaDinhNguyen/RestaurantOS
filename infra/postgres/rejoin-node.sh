#!/usr/bin/env bash
# Offline payload for coordinator.rejoin. Run only after the owning Compose
# service has stopped; never run against a live server's data directory.
set -euo pipefail

: "${PGDATA:?}"
: "${ROS_REJOIN_OFFLINE:?must be launched by the recovery command}"
: "${ROS_REJOIN_SOURCE_DSN:?}"
kind=${ROS_REJOIN_KIND:-region}
if [[ "$kind" == region ]]; then
    : "${ROS_REJOIN_REGION:?}"
    : "${ROS_REJOIN_EPOCH:?}"
    [[ "$ROS_REJOIN_EPOCH" =~ ^[0-9]+$ ]]
elif [[ "$kind" == hq ]]; then
    : "${ROS_REJOIN_HQ_ID:?}"
    : "${ROS_REJOIN_HQ_TIMELINE:?}"
else
    echo 'rejoin: invalid node kind' >&2; exit 1
fi
: "${ROS_PRIMARY_HOST:?}"
: "${ROS_PRIMARY_PORT:?}"
: "${ROS_REPL_PASSWORD:?}"
: "${ROS_SLOT_NAME:?}"
: "${ROS_REJOIN_TIMEOUT_S:?}"
queries=/ros/rejoin-sql
PGDATA=$(realpath -m "$PGDATA")
[[ "$ROS_REJOIN_OFFLINE" == 1 && "$PGDATA" == /var/lib/postgresql/* ]]
[[ "$ROS_SLOT_NAME" =~ ^[a-z0-9_]+$ ]]

# The lock is outside PGDATA: a failed rewind may require replacing its entire
# contents. flock releases the lock even if this helper is killed.
exec 9>"${PGDATA}.rejoin.lock"
flock -n 9 || { echo 'rejoin: another recovery is running' >&2; exit 1; }

source_sql() {
    timeout --kill-after=5 "$ROS_REJOIN_TIMEOUT_S" \
        psql -X -w "$ROS_REJOIN_SOURCE_DSN" -v ON_ERROR_STOP=1 -qAt "$@"
}

if [[ -s "$PGDATA/global/pg_control" ]]; then
    target_id=$(LC_ALL=C pg_controldata "$PGDATA" | awk '/Database system identifier/ {print $NF}')
    [[ "$target_id" == "$(source_sql -f "$queries/rejoin_system.sql")" ]] || {
        echo 'rejoin: source and target are different PostgreSQL clusters' >&2
        exit 1
    }
fi

if [[ "$kind" == hq ]]; then
    expected="${ROS_REJOIN_HQ_ID}|${ROS_REJOIN_HQ_TIMELINE}|f"
    source_query=rejoin_hq_source.sql
else
    expected="${ROS_REJOIN_REGION}|${ROS_REJOIN_EPOCH}|f"
    source_query=rejoin_source.sql
fi
[[ "$(source_sql -f "$queries/$source_query")" == "$expected" ]] || {
    echo 'rejoin: source role or epoch does not match HQ; leaving target stopped' >&2
    exit 1
}

slot=$(source_sql -v slot="$ROS_SLOT_NAME" -f "$queries/rejoin_slot.sql")
rebuild=false
if [[ -n "$slot" ]]; then
    IFS='|' read -r slot_type active wal_status <<< "$slot"
    [[ "$slot_type" == physical && "$active" == f ]] || {
        echo 'rejoin: slot is active or not physical; refusing to replace it' >&2
        exit 1
    }
    if [[ "$wal_status" == lost ]]; then
        echo 'rejoin: lost slot requires a fresh clone'
        source_sql -v slot="$ROS_SLOT_NAME" -f "$queries/rejoin_drop_slot.sql" >/dev/null
        slot=''
        rebuild=true
    fi
fi
if [[ -z "$slot" && "$rebuild" == false ]]; then
    source_sql -v slot="$ROS_SLOT_NAME" -f "$queries/rejoin_create_slot.sql" >/dev/null
fi

mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
# Container PIDs are not comparable between namespaces. The controller has
# stopped the owner, so this is a stale PID file from a possible docker kill.
rm -f "$PGDATA/postmaster.pid"

if [[ -s "$PGDATA/PG_VERSION" && "$rebuild" == false ]]; then
    # The promoted source must have checkpointed its new timeline. pg_rewind
    # also performs the crashed target's single-user crash recovery if needed.
    source_sql -f "$queries/rejoin_checkpoint.sql"
    if timeout --kill-after=5 "$ROS_REJOIN_TIMEOUT_S" gosu postgres pg_rewind \
        --target-pgdata="$PGDATA" --source-server="$ROS_REJOIN_SOURCE_DSN" < /dev/null; then
        echo 'rejoin: rewind completed'
    else
        echo 'rejoin: rewind failed; replacing target with a fresh clone'
        rebuild=true
    fi
else
    rebuild=true
fi

# libpq quoting and postgresql.conf quoting are separate layers.
conninfo_value() {
    local value=${1//\\/\\\\}
    value=${value//\'/\\\'}
    printf "'%s'" "$value"
}
stream="host=$(conninfo_value "$ROS_PRIMARY_HOST") port=$ROS_PRIMARY_PORT user=replicator"
stream+=" password=$(conninfo_value "$ROS_REPL_PASSWORD") application_name=$ROS_SLOT_NAME"
stream+=' connect_timeout=5'

if [[ "$rebuild" == true ]]; then
    # A failed rewind can leave an unusable directory. Rebuild only this
    # stopped node, and never start it if pg_basebackup fails.
    # Create the replacement slot inside pg_basebackup, after its starting
    # checkpoint. Reserving it before that checkpoint can immediately lose
    # the slot when the configured WAL retention bound is small.
    slot=$(source_sql -v slot="$ROS_SLOT_NAME" -f "$queries/rejoin_slot.sql")
    if [[ -n "$slot" ]]; then
        IFS='|' read -r slot_type active wal_status <<< "$slot"
        [[ "$slot_type" == physical && "$active" == f ]] || exit 1
        source_sql -v slot="$ROS_SLOT_NAME" -f "$queries/rejoin_drop_slot.sql" >/dev/null
    fi
    find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    timeout --kill-after=5 "$ROS_REJOIN_TIMEOUT_S" gosu postgres pg_basebackup \
        -d "$stream" -D "$PGDATA" -Fp -Xs -R -C -S "$ROS_SLOT_NAME" --checkpoint=fast < /dev/null
    echo 'rejoin: rebuild completed'
fi

conf_stream=${stream//\\/\\\\}
conf_stream=${conf_stream//\'/\'\'}
{
    printf "\nprimary_conninfo = '%s'\n" "$conf_stream"
    printf "primary_slot_name = '%s'\n" "$ROS_SLOT_NAME"
    printf "recovery_target_timeline = 'latest'\n"
} >> "$PGDATA/postgresql.auto.conf"
rm -f "$PGDATA/recovery.signal"
touch "$PGDATA/standby.signal"
chown postgres:postgres "$PGDATA/postgresql.auto.conf" "$PGDATA/standby.signal"
echo 'rejoin: standby configured; controller may now start the target'

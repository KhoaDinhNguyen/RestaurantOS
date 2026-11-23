# PostgreSQL recovery (WP5)

The coordinator automatically promotes a regional standby. Rejoining the old
primary is an explicit host-side operator command, so Docker and a potentially
long data copy do not run in the heartbeat service. Run from the repository
checkout with the prerequisites in `docs/setup.md`.

After the coordinator has completed promotion, inspect the current roles:

```bash
uv run python -m restaurantos.coordinator.rejoin r1 --plan
```

Then recover HQ's recorded standby (the old primary after a failover):

```bash
uv run python -m restaurantos.coordinator.rejoin r1 --compose-file infra/compose.yaml
```

For a bootstrapped cluster, pass `infra/generated/compose.yaml` instead. Database
credentials and cluster topology come from the existing `ROS_` settings. The
command runs on the Docker host; its helper runs inside the selected Compose
network and takes the replication password from that service's existing
`ROS_REPL_PASSWORD`. No Docker socket is needed in the coordinator service.

The command validates HQ's primary/standby pair and the new primary's epoch.
While stopping the old node it holds the same HQ directory row lock used by
promotion. It checks for distinct data volumes, stops only the recorded
standby, and launches an offline helper using that service's existing image
and volume. It does not change HQ's roles, epochs, menus, or events.

The helper checks the source's role, epoch, and PostgreSQL system identifier.
It uses `pg_rewind` when possible, including single-user crash recovery after
an unclean stop. If rewind fails (for example, necessary WAL or checksum/hint
prerequisites are missing), it replaces **only the stopped target's data** with
`pg_basebackup`. An inactive physical slot with `wal_status = 'lost'` also
requires a clone: the old slot is dropped, and the backup creates its replacement.
Active or nonphysical slots are refused. Rebuild discards the old node's divergent
data; it never merges old-primary writes into the promoted primary.

Both rewind and rebuild configure `standby.signal`, a node-specific replication
slot, and the existing `<old-node>__<new-primary>` proxy. This includes the reverse
proxy after the original roles swap. The command reports success only when the
target is in recovery at HQ's epoch and the new primary reports its slot active.
If backup fails or HQ changes before restart, the target remains stopped. Fix the
source/path and rerun the same command. Never manually start a partially recovered
data directory. The helper's volume lock excludes overlapping offline copies.

`ROS_COORDINATOR_REJOIN_TIMEOUT_S` defaults to 300 seconds per database utility
and for the readiness phase. A readiness timeout stops the standby only while
HQ still records the same roles; it refuses to stop a node newly promoted by
the coordinator. This command does not implement HQ promotion, synchronous
fallback, or automatic scheduling of old-node recovery.

The recovery behavior follows PostgreSQL 18's
[pg_rewind documentation](https://www.postgresql.org/docs/18/app-pgrewind.html)
and [pg_basebackup documentation](https://www.postgresql.org/docs/18/app-pgbasebackup.html).

## Tests

```bash
uv run pytest tests/unit/coordinator/test_rejoin.py
```

`tests/cluster/test_coordinator_rejoin.py` requires a **fresh disposable cluster**
with the dev topology, Docker CLI access, and a separate Compose project/volumes.
Do not use a populated cluster. Set `ROS_REJOIN_TEST_COMPOSE` to that cluster's
Compose file and set `ROS_NETWORK` for the test process. Remove the command-line
`max_slot_wal_keep_size` override in this disposable Compose file so the test can
temporarily lower the setting with `ALTER SYSTEM`; leave the committed Compose
file unchanged. The fixture retains 64 MB of WAL to exercise a successful rewind,
then removes that test-only retention and sets a 1 MB slot limit to produce a
real lost slot.

```bash
ROS_REJOIN_TEST_COMPOSE=/absolute/path/to/disposable/compose.json \
ROS_NETWORK=compose uv run pytest -m cluster tests/cluster/test_coordinator_rejoin.py
```

The test kills the primary, waits for the real coordinator and gateway to promote
the standby, rejoins the crashed node with rewind, checks ongoing replication and
read-only enforcement, and cuts/heals the reverse proxy. It then removes a genuine
rewind prerequisite to test clone fallback, forces a lost slot to test slot
replacement, and verifies that an unavailable source leaves the target stopped
until a retry succeeds. Recreate the disposable cluster after the run.

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

## Synchronous standby fallback

The coordinator checks the primary's actual replication settings and its expected
physical standby slot. Confirmed loss through both health paths overrides a
stale streaming view. After `ROS_COORDINATOR_HEARTBEAT_MISSES` consecutive samples
without an eligible streaming synchronous standby, it records
`sync_downgrade_requested` at HQ. A later cycle clears
`synchronous_standby_names` with `ALTER SYSTEM`, reloads PostgreSQL, verifies the
live setting, and records `sync_downgrade`. Local WAL flushing remains enabled;
the primary's existing and new connections stop waiting for a standby.

The HQ request is the recovery journal. A restart after the configuration change
finishes its audit event without repeating the downgrade event. New requests are
scoped to the current primary and epoch. An unfinished promotion, unhealthy
primary, unknown role/epoch, or an unavailable HQ cannot authorize a new change.
Healthy replication and already-asynchronous primaries are left alone. The
eligibility check also detects a severed replication link while the standby's
health probes still answer. A silent replication-only link can remain reported as streaming until
PostgreSQL's WAL sender timeout expires when both health paths still answer.
Confirmed standby loss does not wait for that stale connection to expire.

Fallback stays asynchronous after reconnection or coordinator restart. The docs
do not specify automatic re-enabling, so an operator must explicitly start a new
synchronous experiment. Configure the policy through a reloadable file or
`ALTER SYSTEM`; a command-line `-c synchronous_standby_names=...` override cannot
be disabled by reloading and is reported as an error. Database configuration
privilege errors leave the completion event pending and are logged without
stopping the failover loop. Async fallback trades standby durability for continued
write availability and is visible in `/admin/cluster` and `/admin/events`.

The WP6a generator currently rejects `synchronous = true`. Enabling that bootstrap
switch is a follow-up for its owner. This increment handles primaries already
configured for synchronous replication and adds no bootstrap or proxy changes.
For a two-node experiment, after setup and with the standby streaming, an operator
can run these statements on the current region primary using the configured
connection helpers:

```sql
ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (*)';
SELECT pg_reload_conf();
```

The default `synchronous_commit = on` preserves local flush plus standby waits.
The wildcard supports both the original standby and a node rejoined with its
node-specific application name. These rules assume the documented single
physical standby per region. See PostgreSQL 18's
[replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html)
for synchronous standby selection.

`ROS_SYNC_TEST_CLUSTER=1 ROS_NETWORK=compose uv run pytest -m cluster
 tests/cluster/test_coordinator_sync.py` runs the disposable-cluster scenario.
It proves an existing commit waits before fallback, resumes after downgrade,
keeps its data, and leaves one completion event after a simulated coordinator
restart. It also checks stable async mode after healing and a second automatic
fallback after an operator explicitly re-enables sync and isolates the standby.
Do not enable this flag on a populated cluster. Recreate the disposable cluster
after the test.

## Manual regional failover API

`POST /admin/regions/{region_id}/failover` takes no body. It uses the same
promotion/epoch/directory/event sequence as automatic failover, and an operator
may call it while the current primary is healthy:

```bash
curl -s -X POST localhost:8001/admin/regions/r1/failover
```

A completed promotion returns `200` with `region_id`, the new `epoch`,
`primary_node`, `standby_node`, and the committed `event_id`. The response is
returned after HQ commits its directory update and matching failover event.
The standby must be marked `up` and pass a fresh role/epoch probe. An unknown
region returns `404 {"error":"not_found"}`; an unavailable standby returns
`409 {"error":"standby_unavailable"}`. A locked region directory row or a
partially completed promotion returns `409 {"error":"failover_in_progress"}`.
The fencing epoch transaction flushes WAL locally so it does not wait for the
old partner to rejoin when a promoted node retains synchronous settings. This
is a transaction-local setting for the control write.
The database lock serializes manual and automatic attempts across coordinator
instances. An unrelated region's in-process sampling lock does not block the
request. HQ is excluded from this API.

A request that cannot confirm completion before the configured failover deadline
returns `503`. Promotion itself cannot be rolled back: the background coordinator
can finish a partial operation after a timeout or restart. Inspect
`/admin/cluster` and `/admin/events` before retrying; this POST has no idempotency
key in the current contract. A repeat request cannot promote an old writable
primary with a lower epoch. After any successful promotion, use the rejoin
command above to return the old node as a physical standby. As documented in
architecture Section 6.2, asynchronous promotion can lose unreplicated old-node
writes. The coordinator OpenAPI export includes the response shapes.

The manual endpoint test runs only on a fresh disposable dev topology:

```bash
ROS_MANUAL_FAILOVER_TEST_CLUSTER=1 ROS_NETWORK=compose \
uv run pytest -m cluster tests/cluster/test_coordinator_manual.py
```

It checks actual HTTP responses, standby rejection, a healthy-primary promotion,
competing coordinator instances, the committed event ID, epoch fencing, local
primary selection, and rejection of an unsafe repeat. Recreate that disposable
cluster after the run.

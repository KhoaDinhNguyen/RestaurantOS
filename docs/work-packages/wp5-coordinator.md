# WP5: Coordinator and replication

**Goal:** keep each region writable when its primary fails, without ever letting two primaries accept writes.

**You own:** `src/restaurantos/coordinator/`, `db/queries/coordinator/`, `infra/postgres/` (WP0 wrote the first two scripts there; they are yours from here on), `tests/unit/coordinator/`, the failover tests in `tests/cluster/`.

**Read first:** `docs/architecture.md` Sections 4 and 6.2; Phase 2 Section 2.4 in full (Table 8 is your specification).

**Needs:** WP0. The second-path check and the gateway's reaction to a new epoch need WP2.

## What to build

### 1. Streaming replication (`infra/postgres/`)

- Each standby follows its primary through the proxy `r<n>-standby__r<n>-primary`, so tests can cut replication.
- Asynchronous by default. A switch in `infra/cluster.toml` turns on synchronous commit for the experiment.
- Scripts to build a standby from a primary (`pg_basebackup`) and to rejoin an old primary as a standby (`pg_rewind`, or rebuild if rewind is not possible). A rejoined node follows the new primary through the reverse proxy `r<n>-primary__r<n>-standby`, so that link can be cut too.

### 2. Failure detection

- Every second, run a trivial query on every node through `coordinator__<node>`, with a short timeout. The timeout has to be kept on your side: use `common.db.run_on` (or `common.deadline.run_with_deadline` with your own, shorter limit). A slowed link (T5) delays the answer without making the server slow, so no server-side timeout fires, and a late heartbeat only counts as a miss if your own clock says so.
- Three misses in a row: mark the node `suspect`.
- The interval and the miss count are settings in your own `coordinator/config.py`: `ROS_COORDINATOR_HEARTBEAT_INTERVAL_S` (default 1) and `ROS_COORDINATOR_HEARTBEAT_MISSES` (default 3). T5 needs to change them without a code edit.
- Then ask the gateway (`GET /internal/probe/{node_id}`). Only if that also fails: mark it `down`.

### 3. Failover decision

| Primary | Standby | Action |
| --- | --- | --- |
| down | up | promote |
| down | down | region is isolated: **do nothing**, record a partition event |
| up | down | keep serving; in synchronous mode fall back to asynchronous and record it |

Promotion, in this order:

1. `pg_promote()` on the standby.
2. Set `node_state.epoch` to the new epoch on the new primary. Do this straight after step 1: it is the signal the region's own edge API and relay wait for (see below).
3. In one transaction at HQ: bump the epoch and swap primary and standby in `region_state`; add a `cluster_events` row.

A promotion must be **resumable**. If the coordinator stops between two steps, the region is left half-promoted, and after step 1 alone both nodes claim to be primary at the same epoch, which the local processes read as "do not move". On start, and before any new decision, check each region for an unfinished promotion (the standby is out of recovery, or its epoch is ahead of `region_state`) and finish it from the step that is missing. Every step is safe to repeat.

The old primary still has the old epoch, so it rejects anything the gateway now sends, and the new primary rejects anything stamped with the old epoch.

After a promotion the node called `r<n>-standby` is the primary. Everyone reaches it through its own proxies (`gateway__r<n>-standby`, `hq__r<n>-standby`, `coordinator__r<n>-standby`), which is why `region_state.primary_node` holds a node ID and nothing may assume the primary is the node named `-primary`.

The fence only stops callers that learned the new epoch from HQ. The edge API and the relay sit next to the region's nodes and take their epoch from the node they use, so the fence cannot stop them. They find the primary themselves, with the rule in architecture Section 4: a writable node with a higher `node_state.epoch` wins. Step 2 is what makes that rule work, and it also covers an old primary that is started again without being rejoined.

### 4. Admin API (Section 6.2)

`/admin/cluster`, `/admin/events`, `/admin/exceptions`, and a manual failover route.

The per-region figures all come from SQL on the region primary, through `coordinator__<node>`. The relay keeps no position of its own that you could read (architecture Section 5.3), so there is no cursor to ask.

| Figure | How to get it |
| --- | --- |
| standby lag | `pg_stat_replication` on the primary: `replay_lag`, or bytes with `pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)`. No row means the standby is not connected; say so instead of showing zero |
| outbox backlog | `SELECT count(*) FROM outbox WHERE relayed_at IS NULL` |
| region-to-HQ lag | age of the oldest event HQ has not confirmed: `SELECT clock_timestamp() - created_at FROM outbox WHERE relayed_at IS NULL ORDER BY seq LIMIT 1`. No row means zero |
| orders per second | sample a running total on every heartbeat and report the difference: `SELECT sum(n_tup_ins) FROM pg_stat_user_tables WHERE relname LIKE 'orders\_d%' OR relname = 'orders_default'`. Do not count `orders` rows by `committed_at`: no index covers it |

Both outbox queries use the partial index `outbox_pending_idx`, so they stay cheap however large the outbox grows. When the coordinator cannot reach a region primary (T2), show the last values it read together with their age. A partition is exactly when the backlog is growing, so a zero there would be wrong.

### 5. HQ

Standby with **manual** promotion only: one `rosctl` command. Automatic HQ failover is a stretch goal.

## Done when

- T1 (cut every link to a region primary under load): writes resume on the standby; when the links come back, the old primary rejects every write. The region's edge API and relay have moved to the new primary by themselves.
- Stop the coordinator after each promotion step in turn and start it again: the promotion finishes and the region ends in the same state as an uninterrupted one.
- T1b (`docker kill` the primary): same, and the old primary rejoins as a standby.
- T2 (region isolated from HQ): no promotion happens.
- T5 (latency up to 2,000 ms): no promotion below the threshold.
- The admin endpoint shows roles, status, epochs and lag changing through all of the above.

## Watch out for

- The whole design rests on "never promote unless the standby is reachable and the primary is not, by both paths". Test the negative cases as hard as the positive ones.
- Record wall-clock times for detection, promotion and first new commit: metric 3 (failover time) is built from them.
- A node that flaps between up and suspect must not trigger repeated promotions.
- Each standby has a replication slot, so a primary keeps WAL for a standby that is cut off, but only up to `max_slot_wal_keep_size` (2 GB, set in the Compose file). Past that the slot is marked `lost` and the standby can never catch up: it has to be cloned again, and the lost slot dropped first. Your rejoin script needs that branch; `standby-entrypoint.sh` today only creates a slot when none exists.

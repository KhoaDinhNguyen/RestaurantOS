# WP5: Coordinator and replication

**Goal:** keep each region writable when its primary fails, without ever letting two primaries accept writes.

**You own:** `src/restaurantos/coordinator/`, `infra/postgres/`, `tests/unit/coordinator/`, the failover tests in `tests/cluster/`.

**Read first:** `docs/architecture.md` Sections 4 and 6.2; Phase 2 Section 2.4 in full (Table 8 is your specification).

**Needs:** WP0. The second-path check and the gateway's reaction to a new epoch need WP2.

## What to build

### 1. Streaming replication (`infra/postgres/`)

- Each standby follows its primary through the proxy `r<n>-standby__r<n>-primary`, so tests can cut replication.
- Asynchronous by default. A switch in `infra/cluster.toml` turns on synchronous commit for the experiment.
- Scripts to build a standby from a primary (`pg_basebackup`) and to rejoin an old primary as a standby (`pg_rewind`, or rebuild if rewind is not possible).

### 2. Failure detection

- Every second, run a trivial query on every node through `coordinator__<node>`, with a short timeout.
- Three misses in a row: mark the node `suspect`.
- Then ask the gateway (`GET /internal/probe/{node_id}`). Only if that also fails: mark it `down`.

### 3. Failover decision

| Primary | Standby | Action |
| --- | --- | --- |
| down | up | promote |
| down | down | region is isolated: **do nothing**, record a partition event |
| up | down | keep serving; in synchronous mode fall back to asynchronous and record it |

Promotion, in this order:

1. `pg_promote()` on the standby.
2. Set `node_state.epoch` to the new epoch on the new primary.
3. In one transaction at HQ: bump the epoch and swap primary and standby in `region_state`; add a `cluster_events` row.

The old primary still has the old epoch, so it rejects anything the gateway now sends, and the new primary rejects anything stamped with the old epoch.

### 4. Admin API (Section 6.2)

`/admin/cluster`, `/admin/events`, `/admin/exceptions`, and a manual failover route. Lag figures: standby lag from `pg_stat_replication`; region-to-HQ lag and outbox backlog from the relay's cursor.

### 5. HQ

Standby with **manual** promotion only: one `rosctl` command. Automatic HQ failover is a stretch goal.

## Done when

- T1 (cut every link to a region primary under load): writes resume on the standby; when the links come back, the old primary rejects every write.
- T1b (`docker kill` the primary): same, and the old primary rejoins as a standby.
- T2 (region isolated from HQ): no promotion happens.
- T5 (latency up to 2,000 ms): no promotion below the threshold.
- The admin endpoint shows roles, status, epochs and lag changing through all of the above.

## Watch out for

- The whole design rests on "never promote unless the standby is reachable and the primary is not, by both paths". Test the negative cases as hard as the positive ones.
- Record wall-clock times for detection, promotion and first new commit: metric 3 (failover time) is built from them.
- A node that flaps between up and suspect must not trigger repeated promotions.

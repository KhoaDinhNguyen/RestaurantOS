# WP6b: Faults, snapshots and the checker

**Goal:** break the system on purpose, in a repeatable way, and prove afterwards that nothing was lost or duplicated.

**You own:** `src/restaurantos/sim/` (faults, scenarios, snapshot, checker, control API, and the toxic half of `toxiproxy.py`), `tests/cluster/`.

**Read first:** `docs/architecture.md` Sections 4 (proxy names and link groups), 6.3 and 7.3 to 7.5; Phase 2 Section 2.4 (Tables 8 and 9) and Section 4.

**Needs:** WP6a (a cluster to break and a playback to run under it). The scenarios only pass once WP4 and WP5 work, but you can write and dry-run them earlier.

## What to build

### Faults

Extend **our own** Toxiproxy client in `sim/toxiproxy.py` (WP6a writes the proxy half; no third-party Toxiproxy library). Add the toxic calls:

| Method | HTTP call |
| --- | --- |
| `add_toxic(proxy, type, attributes, stream="downstream", name=None)` | `POST /proxies/{proxy}/toxics` |
| `list_toxics(proxy)` | `GET /proxies/{proxy}/toxics` |
| `remove_toxic(proxy, name)` | `DELETE /proxies/{proxy}/toxics/{name}` |

Then a fault layer on top that works on **link groups** (from `restaurantos.common.cluster.link_groups`), never on single proxy names. A fault on a group applies the same change to every proxy in it and gets one fault ID, so `heal` undoes exactly that change. For `silence`, `latency` and `reset`, add the toxic in both directions (`upstream` and `downstream`).

| Fault | Toxiproxy action |
| --- | --- |
| `cut` | disable every proxy in the group |
| `silence` | `timeout` toxic with `timeout: 0` |
| `latency` | `latency` toxic (`latency`, `jitter` in ms) |
| `reset` | `reset_peer` toxic |

Plus `heal` for one fault and `heal-all` (Toxiproxy `POST /reset`). Real crashes: `docker kill` and `docker start` on a node.

### Scenarios T1 to T6

One script each, following Phase 2 Table 9: start playback at a steady rate, inject, wait, heal, wait for queues to drain, run the checker, write a result file. T1 must record the timestamps needed for failover time (links cut, then next committed order in that region).

### Snapshot and restore

Save: pause playback; record the playback cursor, seed, speed, simulated clock, Toxiproxy state and `cluster.toml`; stop every PostgreSQL container; archive each data volume into `snapshots/<name>/`; start everything again. Restore is the reverse.

### Invariant checker

After queues drain, compare three numbers that must match: orders acknowledged in the run log, orders stored across the regions, orders in HQ's analytics copy. Also: no `external_order_id` stored twice, and every refused or parked order accounted for. Print a clear pass or fail with the differences.

### Control API (port 8002)

Thin HTTP wrappers over all of the above and over playback (Section 6.3), so the Ops view in the UI can drive them. Every action must also work as a `rosctl` command.

## Done when

- Each of T1, T1b and T2 to T6 runs from one command and ends with a checker verdict.
- Save a snapshot mid-run, keep going, restore: the cluster and playback continue from the saved point and the checker still passes at the end.
- The checker fails loudly when you delete an order by hand or insert a duplicate.
- The Ops view can inject and heal a fault and see it listed.

## Watch out for

- After a failover, the checker's three numbers can differ by the orders that were acknowledged or forwarded to HQ before they reached the standby. Report that amount as "lost at failover", with the order ids, instead of failing; with synchronous replication it must be zero (architecture Section 7.5).
- The services answer a silenced or slowed link with `RegionUnreachableError` after `ROS_REQUEST_DEADLINE_S` (default 3 s). `tests/cluster/test_deadline.py` already checks that for one proxy; your `silence` and `latency` faults should produce the same behaviour through the whole stack.
- The checker and the snapshot tool are batch jobs that run on the host. Open plain connections with `restaurantos.common.cluster.host_conninfo`, not `common.db.make_pool`: the pool's 2 s statement timeout is for request paths and will cut a count over every partition.
- Bootstrap writes the cluster's Compose file and proxy list to `infra/generated/` (git-ignored) from `cluster.toml`. Restore must put the snapshot's `cluster.toml` back and regenerate from it before it starts any container; otherwise it starts whatever cluster the working tree describes today.
- Stop **all** PostgreSQL containers before archiving volumes. Copying a running database gives a broken snapshot.
- "Wait for queues to drain" needs a real condition (outbox backlog zero and holding queue empty), not a fixed sleep.
- A cut link leaves the old primary alive. That is the split-brain case we want; do not "fix" it by killing the container.

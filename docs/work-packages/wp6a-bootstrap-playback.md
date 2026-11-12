# WP6a: Bootstrap and playback

**Goal:** turn an empty machine into a loaded cluster, then replay the real orders against it at any speed.

**You own:** `src/restaurantos/sim/` (bootstrap, playback, oracle, the `rosctl` CLI), `tests/unit/sim/`, `tests/fixtures/mini/`.

**Read first:** `docs/architecture.md` Sections 4, 5.4, 7.1 and 7.2; the data reference in the Phase 2 working notes; the project note on replay and stock parameters.

**Needs:** WP0. Posting orders end to end needs WP2 (and WP1 behind it); until then, point playback at a stub server.

## What to build

### Bootstrap: `rosctl bootstrap`

1. Read `infra/cluster.toml` (regions, standbys, sync or async). Generate the Compose services and the Toxiproxy proxy list.
2. Start the nodes and apply `db/hq` and `db/region`.
3. **Assign cells to regions:** count vendors per geohash cell from `vendors.csv`, sort the cells, cut them into N contiguous ranges with roughly equal vendor counts, write `shard_directory`. Do not use order data for this.
4. Load reference data with `COPY`: everything into HQ and `baseline`; each region gets its vendor and product slice plus full copies of the small tables.
5. Initialise `stock` from the **warm-up window only**: starting stock = max(F, ceil(average daily warm-up usage x C)), also stored as `par_level`. Defaults W = 14 days, C = 7, F = 5, restock every R = 7 days.

Data files are read from `ROS_DATA_DIR` (default `data/`). Check them against `MANIFEST.csv` first.

### Playback: `rosctl playback`

- Read orders in (`order_day`, `order_time`) order, starting at day W. Sort once and cache the sorted file.
- **Scheduling:** an order t simulated seconds after the start goes out t / speed wall-clock seconds after the start. Speed can change mid-run, so keep an anchor (wall-clock now, simulated now) and re-anchor on every change. Do not precompute send times. Default speed 1,000x.
- Send with a bounded number of requests in flight.
- **Status events** (on by default): for each accepted order, schedule accepted, preparing, ready and picked-up at +1, +3, +15, +25 simulated minutes plus jitter. Send them to the region's own API.
- **Restock** every R simulated days.
- **Duplicates and shuffling:** resend a configurable share of orders; optionally reorder delivery.
- **Randomness:** derive every random value from a hash of (`seed`, `order_id`, purpose). Never draw from one shared generator in send order.
- **Run log:** one line per order in `runs/<run_id>/orders.jsonl` with the three gateway timestamps and the outcome.
- Controls: start, pause, resume, stop, change speed, `from_day`, `to_day`.

### Single-region mode

`rosctl playback --region r1 --target edge --sequential` sends only that region's orders, straight to its edge API, one at a time. Needs only that region's primary and edge API.

### Oracle

A plain-Python replay with no database: walk the same orders, apply the same stock rules, and report orders accepted, orders refused and final stock per vendor and item. Used to check a region after a sequential run, and to sanity-check the stock parameters before real runs.

## Done when

- `rosctl bootstrap` on a clean machine gives a running, loaded cluster for 1, 2 and 3 regions.
- Every vendor is in exactly one region; row counts match the manifest.
- The same seed gives the same run log twice, including after a mid-run speed change.
- A sequential single-region run matches the oracle exactly.
- The mini fixture exists and the unit tests use it.

## Watch out for

- 2 million orders and 4 million order lines: stream the files, do not build Python objects for all of them up front.
- At 1,000x the sender itself can become the bottleneck. Measure how many orders per second playback can emit against a stub before trusting any throughput number.
- Never commit anything from `data/` or `runs/`.

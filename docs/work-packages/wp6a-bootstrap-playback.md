# WP6a: Bootstrap and playback

**Goal:** turn an empty machine into a loaded cluster, then replay the real orders against it at any speed.

**You own:** `src/restaurantos/sim/` (the Compose and proxy-list generator, bootstrap, playback, oracle, the `rosctl` CLI, and the proxy half of `toxiproxy.py`), `infra/generated/` (git-ignored output), `db/queries/sim/`, `tests/unit/sim/`, `tests/fixtures/mini/`.

**Read first:** `docs/architecture.md` Sections 4, 5.4, 7.1 and 7.2; `docs/replay-stock-parameters.md` (why stock comes from a warm-up window); `docs/schema/data-dictionary.md` (every file and column of the cleaned dataset, and the rules for reading them).

**Needs:** WP0. Posting orders end to end needs WP2 (and WP1 behind it); until then, point playback at a stub server.

## What to build

### Toxiproxy client, proxy half: `sim/toxiproxy.py`

Bootstrap has to create the proxies for any cluster size, so the client starts here. It is **our own small client** on `httpx` over the Toxiproxy HTTP API; do not add a third-party Toxiproxy library. Base URL comes from `Settings.toxiproxy_url`.

| Method | HTTP call |
| --- | --- |
| `list_proxies()` | `GET /proxies` |
| `create_proxy(proxy)` | `POST /proxies` with `name`, `listen`, `upstream`, `enabled` |
| `set_enabled(name, enabled)` | `POST /proxies/{name}` with `{"enabled": ...}` |
| `delete_proxy(name)` | `DELETE /proxies/{name}` |
| `sync(cluster)` | make the server's proxy list equal `restaurantos.common.cluster.proxies(cluster)`: create missing ones, delete extras, re-enable disabled ones |
| `reset()` | `POST /reset`: re-enable every proxy and remove every toxic |

Take names, ports and upstreams only from `restaurantos.common.cluster`; never type a proxy name by hand. Keep the class small and typed; WP6b adds toxic methods to the same class. Unit-test it against `httpx.MockTransport`, no real server needed.

`rosctl` runs on the host, not in a container: it has to drive `docker compose`. Set `ROS_NETWORK=host` for it; the Toxiproxy URL then defaults to `http://127.0.0.1:8474` (inside the Compose network it is `http://toxiproxy:8474`).

### Bootstrap: `rosctl bootstrap`

1. Read `infra/cluster.toml` (regions, standbys, sync or async) with `restaurantos.common.cluster.load_cluster`.
2. **Generate** two files into `infra/generated/` (create the folder if it is missing):
   - `compose.yaml`: Toxiproxy, every PostgreSQL node, and `baseline`. It publishes exactly the ports in `common.cluster.published_ports(cluster, baseline=True)`, each bound to `127.0.0.1`: the Toxiproxy API, every proxy (services in host mode need them) and every node (for batch tools).
   - `proxies.json`: the text of `common.cluster.toxiproxy_config(cluster)`, mounted into the Toxiproxy container.
3. Start from empty volumes: `docker compose -p restaurantos -f infra/generated/compose.yaml down -v`, then `up -d`. Bootstrap always rebuilds from nothing (keeping state is what snapshots are for), so ask before deleting volumes that already exist, unless `--yes` was given.
4. Wait until every node is healthy, then call `sync(cluster)` so the running Toxiproxy matches. Toxiproxy already loaded `proxies.json` when it started. That matters: each standby clones its primary through its proxy during its first start, before `sync` could run.
5. Do **not** apply the schema to HQ or region nodes. Each primary applies its own on first start: `infra/postgres/init-primary.sh` runs `db/shared` plus `db/hq` or `db/region` and writes the region's `node_state` row. Running the files again fails with "already exists". Give `baseline` the region schema the same way (`ROS_NODE_KIND=region`, `ROS_REGION_ID=baseline`).
6. At HQ, insert one `region_state` row per region (`epoch` 1, `primary_node` and `standby_node` from `common.cluster`). `shard_directory` has a foreign key to it, so this comes first.
7. **Assign cells to regions:** count vendors per geohash cell from `vendors.csv`, sort the cells, cut them into N contiguous ranges with roughly equal vendor counts, write `shard_directory`. Do not use order data for this.
8. Load reference data with `COPY`: everything into HQ and `baseline`; each region gets its vendor and product slice plus full copies of the small tables. Follow the rules in the data dictionary; the ones that bite:
   - read every column as text (some ids are all digits, a few with leading zeros);
   - give `COPY` the column list from the file's header (`products.csv` and `product_dish.csv` start with `product_id`, the tables with `vendor_id`);
   - load in foreign-key order: `ingredients`, `dishes`, `dish_ingredients`, `vendors`, `products`, `product_dish`;
   - after loading, check that no ingredient appears in `dish_ingredients` with two different units, and stop with a clear message if one does. A stock counter is kept in its ingredient's unit.
9. Initialise `stock` from the **warm-up window only**: starting stock = max(F, ceil(average daily warm-up usage x C)), also stored as `par_level`. Defaults W = 14 days, C = 7, F = 5, restock every R = 7 days. Usage is `quantity x multiplier x amount`, with `amount` and `multiplier` read from the data. The warm-up window is days 0 to 13: 292,587 orders. Create a row for every counter a vendor's products can need, including ones never used during warm-up; `place_order` treats a missing row as zero.

Data files are read from `ROS_DATA_DIR` (default `data/`). Check them against `MANIFEST.csv` first.

**Which files bootstrap may write.** Only `infra/generated/`, which is git-ignored. The generated Compose file is complete on its own: it does not include or extend `infra/compose.yaml`. Never write to `infra/compose.yaml` or `infra/toxiproxy/proxies.json`; those are the committed one-region dev baseline and belong to WP0. For `regions = 1` your database services must match that baseline (same image, flags, volumes and ports), so the two cannot drift apart. Both use the Compose project name `restaurantos`, so they share container and volume names: only one can run at a time.

**Which connections bootstrap uses.** Bootstrap is a batch job, not a request path. Open plain connections with `psycopg.AsyncConnection.connect(restaurantos.common.cluster.host_conninfo(cluster, node))`: straight to the node from the host, no Toxiproxy, no statement timeout. Do not use `common.db.make_pool`. It stamps every connection with `statement_timeout = 2000` ms, and loading a quarter of a million products or working out starting stock takes longer than two seconds. The same goes for the stock initialisation and for anything else here that runs longer than a request.

### Starting the services: `rosctl up`

The Python services run in one of two modes (architecture Section 4). Host mode is the one to build now:

- `rosctl up` starts, as child processes on the host with `ROS_NETWORK=host`: the gateway, the coordinator, and one relay and one edge API per region. `rosctl down` stops them. Each gets its own `ROS_CLIENT`, `ROS_MODE` and `ROS_REGION_ID`; HTTP ports come from `common.cluster.service_port`.
- Send each process's output to `runs/services/<name>.log`, and make `rosctl up <name>` start a single one, so that someone can run one service under a debugger and the rest normally.
- Container mode comes later: when WP0 adds the image, the generator also writes one service entry per process into `infra/generated/compose.yaml`, with the same names, ports and environment and `ROS_NETWORK=compose`. Keep the list of services in one place in the generator so that both modes read it.

### The baseline: `rosctl baseline sync`

Makes `baseline` hold, on one node, exactly what the regions hold between them (architecture Section 7.1).

1. Refuse to run unless the system is quiet: playback paused or stopped, and no request in flight. Say what is still running.
2. For each region primary, `COPY` out `orders`, `order_items`, `stock` and `product_availability`.
3. On `baseline`, in one transaction: empty those four tables and `COPY` the rows in.
4. Print the row counts per table and per region.

Use plain connections from the host (`host_conninfo`), as bootstrap does. A region that cannot be reached is an error, not something to skip: a baseline with one region missing would make every comparison fail for the wrong reason.

Stretch goal, only if the report needs a write-throughput comparison: no new code, two runs. Bootstrap with `regions = 1` and replay in single-region mode, then bootstrap with `regions = 3` and replay through the gateway, and compare the two run logs.

### Playback: `rosctl playback`

- Read orders in (`order_day`, `order_time`, `order_id` compared as a number) order, starting at day W (1,707,401 orders from day 14 to day 90). `order_items.csv` has no restaurant, line number or price: join it to its order on `order_id`. The data dictionary shows how one order becomes one `POST /orders`. The third key makes the order total: orders in the same second must always go out in the same sequence, or a sequential run cannot match the oracle. Sort once and cache the sorted file.
- **Scheduling:** an order t simulated seconds after the start goes out t / speed wall-clock seconds after the start. Speed can change mid-run, so keep an anchor (wall-clock now, simulated now) and re-anchor on every change. Do not precompute send times. Default speed 1,000x.
- Send with a bounded number of requests in flight.
- **Status events** (on by default): for each accepted order, schedule accepted, preparing, ready and picked-up at +1, +3, +15, +25 simulated minutes plus jitter. Send them to the region's own API.
- **Restock** every R simulated days: at 00:00:00 of days W + R, W + 2R, and so on, for every vendor, before the first order of that day. In sequential mode, wait for the restock calls to finish before sending that order.
- **Duplicates and shuffling:** resend a configurable share of orders; optionally reorder delivery.
- **Randomness:** derive every random value from a hash of (`seed`, `order_id`, purpose). Never draw from one shared generator in send order.
- **Run log:** one line per order in `runs/<run_id>/orders.jsonl` with the three gateway timestamps and the outcome.
- Controls: start, pause, resume, stop, change speed, `from_day`, `to_day`.

### Single-region mode

`rosctl playback --region r1 --target edge --sequential` sends only that region's orders, straight to its edge API, one at a time. Needs only that region's primary and edge API.

### Oracle

A plain-Python replay with no database: walk the same orders in the same order (read the same sorted file as playback), apply the same stock rules and the same restock instants, and report orders accepted, orders refused and final stock per vendor and item. Used to check a region after a sequential run, and to sanity-check the stock parameters before real runs.

The stock rules are WP1's `place_order` steps 3 to 7, with `amount` and `multiplier` read from the files, never assumed. The mini fixture must contain recipe rows with `amount` above 1, a unit other than `portion`, and products with `multiplier` above 1; the real dataset has amount 1 on every row, so it cannot catch a rule that forgets to multiply. The oracle does not need to model `product_availability`: a product is only switched off for stock when one of its counters is at zero, and an order for it would be refused on stock anyway.

## Done when

- `rosctl bootstrap` on a clean machine gives a running, loaded cluster for 1, 2 and 3 regions, and leaves `git status` clean.
- After bootstrap, `GET /proxies` on Toxiproxy lists exactly the proxies `common.cluster.proxies()` gives, all enabled.
- Every vendor is in exactly one region; row counts match the manifest.
- The same seed gives the same run log twice, including after a mid-run speed change.
- A sequential single-region run matches the oracle exactly.
- `rosctl up` brings up every service in host mode for 1 and 3 regions, and an order posted to the gateway lands in the right region.
- After a run and `rosctl baseline sync`, each of the four tables has the same row count on the baseline as on the regions together, and the sync refuses to start while playback is running.
- The mini fixture exists and the unit tests use it.

## Watch out for

- 2 million orders and 4 million order lines: stream the files, do not build Python objects for all of them up front.
- At 1,000x the sender itself can become the bottleneck. Measure how many orders per second playback can emit against a stub before trusting any throughput number.
- Never commit anything from `data/`, `runs/` or `infra/generated/`.

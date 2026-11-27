# The four views

The web UI has four views: HQ, Region, Customer and Ops (`docs/architecture.md` Section 8). You switch between them with the tabs at the top. How to start the UI is in `README.md` in this folder.

## Switching tabs

Each view stays as you left it when you switch to another tab and back: what you typed and picked, the results of your searches and queries, the open restaurant and the last answers.

- A view you have not opened yet starts at its starting point.
- While a view is hidden it does not call the API. Coming back, its live panels (cluster, kitchen board, summaries, ...) refresh at once.
- A query or search you ran once keeps its result until you press **Run** or **Search** again.
- Reloading the page starts every view afresh.

## The header and the simulated clock

The header always shows the **simulated clock**: a day from 0 to 90 and a time of day. Every screen works in that time, never in your computer's. It comes from `GET /sim/clock` and is read once a second.

Before the first playback run the clock has no day. The header then shows a **day** and a **time** box. Fill them in (for example `20` and `12:00:00`) or start a run from the Ops view. The same boxes appear when the control plane does not answer.

In mock mode the header also shows a yellow **MOCK DATA** label.

## HQ (head office)

What head office sees across the whole city.

| Panel                   | What it shows or does                                                                                                                                                                                                                                                                                                                                              | Route                               |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------- |
| Cluster                 | Every database node with its role (`primary` or `standby`), its health (`up`, `suspect` or `down`) and its last heartbeat. Below that, each region's epoch, primary and standby, replication mode, orders per second, standby lag, lag to HQ, outbox backlog, holding queue and menu version. Refreshes every 2 seconds, so a failover shows without a page reload | `GET /admin/cluster`                |
| Failover button         | One per region. Promotes that region's standby to primary after a confirmation. The answer names the new primary and the new epoch                                                                                                                                                                                                                                 | `POST /admin/regions/{id}/failover` |
| Top dishes              | Choose the mode, a day range, an optional time window, how to rank and how many rows, then press **Run**. Tick "refresh every 2 s" to keep it updating                                                                                                                                                                                                             | `GET /query/top-dishes`             |
| Ingredient usage        | The same range fields, plus strategy A, B or C (only for `live` mode)                                                                                                                                                                                                                                                                                              | `GET /query/ingredient-usage`       |
| Cluster events          | Failovers, partitions and recoveries, newest 20                                                                                                                                                                                                                                                                                                                    | `GET /admin/events`                 |
| Exceptions              | Status changes that lost to an earlier one, newest 20                                                                                                                                                                                                                                                                                                              | `GET /admin/exceptions`             |
| Add or change a product | Leave the product id empty to add a product; fill it in to rename or reprice one. A new product needs a dish id and a multiplier. HQ answers `202` with the new menu version                                                                                                                                                                                       | `POST /menu/products`               |

Notes on the query panels:

- **Mode:** `live` asks the regions, `hq` reads head office's copy, and `baseline` reads the single comparison node.
- **Day fields:** both are included. Leave them empty to use the current simulated day.
- **Time fields:** use them together, as `HH:MM:SS`. The window repeats on each day of the range.
- **Meta:** every result has a meta box next to it showing how fresh the data is, which nodes were asked, which regions did not answer, how many rows were transferred and how long it took. A region that did not answer is shown in amber under "missing regions". The query still answers with what it has.

## Region (regional office and kitchens)

What one region sees. Every call goes to that region's own **edge API** (`/edge/r{n}/...`), not to the gateway, so this view keeps working while the region is cut off from head office (test T2).

1. Pick the **region** at the top right.
2. The **summary** shows the day's figures.
3. Type a **vendor id** and press **Open** to get that restaurant's kitchen board, menu and stock.

The region and vendor are kept in the address, for example `/region?region=r1&vendor=00a1f3c2`, so a view can be bookmarked or shared.

| Panel                 | What it shows or does                                                                                                                                                                                                                                              | Route                                                                     |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| Summary               | Epoch, menu version, number of restaurants, orders (total and per status), refused orders, sales units, products switched off, outbox backlog, and the time of the read                                                                                            | `GET /regions/{id}/summary?day=`                                          |
| Kitchen board         | The restaurant's orders of the simulated day, filtered by the status checkboxes (all unfinished statuses by default)                                                                                                                                               | `GET /vendors/{id}/orders?day=&status=`                                   |
| Status buttons        | One button per other status on each order. A change that lost to an earlier one answers `409`, and the panel shows the order's actual status. When no answer came back, **Retry** sends the same change again with the same event id, so it is never counted twice | `POST /vendors/{id}/orders/{order_id}/status`                             |
| Menu and availability | Each product with its price, dish and availability. **Mark sold out** switches a product off by hand; **Switch on** removes that mark                                                                                                                              | `GET /vendors/{id}/menu`, `PUT /vendors/{id}/products/{pid}/availability` |
| Stock                 | Each stock counter with what is on hand, its par level and its unit. **Restock to par** tops every counter up and switches back on the products that were off for lack of stock                                                                                    | `GET /vendors/{id}/stock`, `POST /vendors/{id}/restock`                   |

Switching a product on can still answer "unavailable (stock)" when there is not enough stock to make one unit. That is the server's answer, not an error.

## Customer (delivery-app user)

1. Enter **my geohash cell** at the top right (default `w21z7`).
2. Under **Restaurants in my cell**, search, optionally by cuisine or product name. Only that one cell is searched, not its neighbours.
3. Press **Open menu** on a restaurant.
4. In **Menu of ...**, enter quantities and press **Place order**. The order carries the simulated day and time.
5. **My order** appears once the order has an id, and follows its status every 2 seconds.

Every order goes through the gateway. The possible answers:

| Answer        | Meaning                                                                       | What to do                                                                                                    |
| ------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `201` (green) | stored; shows the order id, the region and the three timestamps               | follow it under **My order**                                                                                  |
| `200` (green) | this order was sent before; the first answer again                            | nothing                                                                                                       |
| `202` (amber) | the region cannot be reached; the order is parked and will be delivered later | press **Ask again**. It sends the identical request with the same order id, so it can never become two orders |
| `409` (red)   | refused as a whole: `out_of_stock`, `unavailable` or `unknown_product`        | press **New order**                                                                                           |

**New order** clears the form and starts a fresh order with a new id.

**Find a dish nearby** answers "who near me can make this right now?":

1. Search a dish name. You get every dish that matches.
2. Press **Nearby** on each dish you want.

One name can be several dishes, for example a chain's own version of a dish (see the `scope` column). So each dish gets its own result list and its own meta box. "Nearby" means your cell and the eight around it.

| Panel                  | Route                                      |
| ---------------------- | ------------------------------------------ |
| Restaurants in my cell | `GET /vendors?geohash=&q=`                 |
| Menu                   | `GET /vendors/{id}/menu` (gateway)         |
| Place order            | `POST /orders`                             |
| My order               | `GET /vendors/{id}/orders/{order_id}?day=` |
| Dish search            | `GET /dishes?q=`                           |
| Nearby                 | `GET /query/nearby?geohash=&dish=`         |

## Ops (the operator, outside the system)

| Panel              | What it shows or does                                                                                                                                                                                                                                                                                                                                                           | Route                                                                                        |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Clock and playback | The simulated clock, the run's state and speed, and its counters (orders sent, acknowledged, parked, refused, events, restocks, ...). **Start** a run with a from day (required), to day, speed, seed and run id; **Pause**, **Resume** and **Stop** it. **Set speed** changes the speed while it runs. A command the current state does not allow answers `409` with the state | `GET /sim/clock`, `GET`, `POST` and `PATCH /sim/playback`                                    |
| Cluster            | The same node and region table as on HQ, without the failover button                                                                                                                                                                                                                                                                                                            | `GET /admin/cluster`                                                                         |
| Links and faults   | Every link group with its proxies and its current faults. Pick a group and a fault, fill in its parameters, then press **Inject**. **Remove** takes one fault away; **Heal all** removes every fault                                                                                                                                                                            | `GET /sim/links`, `POST /sim/faults`, `DELETE /sim/faults/{id}`, `POST /sim/faults/heal-all` |
| Nodes              | **Kill** (after a confirmation) or **Start** a database node. A node id can also be typed, for when the coordinator itself is not answering                                                                                                                                                                                                                                     | `POST /sim/nodes/{id}/kill`, `POST /sim/nodes/{id}/start`                                    |

The four faults:

| Fault     | Effect                                     | Parameters                           |
| --------- | ------------------------------------------ | ------------------------------------ |
| `cut`     | the link refuses connections               | none                                 |
| `silence` | the link stays open but nothing comes back | none                                 |
| `latency` | the link is slow                           | `latency_ms` (required), `jitter_ms` |
| `reset`   | the connection is dropped                  | `timeout_ms`                         |

The optional control-plane routes (scenarios, snapshots, the checker) are not on this view yet.

## How errors look on every view

- **Amber `503`:** the region (or HQ's database) that owns the data cannot be reached right now. During the fault tests this is the expected, correct answer.
- **Red "the service did not answer":** the service itself is not running, or the dev server cannot reach it.
- **Red `404`:** an unknown restaurant, order, product or region.
- Nothing retries on its own. A retry is always a button, and it reuses the same id.

## Walkthrough in mock mode

Start the UI with mock data:

```bash
cd ~/RestaurantOS/frontend
nvm use
npm run dev:mock
```

Open the address Vite prints (usually `http://localhost:5173`). All data is made up and lives in the browser tab: reloading the page starts over.

In mock mode **region `r2` is cut off from HQ**: through the gateway its routes answer `503` and its orders are parked (`202`), while its own edge API keeps working. That lets you see every fault case without breaking anything.

The made-up restaurants:

| Restaurant | Region | Cell    | Notes                                                  |
| ---------- | ------ | ------- | ------------------------------------------------------ |
| `00a1f3c2` | r1     | `w21z7` | noodles; "Noodle soup, large" is off for lack of stock |
| `3b7e91d0` | r1     | `w21z7` | rice; "Chicken rice set of 2" is switched off by hand  |
| `71c4d2aa` | r1     | `w21z6` | dessert, part of chain `c0042`                         |
| `9e02b5f4` | r2     | `w21zk` | noodles; in the cut-off region                         |

### 1. Start the clock

1. Open **Ops**.
2. Leave from day at `14` and press **Start**.
3. The header now shows `day 14` and a time that moves forward. The playback state reads `running`.
4. Try **Set speed** with `5000`, then **Pause** and **Resume**. Pressing **Resume** while running answers `409 invalid_state`.

### 2. Place an order

1. Open **Customer**. The cell is already `w21z7`. Press **Search w21z7**.
2. Press **Open menu** on `00a1f3c2`.
3. Enter `2` for "Noodle soup" and press **Place order**.
4. A green `201` names the order id and region `r1`. **My order** appears below with the status `received`.

### 3. Work the order in the kitchen

1. Open **Region**, keep region `r1`, type `00a1f3c2` and press **Open**.
2. The kitchen board shows your order. Press `accepted`, then `preparing`.
3. Go back to **Customer**: **My order** now shows `preparing`.
4. Back on **Region**, press `received` on the same order. The state machine does not allow going back, so you get `409`: "received lost to an earlier change".

### 4. Sold out and restock

1. Still on **Region** for `00a1f3c2`, press **Mark sold out** on "Noodle soup". It now reads "no (manual)".
2. On **Customer**, try to order it: a red `409 (unavailable)`.
3. On **Region**, press **Switch on**, then **Restock to par** in the Stock panel. Every counter reaches its par level, and "Noodle soup, large" (off for stock) comes back on.

### 5. A region cut off from HQ

1. On **Customer**, change the cell to `w21zk`, search, and open `9e02b5f4`.
2. The menu answers an amber `503: region r2 cannot be reached`.
3. Press **Place order** anyway: an amber `202`, parked for later delivery.
4. Press **Ask again**: the same order id is sent again, so it is still one order.
5. Open **Region**, choose `r2`, type `9e02b5f4` and press **Open**. The summary, kitchen board, menu and stock all load, with no `503`, because this view calls the region's own edge API, which does not need HQ.
6. Go back to **Customer**: the `503` and the `202` are still there, and they should be. The Customer view always goes through the gateway, and in mock mode `r2` stays cut off from HQ for the whole session. Opening the restaurant on **Region** does not reconnect it, and neither does **Heal all** on **Ops** (step 8), which in mock mode only clears the fault list.

### 6. Head office

1. Open **HQ**. The cluster table shows `r2-primary` as `suspect` and `r2`'s standby lag as "not connected".
2. Press **Run** on Top dishes in `live` mode. The meta box shows **missing regions: r2**.
3. Switch the mode to `hq` and run again: no missing regions, and the nodes asked are `hq-primary`.
4. Run Ingredient usage with strategy `B`, then `C`, and compare their meta.
5. Press **Failover** on `r2` and confirm. The answer names the new primary, `r2-standby`, and the table shows epoch `2`. A `failover` event appears under Cluster events.

### 7. Find a dish nearby

1. On **Customer**, set the cell back to `w21z7`.
2. Under **Find a dish nearby**, search `chicken rice`. Two dishes come back: a global one and chain `c0042`'s own.
3. Press **Nearby** on both. Each gets its own list and meta. The chain's dish is only sold by `71c4d2aa`, one cell away (`distance_m` 4890).

### 8. Faults and nodes

1. Open **Ops**. Under **Links and faults**, `link:r2<->hq` already has a `cut`.
2. Pick `link:r1<->hq`, choose `latency`, enter `latency_ms` `4000` and press **Inject**. The fault appears under that group.
3. Injecting the same kind on the same group again answers `409 fault_exists`.
4. Press **Heal all**: every fault is removed.
5. Under **Nodes**, press **Kill** on `r1-primary`. Its status turns `down` in the cluster table. **Start** brings it back `up`.

Against the real system the steps are the same. The data, the faults and the failovers are then real, and a `503` or `202` means a region really cannot be reached.

# The four views

The web UI has four views: HQ, Region, Customer and Ops (`docs/architecture.md` Section 8). You switch between them with the tabs at the top. How to start the UI is in `README.md` in this folder.

## Switching tabs

Each view stays as you left it when you switch to another tab and back: what you typed and picked, the results of your searches and queries, the open restaurant and the last answers.

- A view you have not opened yet starts at its starting point.
- While a view is hidden it does not call the API. Coming back, its live panels (cluster, kitchen board, summaries, ...) refresh at once.
- A query or search you ran once keeps its result until you press **Run** or **Search** again.
- Reloading the page starts every view afresh.

## The header and the simulated clock

The header always shows the **simulated clock**: a day from 0 to 90 and a time of day, with the playback state and speed next to it. Every screen works in that time, never in your computer's. It comes from `GET /sim/clock` and is read once a second.

Before the first playback run the clock has no day. The header then says "no run yet: pick a day" and shows a **day** and a **time** box. Fill them in (for example `20` and `12:00:00`) or start a run from the Ops view. The same boxes appear, with "control plane not answering: pick a day", when the control plane does not answer.

In mock mode the header also shows a yellow **MOCK DATA** label.

## HQ (head office)

What head office sees across the whole city. From top to bottom: the cluster, the two queries side by side, cluster events and exceptions side by side, and the product form.

| Panel                   | What it shows or does                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Route                               |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------- |
| Cluster                 | Every database node with its region (`HQ` for head office's two), its role (`primary` or `standby`), its health (`up`, `suspect` or `down`) and its last heartbeat. Below that, each region's epoch, primary and standby, replication mode, orders per second, standby lag, lag to HQ, outbox backlog, holding queue, menu version and how old those figures are. A region running without a standby (after T1, until it is rejoined) shows "no standby". Refreshes every 2 seconds, so a failover shows without a page reload | `GET /admin/cluster`                |
| Failover button         | One per region, at the end of its row. Promotes that region's standby to primary after a confirmation. The answer names the new primary and the new epoch. A region with no standby answers `409 standby_unavailable`                                                                                                                                                                                                                                                                                                          | `POST /admin/regions/{id}/failover` |
| Top dishes              | Choose the mode, a day range, an optional time window, **rank by** (`units` by default, or `quantity`, `order_lines`, `sales_units`) and **limit** (10 by default), then press **Run**. The rows keep the order the server ranked them in and show all four numbers. Tick "refresh every 2 s" to keep it updating                                                                                                                                                                                                              | `GET /query/top-dishes`             |
| Ingredient usage        | The same mode, day and time fields, plus the **strategy** A, B or C (C by default). The strategy box is greyed out unless the mode is `live`. Each row is an ingredient, its amount and its unit. It runs once per **Run**                                                                                                                                                                                                                                                                                                     | `GET /query/ingredient-usage`       |
| Cluster events          | Failovers, partitions and recoveries: the newest 20, oldest first, with their wall-clock time, region and node. Refreshes every 2 seconds                                                                                                                                                                                                                                                                                                                                                                                      | `GET /admin/events`                 |
| Exceptions              | Status changes that lost to an earlier one: the newest 20, each with its region, its order, the status the order had ("was") and the one that lost ("attempted"). Refreshes every 2 seconds                                                                                                                                                                                                                                                                                                                                    | `GET /admin/exceptions`             |
| Add or change a product | Fill in **vendor id**, **name** and **price units**. Leave **product id (empty = new)** empty to add a product, or fill it in to rename or reprice one. A new product also needs a **dish id** and a **multiplier** (1 by default); find a dish id with the dish search on the Customer view. An optional **note** goes with the menu version. Press **Publish**: HQ answers `202` with the new menu version and, per product, its region and "created" or "changed or already there"                                          | `POST /menu/products`               |

Notes on the query panels:

- **Mode:** `live` asks the regions, `hq` reads head office's copy, and `baseline` reads the single comparison node.
- **Day fields:** both are included. Leave them empty to use the current simulated day, which the grey text in the boxes shows.
- **Time fields:** use them together, as `HH:MM:SS`. The window repeats on each day of the range.
- **Meta:** every result has a meta box above it showing how fresh the data is, which nodes were asked, which regions did not answer, how many rows were transferred and how long it took. A region that did not answer is shown in amber under "missing regions". The query still answers with what it has.
- Under the meta box, a line repeats what the server answered for: the days, the time window, and the ranking or the strategy.

## Region (regional office and kitchens)

What one region sees. Every call goes to that region's own **edge API** (`/edge/r{n}/...`), not to the gateway, so this view keeps working while the region is cut off from head office (test T2).

1. Pick the **region** at the top right.
2. The **summary** shows the day's figures. It needs a simulated day: before the first run, pick one in the header.
3. Type a vendor id in **restaurant (vendor id)** and press **Open** to get that restaurant's kitchen board, menu and stock.

The region and vendor are kept in the address, for example `/region?region=r1&vendor=00a1f3c2`, so a view can be bookmarked or shared. Changing the region closes the open restaurant.

| Panel                 | What it shows or does                                                                                                                                                                                                                                                                                                                                                                                                                     | Route                                                                     |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Summary               | Epoch, menu version, number of restaurants, orders (total and per status), refused orders, sales units, products switched off, outbox backlog, and the time of the read. Refreshes every 2 seconds                                                                                                                                                                                                                                        | `GET /regions/{id}/summary?day=`                                          |
| Kitchen board         | The restaurant's orders of the simulated day, oldest first, with their time, order id, channel, items and status. The status checkboxes filter them: `received`, `accepted`, `preparing` and `ready` by default. Refreshes every 2 seconds                                                                                                                                                                                                | `GET /vendors/{id}/orders?day=&status=`                                   |
| Status buttons        | Under "change to", one button per other status on each order. The answer shows above the board: green "Order ... is now ..." when it was applied; amber `409` "... lost to an earlier change; the order is ..." when an earlier change won; grey "Already applied (same event id)" for a repeat. When no answer came back, **Retry ... (same event id)** sends the same change again with the same event id, so it is never counted twice | `POST /vendors/{id}/orders/{order_id}/status`                             |
| Menu and availability | Each product with its price, dish and availability: "yes", or "no" with the reason (`manual` or `stock`). **Mark sold out** switches a product off by hand; on a product switched off by hand the button reads **Switch on** and removes that mark. Refreshes every 2 seconds                                                                                                                                                             | `GET /vendors/{id}/menu`, `PUT /vendors/{id}/products/{pid}/availability` |
| Stock                 | Each stock counter (ingredient or dish) with what is on hand, its par level, its unit ("servings" for a dish counter) and when it last changed. **Restock to par** tops every counter up and switches back on the products that were off for lack of stock; the answer says how many counters were restocked and how many products came back on. Refreshes every 2 seconds                                                                | `GET /vendors/{id}/stock`, `POST /vendors/{id}/restock`                   |

Switching a product on can still answer "unavailable (stock)" when there is not enough stock to make one unit. That is the server's answer, not an error.

## Customer (delivery-app user)

1. Enter **my geohash cell** at the top right (default `w21z7`).
2. Under **Restaurants in my cell**, press **Search** (the button names the cell), optionally with a cuisine or product name. Only that one cell is searched, not its neighbours.
3. Press **Open menu** on a restaurant. Its button then reads "selected".
4. In **Menu of ...**, enter quantities and press **Place order**. The order carries the simulated day and time; before the first run, pick a day in the header first, or the button stays greyed out. The menu refreshes every 5 seconds.
5. **My order** appears once the order has an id, and follows its status every 2 seconds.

Every order goes through the gateway. The possible answers:

| Answer                    | Meaning                                                                       | What to do                                                                                             |
| ------------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `201` (green)             | stored; shows the order id, the region and the three timestamps               | follow it under **My order**                                                                           |
| `200` (green)             | this order was sent before; the first answer again ("already placed before")  | nothing                                                                                                |
| `202` (amber)             | the region cannot be reached; the order is parked and will be delivered later | press **Ask again (same order id)**. It sends the identical request, so it can never become two orders |
| `409` (red)               | refused as a whole: `out_of_stock`, `unavailable` or `unknown_product`        | press **New order**                                                                                    |
| no answer, or `5xx` (red) | the request may or may not have arrived                                       | press **Retry (same order id)**: the same request again, never a second order                          |

After the order is sent, the quantities are locked and the order's `external_order_id`, day and time show next to the buttons. **New order** clears the form and starts a fresh order with a new id.

**Find a dish nearby** answers "who near me can make this right now?":

1. Type a **dish name** and press **Search dishes**. You get every dish that matches, with its id, its `scope` and how many restaurants sell it.
2. Press **Nearby** on each dish you want; **Hide** closes it again.

One name can be several dishes, for example a chain's own version of a dish (see the `scope`). So each dish gets its own result list and its own meta box. "Nearby" means your cell and the eight around it. Each row is a restaurant, its product, the price, the distance in metres and the region.

| Panel                  | Route                                      |
| ---------------------- | ------------------------------------------ |
| Restaurants in my cell | `GET /vendors?geohash=&q=`                 |
| Menu                   | `GET /vendors/{id}/menu` (gateway)         |
| Place order            | `POST /orders`                             |
| My order               | `GET /vendors/{id}/orders/{order_id}?day=` |
| Dish search            | `GET /dishes?q=`                           |
| Nearby                 | `GET /query/nearby?geohash=&dish=`         |

## Ops (the operator, outside the system)

From top to bottom: clock and playback, the cluster, links and faults next to nodes, then the job panels.

| Panel              | What it shows or does                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Route                                                                                        |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Clock and playback | The simulated clock, the run's state and speed. **Start** a run with a from day (required), to day, speed (1000 by default), seed, run id, **duplicate rate** (a share from 0 to 1 of orders sent twice) and **shuffle**; **Pause**, **Resume** and **Stop** it. **Set speed** changes the speed while it runs. A command the current state does not allow answers `409` with the state. Below: the run id, seed and days; where dispatch has got to ("dispatched up to", "earliest held order", "next due"); the counters (orders sent, acknowledged, parked, refused, failed, resent, events, restocks, in flight, ...); and **engine**, which opens the settings fixed when the playback process started | `GET /sim/clock`, `GET`, `POST` and `PATCH /sim/playback`                                    |
| Cluster            | The same node and region table as on HQ, without the failover button                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `GET /admin/cluster`                                                                         |
| Links and faults   | Every link group with its proxies and its current faults. Pick a group and a fault, fill in its parameters, then press **Inject**. **Remove** takes one fault away; **Heal all** removes every fault. An amber box under a group lists what Toxiproxy holds that no known fault explains (a leftover from an earlier control process) and proxies a cut found already disabled; **Heal all** clears the first. Refreshes every 2 seconds                                                                                                                                                                                                                                                                    | `GET /sim/links`, `POST /sim/faults`, `DELETE /sim/faults/{id}`, `POST /sim/faults/heal-all` |
| Nodes              | **Kill** (after a confirmation) or **Start** a database node. A node id can also be typed, for when the coordinator itself is not answering. The answer says whether the node is now running                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `POST /sim/nodes/{id}/kill`, `POST /sim/nodes/{id}/start`                                    |

The four faults:

| Fault     | Effect                                     | Parameters                           |
| --------- | ------------------------------------------ | ------------------------------------ |
| `cut`     | the link refuses connections               | none                                 |
| `silence` | the link stays open but nothing comes back | none                                 |
| `latency` | the link is slow                           | `latency_ms` (required), `jitter_ms` |
| `reset`   | the connection is dropped                  | `timeout_ms`                         |

Below them, the work the control plane runs as **jobs**, one at a time (architecture Section 6.3):

| Panel     | What it shows or does                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Route                                                                             |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| Job       | The job started from this page: its id, kind, state (`running`, `done` or `failed`) and times, the progress line while it runs (for a scenario, its phase), then the outcome. A scenario shows `PASSED` or `FAILED`, each condition, its measurements (failover time, drain time, ...), the checker's result, warnings and next steps. A check shows its verdict and numbers. A restore shows whether playback resumed and, if it could not resume the run itself, the command that does. **full result** opens the whole answer. Asked every second while it runs | `GET /sim/jobs/{id}`                                                              |
| Scenarios | Pick T1 to T6 under **scenario** and read its one-line summary. Fill in only the parameters you want to change (the grey text is the default; empty means default) and press **Run T1** (the button names the scenario), then confirm. It starts its own playback run, breaks the cluster under load, heals it and runs the checker                                                                                                                                                                                                                                | `GET /sim/scenarios`, `POST /sim/scenarios/{name}`                                |
| Snapshots | The saved snapshots with their simulated point, regions and when they were taken. Type a **new snapshot name** and press **Take snapshot**. **Restore** (after a confirmation) puts every database back and replays the snapshot's day as a new run; "services after restore" (`auto`, `host`, `containers` or `none`) and "replace cluster file" are its options. The list refreshes every 5 seconds                                                                                                                                                              | `GET /sim/snapshots`, `POST /sim/snapshots`, `POST /sim/snapshots/{name}/restore` |
| Checker   | Enter one or more **run ids** (comma-separated) and press **Run checker**. Tick "allow stored-but-unanswered orders" for runs that had faults. Check a run once it has stopped                                                                                                                                                                                                                                                                                                                                                                                     | `POST /sim/check`                                                                 |

While a job runs, the job buttons (**Run**, **Take snapshot**, **Restore**, **Run checker**) are greyed out. A job started elsewhere (for example `rosctl scenario` in a shell) makes a new one answer `409 busy`, and the Job panel then follows the running one. Starting a run with **duplicate rate** and **shuffle** on the playback panel is what T4 does by hand.

Against the real control plane, every button on this view that changes something needs this page's origin in `ROS_SIM_CONTROL_ALLOWED_ORIGINS` (see `README.md`). Without it the answer is a red `403` that says so.

## How errors look on every view

- **Amber `503`:** the region (or HQ's database) that owns the data cannot be reached right now. During the fault tests this is the expected, correct answer.
- **Red "the service did not answer":** the service itself is not running, or the dev server cannot reach it.
- **Red `404`:** an unknown restaurant, order, product or region.
- **Red `403` naming `ROS_SIM_CONTROL_ALLOWED_ORIGINS`:** the control plane refused a button on the Ops view because this page's origin is not on its list (see `README.md`).
- **Red `422` with a message:** the server refused what was typed (a bad day, time or name) and says why.
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
3. The header now shows `day 14` and a time that moves forward. The playback state reads `running`, and "dispatched up to" shows `day 14 · 00:00:00`.
4. Try **Set speed** with `5000`, then **Pause** and **Resume**. Pressing **Resume** while running answers `409 invalid_state`.

### 2. Place an order

1. Open **Customer**. The cell is already `w21z7`. Press **Search w21z7**.
2. Press **Open menu** on `00a1f3c2`.
3. Enter `2` for "Noodle soup" and press **Place order**.
4. A green `201` names the order id and region `r1`. **My order** appears below with the status `received`.

### 3. Work the order in the kitchen

1. Open **Region**, keep region `r1`, type `00a1f3c2` and press **Open**.
2. The kitchen board shows your order. Press `accepted`, then `preparing`. Each answer reads "Order ... is now ...".
3. Go back to **Customer**: **My order** now shows `preparing`.
4. Back on **Region**, press `received` on the same order. The state machine does not allow going back, so you get `409`: "received lost to an earlier change; the order is preparing".

### 4. Sold out and restock

1. Still on **Region** for `00a1f3c2`, press **Mark sold out** on "Noodle soup". It now reads "no (manual)" and its button reads **Switch on**.
2. On **Customer**, try to order it: a red `409: refused (unavailable)`.
3. On **Region**, press **Switch on**, then **Restock to par** in the Stock panel. Every counter reaches its par level, and "Noodle soup, large" (off for stock) comes back on.

### 5. A region cut off from HQ

1. On **Customer**, change the cell to `w21zk`, search, and open `9e02b5f4`.
2. The menu answers an amber `503: region r2 cannot be reached`.
3. Press **Place order** anyway: an amber `202`, parked for later delivery.
4. Press **Ask again (same order id)**: the same order id is sent again, so it is still one order.
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
2. Under **Find a dish nearby**, search `chicken rice` with **Search dishes**. Two dishes come back: a global one and chain `c0042`'s own.
3. Press **Nearby** on both. Each gets its own list and meta. The chain's dish is only sold by `71c4d2aa`, one cell away (`distance_m` 4890).

### 8. Faults and nodes

1. Open **Ops**. Under **Links and faults**, `link:r2<->hq` already has a `cut`.
2. `node:r1-primary` shows an amber box: a toxic left in Toxiproxy that no known fault explains.
3. Pick `link:r1<->hq`, choose `latency`, enter `latency_ms` `4000` and press **Inject**. The fault appears under that group.
4. Injecting the same kind on the same group again answers `409 fault_exists`.
5. Press **Heal all**: every fault is removed, and the amber box is gone.
6. Under **Nodes**, press **Kill** on `r1-primary` and confirm. Its status turns `down` in the cluster table. **Start** brings it back `up`.

### 9. Scenarios, snapshots and the checker

1. Under **Scenarios**, pick `T2`, leave the fields empty, press **Run T2** and confirm.
2. The **Job** panel shows the progress line ("r1 broken: 25 s left", then "running the checker"), then `PASSED` with the conditions, the measurements and the checker's numbers. In mock mode a job takes about two seconds. While it runs, the job buttons are greyed out.
3. Under **Checker**, type `mock-t2` and press **Run checker**: a check job that passes.
4. Under **Snapshots**, take one named `demo`; it appears in the list within five seconds of its job finishing. **Restore** on `before-t2` and confirm: the Job panel says playback resumed as run `mock-run-r1`.

Against the real system the steps are the same. The data, the faults and the failovers are then real, and a `503` or `202` means a region really cannot be reached.

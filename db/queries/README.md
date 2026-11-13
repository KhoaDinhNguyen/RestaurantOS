# Named queries

Loaded by `restaurantos.common.sqlfiles.load_query()`. One statement per file.

One subfolder per component, owned by that component's work package:

| Folder | Owner |
| --- | --- |
| `common/` | WP0 |
| `region/` | WP1 |
| `gateway/` | WP2 |
| `router/` | WP3 |
| `relay/` | WP4 |
| `coordinator/` | WP5 |
| `sim/` | WP6a and WP6b |

`load_query("region/place_order")` reads `db/queries/region/place_order.sql`.

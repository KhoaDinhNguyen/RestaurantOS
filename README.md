# RestaurantOS

A distributed order system for a restaurant chain, built as the group project for CSE 512 (Distributed Database Systems) at Arizona State University.

Restaurants are grouped into regions by location. Each region has its own PostgreSQL database with a live standby, takes its own orders and keeps its own stock. Head office (HQ) holds the menu, the directory of regions, and a copy of every order for reporting. A simulation replays three months of real food-delivery orders against the system, breaks links and nodes on purpose, and checks afterwards that no order was lost or stored twice.

## What it shows

| Course topic | In this system |
| --- | --- |
| Partitioning | restaurants are assigned to regions by geohash cell; a gateway routes each request to its region |
| Replication and consistency | a streaming standby per database; order status is always read from the region's primary; HQ's copy of the orders arrives in the background and may lag |
| Failover | a coordinator promotes the standby when a primary is lost; a region epoch stops the old primary from accepting writes afterwards |
| Working through a partition | orders for an unreachable region are parked and delivered later; a region keeps working when cut off from HQ; an order sent twice is stored once |
| Distributed queries | questions that span regions are answered by scatter-gather, with three join strategies to compare |

## Status

The foundations are in place: the database schema, the shared Python code and a small development cluster. The services are being built as separate work packages.

## Quick start

Install the system components first: `docs/setup.md` (Ubuntu 24.04, or WSL 2 on Windows).

```bash
uv sync                                         # Python 3.14 and all dependencies
uv run pre-commit install --install-hooks
docker compose -f infra/compose.yaml up -d      # HQ and one region, with standbys, and Toxiproxy
uv run pytest -m "not integration and not cluster"
```

## Where to read next

| If you want to | Read |
| --- | --- |
| understand the design | `docs/architecture.md` |
| build a part of it | `docs/work-packages/README.md`, then the brief of your package |
| set up a machine | `docs/setup.md` |
| know how we work: branches, tests, reviews | `CONTRIBUTING.md` |
| know what is in the dataset | `docs/schema/data-dictionary.md` |
| point a coding agent at the repository | `AGENTS.md` |

## Layout

```text
db/        hand-written SQL: the schema, and the queries each component runs
src/       the Python package: common code, region logic, gateway, relay, coordinator, simulation
frontend/  the web UI (React)
infra/     the development cluster: Docker Compose, PostgreSQL scripts, Toxiproxy proxies
tests/     unit, integration and cluster tests
docs/      architecture, work-package briefs, setup, data dictionary
```

## Dataset

The order data is a cleaned version of the Delivery Hero Recommendation Dataset, limited to Singapore, with ingredients from RecipeNLG. It is not public and is not in this repository; `docs/setup.md` says where team members get it. Tests run on a small hand-made dataset instead.

## License

GNU Affero General Public License v3. See `LICENSE`.

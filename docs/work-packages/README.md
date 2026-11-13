# Work packages: start here

RestaurantOS is split into work packages so that four people can build it at the same time. Each package has a brief in this folder. This page tells you what to read, the rules every package shares, and how to run things.

## Reading order

1. This page.
2. `docs/architecture.md` Section 1 (the big picture), Section 3 (who owns what) and Section 4 (what runs where).
3. The brief of your package. Its "Read first" line names the other sections you need; you do not have to read the whole architecture document.
4. `CONTRIBUTING.md` for branches, pull requests and tests. If you work with a coding agent, it reads `AGENTS.md`.

The Phase 2 document explains why the system is designed this way. The files in this repository say how it is built. Where they differ, the repository is right, and `docs/architecture.md` Section 10 lists what was decided since.

## The packages

| WP | Brief | In one line | Can start | Owner |
| --- | --- | --- | --- | --- |
| 0 | (done) | schema, shared code, dev cluster | | Mark |
| 1 | `wp1-region-write-path.md` | the order transaction, stock and status changes on one region | now | open |
| 2 | `wp2-gateway.md` | the HTTP front door; sends each request to its region | now; real writes need WP1 | open |
| 3 | `wp3-query-router.md` | questions that span regions, and the three join strategies | now | open |
| 4 | `wp4-relay.md` | moves events up to HQ, and parked orders and menus back down | now; real events need WP1 | open |
| 5 | `wp5-coordinator.md` | notices a dead node, promotes its standby, reports health | now | open |
| 6a | `wp6a-bootstrap-playback.md` | builds the cluster, loads the data, replays the orders | now | open |
| 6b | `wp6b-faults-snapshot-checker.md` | breaks the system on purpose and checks nothing was lost | the fault layer now; the rest after WP6a | open |
| 7 | `wp7-web-ui.md` | the four screens | now, against mocked answers | open |

"Now" means the package can be built and tested on its own. Each brief's "Run it on its own" section says how.

## How a brief is laid out

Every brief has the same parts, in the same order:

| Part | What it gives you |
| --- | --- |
| Goal, You own, Read first, Needs | the job in one sentence, your folders, what to read, what must exist first |
| Shared code you must use | the functions that already exist for you. Do not write your own version of these |
| What to build | the work itself |
| Run it on its own | how to start and try your part before the others are finished |
| Done when | the checks that say you are finished |
| Watch out for | traps other people already fell into |

Each "Done when" line starts with a mark that says what it takes to check it:

| Mark | Needs |
| --- | --- |
| `unit` | nothing: a plain unit test |
| `db` | one PostgreSQL (an integration test) |
| `cluster` | the dev cluster and Toxiproxy |
| `data` | a bootstrapped cluster with data in it (WP6a) |

A note in brackets names any other package that must exist first.

## Rules every package shares

Short versions. The place that explains each one is in brackets.

1. **Reach databases through `common.db`.** Build connection strings with `node_conninfo`, pools with `make_pool`, and run every call on a request path through `run_on`. Never type a host name or a port. (architecture Section 5.2)
2. **Never cancel a task that is inside a database call.** No `task.cancel()`, `asyncio.wait_for` or `asyncio.timeout` around psycopg. `run_on` gives up without cancelling. (`src/restaurantos/common/deadline.py`)
3. **A service works in both network modes.** `ROS_NETWORK=host` on your machine, `compose` in a container, with no code difference. (architecture Section 4)
4. **Every write to a region carries the epoch and goes through `restaurantos.region`.** No other package writes a region's tables with its own SQL. (architecture Section 5.2)
5. **When a region cannot be reached:** orders are parked and answered `202`; every other route answers `503` and names the region; queries answer with what they have and list the missing regions. Nothing falls back to HQ's copy. (architecture Section 6.1)
6. **Time is simulated.** "Today" is the `order_day` in the request. Never use `now()` or the machine's clock for it. (architecture Section 5.4)
7. **Amounts, units and multipliers are data.** Never assume an amount of 1 or the unit `portion`, in code or in tests. (`docs/schema/data-dictionary.md`)
8. **Your settings live in your own `config.py`**, with the prefix `ROS_<COMPONENT>_`. Add each one to `.env.example`. (`AGENTS.md`)
9. **Your SQL lives in `db/queries/<component>/`**, one statement per file, loaded with `load_query`. (`db/queries/README.md`)
10. **The contracts are fixed:** the schema in `db/`, `common/models.py`, the HTTP routes and the proxy names. Ask before changing one, and before adding a backend dependency. (`CONTRIBUTING.md`)

## Running things

Everything below runs on your machine. The databases and Toxiproxy run in Docker; your service runs next to them.

```bash
# once
cp .env.example .env                              # then set ROS_NETWORK=host in it

# the dev cluster: HQ and one region, each with a standby, and Toxiproxy
docker compose -f infra/compose.yaml up -d
docker compose -f infra/compose.yaml down -v      # stop it and delete its data
```

| What | Where |
| --- | --- |
| HQ primary and standby | `127.0.0.1:5500` and `5501` |
| Region 1 primary and standby | `127.0.0.1:5510` and `5511` |
| Toxiproxy API | `127.0.0.1:8474` |
| Database, user, password | `restaurantos`, `ros`, `ros` |

**Tests.** Unit tests need nothing. Integration tests need one throwaway database and skip without it:

```bash
PGPASSWORD=ros psql -h 127.0.0.1 -p 5510 -U ros -d restaurantos -c "CREATE DATABASE restaurantos_test"
export ROS_TEST_DSN="host=127.0.0.1 port=5510 dbname=restaurantos_test user=ros password=ros"

uv run pytest -m "not integration and not cluster"     # unit
uv run pytest -m "not cluster"                         # unit and integration
uv run pytest -m cluster                               # needs the dev cluster
```

**Services.** Each one is started with `uv run python -m restaurantos.<component>`, and settings say who the process is. The full table is "How each service starts" in architecture Section 4; each brief repeats the line for its own service. `rosctl up` (WP6a) will start all of them at once.

**Data.** The dev cluster starts empty. Until WP6a's bootstrap and mini fixture exist, tests insert the few rows they need, and the briefs show the control rows a service needs to start.

## Your first pull request

For a service: the skeleton, with its routes and their response shapes, and the OpenAPI description exported to `docs/api/`. The UI and the other packages can then build against real shapes while you fill in the logic.

## Words used in the briefs

| Word | Meaning |
| --- | --- |
| region | a group of restaurants, by location, with its own database. `r1`, `r2`, ... |
| HQ | head office: the menu master, the directory of regions, and a read-only copy of all orders |
| primary, standby | the database that takes writes, and its live copy that can take over |
| promotion, failover | making the standby the new primary after the primary is lost |
| epoch | a counter per region that goes up at every promotion |
| epoch fence | every write says which epoch it believes in; a node refuses a write with the wrong one. It stops an old primary from accepting writes |
| proxy | a Toxiproxy entry in front of one database link, named `<caller>__<node>`. Tests break links by changing proxies |
| link group | a named set of proxies that a fault applies to, such as "every link to `r1-primary`" |
| cut, silence, latency, reset | the four faults: link refused, link open but nothing comes back, link slow, connection dropped |
| parked order, holding queue | an order whose region could not be reached; it waits at HQ and is delivered later |
| inbox | a region's record of every order it has seen, so the same order sent twice has one effect |
| outbox, relay | events a region writes in the same transaction as the order, and the process that carries them to HQ |
| HQ worker | the HQ-side process that delivers parked orders and publishes menu versions |
| edge API | the gateway's code running inside one region, so the region works when cut off from HQ |
| deadline, abandoned call | the time a request waits for a database; a call that misses it is left to finish in the background and rolled back |
| playback | replaying the dataset's orders against the system, faster than real time |
| warm-up window | the first 14 days of orders, used only to set starting stock and never replayed |
| oracle | a plain-Python replay with no database, used to check stock and refusals |
| checker | compares orders acknowledged, stored in the regions, and copied to HQ after a run |
| baseline | one database holding what all regions hold, to check and time the cross-region queries |
| T1 to T6 | the scripted failure tests (Phase 2 Table 9) |

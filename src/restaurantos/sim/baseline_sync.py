"""rosctl baseline sync: make the baseline hold what the regions hold.

The baseline node is one PostgreSQL with the region schema, loaded by
bootstrap with the same reference data as the regions. `sync` makes it
hold, on one node, exactly what the regions hold between them
(architecture Section 7.1): it refuses to start unless the system is
quiet, COPYs `orders`, `order_items`, `stock` and `product_availability`
out of every region primary, and, on the baseline, in one transaction,
empties those four tables and COPYs the rows in. A region that cannot
be reached is an error, not a skip: a baseline missing one region's
rows would make every comparison against it fail for the wrong reason.

The one transaction is the whole guarantee: a failure anywhere between
the DELETE and the COMMIT leaves the baseline exactly as it was. The
region connections are opened (and checked) before the baseline is
touched at all, so a down region fails before anything is emptied.

No key remapping is needed (verified against db/region/010_operational.sql
on main): `orders.order_id` is a uuid assigned by the region,
`stock`'s and `product_availability`'s keys start with `vendor_id`, and
every vendor is in exactly one region (bootstrap's invariant), so
region copies cannot collide. Foreign keys force the copy order: all
regions' `orders` before any `order_items`, then `stock`, then
`product_availability`.

Quiet gate: `GET /sim/playback` on the control endpoint
(sim.control_host / sim.control_port — the playback process hosts the
routes until WP6b's control process exists). The status body's `state`
is lowercase (idle, running, paused, stopping, done, stopped, failed).
Quiet = state not running/stopping and `inflight == 0`; a paused run
with requests still in flight is NOT quiet. Connection refused means no
playback process is bound at all, so it is quiet; any other failure on
a bound port (timeout, HTTP error, unparsable or unknown status) is a
possibly-live run and refuses the sync.

Batch job, not a request path: plain sync psycopg connections from the
host (common.cluster.host_conninfo), no Toxiproxy, no statement timeout,
never common.db.make_pool. COPY streams raw chunks from the region's
COPY TO STDOUT straight into the baseline's COPY FROM STDIN: no row is
materialised in Python.
"""

from __future__ import annotations

import errno

import httpx
import psycopg

from restaurantos.common.cluster import BASELINE, Cluster, host_conninfo, primary_of
from restaurantos.sim.config import SimSettings

# The four synced tables. Region and baseline run the same region
# schema (bootstrap creates the baseline with ROS_NODE_KIND=region), so
# plain table COPY matches column-for-column.
SYNC_TABLES = ("orders", "order_items", "stock", "product_availability")

# Children before parents, so the DELETE never rides a cascade.
_DELETE_ORDER = ("order_items", "orders", "stock", "product_availability")

# The playback status states (sim/playback.py). An unknown state is
# treated as not quiet: a future state the gate does not know may mean
# a live run.
_QUIET_OK_STATES = frozenset({"idle", "paused", "done", "stopped", "failed"})


class BaselineSyncError(RuntimeError):
    """A sync failure the user can act on: playback still running, a
    region that cannot be reached, a control endpoint that answers
    garbage. The CLI prints these as a message; genuine bugs keep their
    own exception and their traceback."""


# --- the quiet gate ---------------------------------------------------------


def control_url(settings: SimSettings) -> str:
    return f"http://{settings.control_host}:{settings.control_port}"


def playback_refusal(client: httpx.Client) -> str | None:
    """Why the system is not quiet, or None when it is.

    The client is passed in (not built here) so the tests can wire it
    to a fake control endpoint.
    """
    try:
        response = client.get("/sim/playback")
    except httpx.ConnectError as error:
        if _connection_refused(error):
            # Nothing is bound on the control port: no playback
            # process exists, so nothing is running.
            return None
        return f"cannot reach the playback control endpoint: {error}"
    except httpx.HTTPError as error:
        return f"cannot reach the playback control endpoint: {error}"
    if response.status_code != 200:
        return (
            f"the playback control endpoint answered HTTP {response.status_code}, "
            "not a status body; is the control process healthy?"
        )
    try:
        body = response.json()
    except ValueError as error:
        return f"the playback control endpoint returned a body that is not JSON ({error})"
    if not isinstance(body, dict):
        return "the playback control endpoint returned a status that is not an object"
    state = body.get("state")
    inflight = body.get("inflight")
    run_id = body.get("run_id")
    if not isinstance(state, str) or state not in _QUIET_OK_STATES | {"running", "stopping"}:
        return f"playback status has an unknown state {state!r}"
    if isinstance(inflight, bool) or not isinstance(inflight, int) or inflight < 0:
        return f"playback status has a bad in-flight count {inflight!r}"
    if run_id is not None and not isinstance(run_id, str):
        return f"playback status has a bad run id {run_id!r}"
    if state in ("running", "stopping") or inflight > 0:
        run = run_id if run_id else "<?>"
        return (
            f"playback run {run!r} is {state} with {inflight} request(s) "
            "still in flight; pause or stop it and let them finish before "
            "syncing the baseline"
        )
    return None


def _connection_refused(error: BaseException, depth: int = 8) -> bool:
    """True when the failure is ECONNREFUSED, not a timeout or a reset.

    httpx wraps the socket error, so the errno is on the cause chain.
    """
    seen: BaseException | None = error
    for _ in range(depth):
        if seen is None:
            return False
        if isinstance(seen, OSError) and seen.errno == errno.ECONNREFUSED:
            return True
        seen = seen.__cause__ or seen.__context__
    return False


# --- the sync ----------------------------------------------------------------


def sync(
    cluster: Cluster,
    settings: SimSettings,
    *,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, dict[str, int]]:
    """Copy the regions' four tables into the baseline; return the rows
    copied per region per table: {region_id: {table: count}}.

    Raises BaselineSyncError when the system is not quiet or a region
    cannot be reached; in every failure case the baseline is left
    untouched.
    """
    with httpx.Client(
        base_url=control_url(settings), timeout=settings.http_timeout_s, transport=transport
    ) as client:
        refusal = playback_refusal(client)
    if refusal is not None:
        raise BaselineSyncError(f"refusing to sync the baseline: {refusal}")

    # Every region must be reachable before the baseline is touched.
    regions: dict[str, psycopg.Connection] = {}
    try:
        for rid in cluster.region_ids:
            node = primary_of(rid)
            try:
                regions[rid] = psycopg.connect(host_conninfo(cluster, node), autocommit=True)
            except psycopg.Error as error:
                raise BaselineSyncError(
                    f"cannot reach {node}: {error}. A region that cannot be reached "
                    "is an error, not a skip: the baseline would miss its rows and "
                    "every comparison against it would fail for the wrong reason."
                ) from error

        # One transaction on the baseline: the DELETE and every COPY
        # commit together or not at all.
        with psycopg.connect(host_conninfo(cluster, BASELINE)) as baseline:
            for table in _DELETE_ORDER:
                baseline.execute(f"DELETE FROM {table}")
            counts: dict[str, dict[str, int]] = {rid: {} for rid in cluster.region_ids}
            for table in SYNC_TABLES:
                for rid in cluster.region_ids:
                    counts[rid][table] = _copy_table(regions[rid], baseline, table)
    finally:
        for connection in regions.values():
            connection.close()
    return counts


def _copy_table(source: psycopg.Connection, baseline: psycopg.Connection, table: str) -> int:
    """COPY one table from a region into the baseline, streaming raw
    chunks. Returns the rows copied (the baseline cursor's rowcount
    after COPY FROM; Copy.rows() is a COPY-TO iterator, not a count).

    The region side reads `COPY (SELECT *)`: `orders` and `order_items`
    are partitioned, and PostgreSQL refuses a plain COPY TO on a
    partitioned table. The baseline side writes `COPY table FROM STDIN`
    into the parent, which routes rows to the partitions. Both sides
    are the same region schema, so the SELECT's column order is exactly
    what COPY FROM expects."""
    with source.cursor() as src, baseline.cursor() as dst:
        with (
            src.copy(f"COPY (SELECT * FROM {table}) TO STDOUT") as out,
            dst.copy(f"COPY {table} FROM STDIN") as into,
        ):
            while chunk := out.read():
                into.write(chunk)
        return dst.rowcount


def format_report(counts: dict[str, dict[str, int]]) -> str:
    """One line per region and one total line: rows copied per table."""
    lines: list[str] = []
    total = dict.fromkeys(SYNC_TABLES, 0)
    for rid in sorted(counts):
        rows = counts[rid]
        lines.append(
            f"{rid}: " + ", ".join(f"{table} {rows.get(table, 0)}" for table in SYNC_TABLES)
        )
        for table in SYNC_TABLES:
            total[table] += rows.get(table, 0)
    lines.append("total: " + ", ".join(f"{table} {total[table]}" for table in SYNC_TABLES))
    return "\n".join(lines)

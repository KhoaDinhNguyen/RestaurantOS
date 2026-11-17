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

One snapshot per region: each region connection is one read-only
REPEATABLE READ transaction, opened (and probed with a first
statement, which is what takes the snapshot) during the preflight,
that spans all four COPYs. Autocommit would give every COPY its own
snapshot, and one region's `orders` and `order_items` could come from
different moments.

Quiet gate, twice: `GET /sim/playback` on the control endpoint
(sim.control_host / sim.control_port — the playback process hosts the
routes until WP6b's control process exists). The status body's `state`
is lowercase (idle, running, paused, stopping, done, stopped, failed).
Quiet = state not running/stopping and `inflight == 0`; a paused run
with requests still in flight is NOT quiet. Connection refused means no
playback process is bound at all, so it is quiet; any other failure on
a bound port (timeout, HTTP error, unparsable or unknown status) is a
possibly-live run and refuses the sync. The gate is read a second time
after the last COPY and before the baseline transaction commits, and
the sync aborts (rolling the baseline back) unless the second read is
quiet by the same rules AND unchanged from the first: same
presence/absence of a process, same run_id, and every counter in the
status body (orders today; the events_* and restocks_* fields once the
extras PR lands - both kinds of request write to the regions). This is
detection, not prevention: it catches anything playback sent during the
copy and rolls back, but a `resume` racing the copy still wastes the
work. Residual, accepted: writers other than the configured playback
process are invisible to the gate; a real exclusion (a marker playback
honours) belongs with the WP6b control process.

Batch job, not a request path: plain sync psycopg connections from the
host (common.cluster.host_conninfo), no Toxiproxy, no statement timeout,
never common.db.make_pool. COPY streams raw chunks from the region's
COPY TO STDOUT straight into the baseline's COPY FROM STDIN: no row is
ever materialised in Python.
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


def _control_client(
    settings: SimSettings, transport: httpx.BaseTransport | None = None
) -> httpx.Client:
    """The gate's HTTP client. trust_env=False: a dead HTTP_PROXY/ALL_PROXY
    would fail with ECONNREFUSED, which the gate would misread as "no
    playback process = quiet". (The clients in playback_cli.py have the
    same exposure; they are sibling territory and left alone.)"""
    return httpx.Client(
        base_url=control_url(settings),
        timeout=settings.http_timeout_s,
        transport=transport,
        trust_env=False,
    )


def read_playback_status(client: httpx.Client) -> dict | None:
    """One read of `GET /sim/playback`, validated. Returns the status
    body, or None when the connection was refused (no playback process
    is bound, so nothing is running). Raises BaselineSyncError when the
    endpoint answers anything the gate cannot trust: a live-but-sick
    control endpoint is exactly the case the gate exists for."""
    try:
        response = client.get("/sim/playback")
    except httpx.ConnectError as error:
        if _connection_refused(error):
            return None
        raise BaselineSyncError(f"cannot reach the playback control endpoint: {error}") from error
    except httpx.HTTPError as error:
        raise BaselineSyncError(f"cannot reach the playback control endpoint: {error}") from error
    if response.status_code != 200:
        raise BaselineSyncError(
            f"the playback control endpoint answered HTTP {response.status_code}, "
            "not a status body; is the control process healthy?"
        )
    try:
        body = response.json()
    except ValueError as error:
        raise BaselineSyncError(
            f"the playback control endpoint returned a body that is not JSON ({error})"
        ) from error
    if not isinstance(body, dict):
        raise BaselineSyncError(
            "the playback control endpoint returned a status that is not an object"
        )
    state = body.get("state")
    if not isinstance(state, str) or state not in _QUIET_OK_STATES | {"running", "stopping"}:
        raise BaselineSyncError(f"playback status has an unknown state {state!r}")
    inflight = body.get("inflight")
    if isinstance(inflight, bool) or not isinstance(inflight, int) or inflight < 0:
        raise BaselineSyncError(f"playback status has a bad in-flight count {inflight!r}")
    run_id = body.get("run_id")
    if run_id is not None and not isinstance(run_id, str):
        raise BaselineSyncError(f"playback status has a bad run id {run_id!r}")
    return body


def live_refusal(body: dict) -> str | None:
    """Why a valid status body says the system is not quiet, or None."""
    state, inflight = body["state"], body["inflight"]
    if state in ("running", "stopping") or inflight > 0:
        run = body.get("run_id") or "<?>"
        return (
            f"playback run {run!r} is {state} with {inflight} request(s) "
            "still in flight; pause or stop it and let them finish before "
            "syncing the baseline"
        )
    return None


def playback_refusal(client: httpx.Client) -> str | None:
    """Why the system is not quiet, or None when it is: one read of the
    control endpoint, validated, judged.

    The client is passed in (not built here) so the tests can wire it
    to a fake control endpoint."""
    try:
        body = read_playback_status(client)
    except BaselineSyncError as error:
        return str(error)
    if body is None:
        return None
    return live_refusal(body)


def _counter_names(before: dict, after: dict) -> set[str]:
    """Every key in either body whose value is a counter: an int, not a
    bool (booleans are flags; state and run_id are compared on their
    own). Any counter, not a fixed list: the status body grows the
    events_* and restocks_* counters in the extras PR, and both kinds
    of request write to the regions, so all of them must match. A key
    missing from one body counts as a change."""
    return {
        name
        for body in (before, after)
        for name, value in body.items()
        if isinstance(value, int) and not isinstance(value, bool)
    }


def _shown(value: object) -> str:
    return "missing" if value is None else str(value)


def gate_recheck(before: dict | None, after: dict | None) -> str | None:
    """The second gate read, taken after the last COPY and before the
    baseline transaction commits. None when the system is still quiet
    and nothing changed; otherwise the abort message. Both bodies come
    from read_playback_status (None = no playback process answered).

    Changed means: a playback process appeared or disappeared, the run
    id moved, or ANY counter in the status body advanced (orders today,
    the events_* and restocks_* fields once they land) - a resume that
    only sends status events or a restock still aborts the sync, not
    just one that sends orders.

    Detection, not prevention: an abort here means the copy is thrown
    away (the baseline transaction rolls back), not that it never
    happened."""
    if before is None and after is None:
        return None
    if before is None:
        return (
            "a playback process appeared while the baseline was being "
            "rebuilt; the baseline was left as it was; rerun the sync "
            "once playback is quiet"
        )
    run = before.get("run_id") or "<?>"
    if after is None:
        return (
            f"the playback process (run {run!r}) disappeared while the "
            "baseline was being rebuilt; the baseline was left as it was; "
            "rerun the sync once playback is quiet"
        )
    if before.get("run_id") != after.get("run_id"):
        return (
            f"the playback run changed while the baseline was being rebuilt "
            f"({run!r} -> {after.get('run_id')!r}); the baseline was left "
            "as it was; rerun the sync once playback is quiet"
        )
    changed = [
        name
        for name in sorted(_counter_names(before, after))
        if before.get(name) != after.get(name)
    ]
    if changed:
        what = ", ".join(
            f"{name} {_shown(before.get(name))} -> {_shown(after.get(name))}" for name in changed
        )
        return (
            f"playback run {run!r} advanced while the baseline was being "
            f"rebuilt ({what}); the baseline was left as it was; rerun "
            "the sync once playback is quiet"
        )
    still_running = live_refusal(after)
    if still_running is not None:
        return (
            f"{still_running}; the baseline was left as it was; rerun "
            "the sync once playback is quiet"
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

    Raises BaselineSyncError when the system is not quiet, playback
    changed during the copy, or a database cannot be reached; in every
    failure case the baseline is left untouched (the one transaction
    rolls back). Genuine bugs (non-psycopg exceptions) keep their
    traceback."""
    # First gate read: quiet now, or refuse before anything is touched.
    with _control_client(settings, transport) as client:
        before = read_playback_status(client)
    if before is not None:
        refusal = live_refusal(before)
        if refusal is not None:
            raise BaselineSyncError(f"refusing to sync the baseline: {refusal}")

    # Every region must be reachable, and its snapshot taken, before
    # the baseline is touched. One read-only REPEATABLE READ transaction
    # per region spans all four COPYs: orders and order_items then come
    # from one moment.
    regions: dict[str, psycopg.Connection] = {}
    try:
        for rid in cluster.region_ids:
            node = primary_of(rid)
            try:
                conn = psycopg.connect(host_conninfo(cluster, node))
                conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
                conn.read_only = True
                conn.execute("SELECT 1")  # takes the snapshot, proves the link
                regions[rid] = conn
            except psycopg.Error as error:
                raise BaselineSyncError(
                    f"cannot reach {node}: {error}. A region that cannot be reached "
                    "is an error, not a skip: the baseline would miss its rows and "
                    "every comparison against it would fail for the wrong reason."
                ) from error

        # One transaction on the baseline: the DELETE, every COPY and
        # the second gate read commit together or not at all.
        try:
            with psycopg.connect(host_conninfo(cluster, BASELINE)) as baseline:
                for table in _DELETE_ORDER:
                    baseline.execute(f"DELETE FROM {table}")
                counts: dict[str, dict[str, int]] = {rid: {} for rid in cluster.region_ids}
                for table in SYNC_TABLES:
                    for rid in cluster.region_ids:
                        try:
                            counts[rid][table] = _copy_table(regions[rid], baseline, table)
                        except psycopg.Error as error:
                            raise BaselineSyncError(
                                f"copying {table} from {primary_of(rid)} into the "
                                f"baseline failed: {error}; the baseline was left "
                                "untouched (the transaction rolled back)"
                            ) from error
                # Second gate read, inside the transaction: if playback
                # sent anything during the copy, or the process came or
                # went, the baseline rolls back instead of committing a
                # torn snapshot. A read that fails here aborts too - and
                # the message must say the rollback happened, which is
                # why it is wrapped separately from the read failures of
                # the first gate check.
                try:
                    with _control_client(settings, transport) as client:
                        after = read_playback_status(client)
                except BaselineSyncError as error:
                    raise BaselineSyncError(f"{error}; the baseline was left as it was") from error
                abort = gate_recheck(before, after)
                if abort is not None:
                    raise BaselineSyncError(abort)
        except psycopg.Error as error:
            # Covers everything up to the COMMIT: connect, DELETE, COPY,
            # the wrapped failures above. A failure during the COMMIT
            # itself (the connection lost mid-commit) has an unknown
            # outcome - the message below would then overstate what is
            # known, but that window is a fraction of a second.
            raise BaselineSyncError(
                f"the baseline node failed during the sync: {error}; it was "
                "left untouched (the transaction rolled back)"
            ) from error
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

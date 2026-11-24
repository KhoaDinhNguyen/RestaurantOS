"""The invariant checker (WP6b, architecture Section 7.5).

After a run and after the queues drain, three sets of orders must match:
the orders acknowledged in the run log, the orders stored across the
regions, and the orders in HQ's analytics copy. Also: no
`(channel, external_order_id)` is stored twice (across regions, inside
one region, or at HQ), every refused order is in a region's inbox, and
every parked order is in HQ's holding queue, delivered.

Each difference is classified, never just counted:

- **lost at failover** - acked (or forwarded to HQ) but missing from its
  region, and that region failed over after the order was acked while it
  replicated asynchronously. The expected cost of async replication; it
  never fails the check on its own. With synchronous replication (and no
  downgrade to async before the failover) the same order is unaccounted.
- **stored_unacked** - stored, but every send of it errored, so no client
  was told anything. Fails the check unless `allow_unacked` (fault
  scenarios, where a timeout that still landed is normal).
- **unaccounted** - everything else: a bug. Fails the check.

The evaluation (`evaluate`) is pure and works on plain facts; `collect`
reads them from the databases over plain psycopg connections from the
host (`host_conninfo`, never `make_pool`: its statement timeout would cut
a count over every partition).
"""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from restaurantos.common.cluster import HQ_PRIMARY, HQ_STANDBY, Cluster, host_conninfo

Key = tuple[str, str]  # (channel, external_order_id)

ACCEPTED, PARKED, REFUSED, FAILED = "accepted", "parked", "refused", "failed"
_RANK = {ACCEPTED: 3, PARKED: 2, REFUSED: 1, FAILED: 0}

RESUMED_FROM = "resumed_from.json"
"""Written by a snapshot restore into the run it starts: the run it continues."""


class CheckError(RuntimeError):
    """The check could not be run (no drain, a node unreachable, a bad run)."""


# --- the run logs -----------------------------------------------------------


@dataclass
class OrderOutcome:
    outcome: str
    region_id: str | None = None
    acked_at: datetime | None = None


@dataclass
class RunSet:
    run_ids: list[str]
    from_day: int
    to_day: int
    region: str | None
    orders: dict[Key, OrderOutcome] = field(default_factory=dict)

    def keys(self, outcome: str) -> set[Key]:
        return {key for key, order in self.orders.items() if order.outcome == outcome}


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def classify_line(line: dict[str, Any]) -> str:
    """What one order send came to, as playback counts it: 200/201 with an
    order id accepted, 202 parked at HQ, 409 refused, anything else -
    including every transport error - failed."""
    if "error" in line:
        return FAILED
    status = line.get("http_status")
    if status in (200, 201) and line.get("order_id") is not None:
        return ACCEPTED
    if status == 202:
        return PARKED
    if status == 409:
        return REFUSED
    return FAILED


def _wall_acked_at(line: dict[str, Any], meta: dict[str, Any]) -> datetime | None:
    acked = line.get("wall_acked_at")
    start_epoch = meta.get("wall_start_epoch")
    start_mono = meta.get("wall_start_monotonic")
    if not all(isinstance(v, int | float) for v in (acked, start_epoch, start_mono)):
        return None
    return datetime.fromtimestamp(start_epoch + (acked - start_mono), tz=UTC)


def run_chain(runs_dir: Path, run_ids: Sequence[str]) -> list[str]:
    """The given runs plus every run they continue (a restored run names
    its parent in resumed_from.json), oldest first, each once."""
    ordered: list[str] = []
    for run_id in run_ids:
        chain: list[str] = []
        current: str | None = run_id
        while current is not None and current not in chain and current not in ordered:
            chain.append(current)
            pointer = runs_dir / current / RESUMED_FROM
            current = (
                json.loads(pointer.read_text(encoding="utf-8")).get("parent_run")
                if pointer.is_file()
                else None
            )
        ordered.extend(reversed(chain))
    return ordered


def read_runs(runs_dir: Path, run_ids: Sequence[str], channel: str) -> RunSet:
    """Every order line of the runs (and the runs they continue), one
    entry per order with its best outcome: resends, and the replayed part
    of a restored day, map to the order they repeat."""
    if not run_ids:
        raise CheckError("name at least one run (--run)")
    chain = run_chain(runs_dir, run_ids)
    days: list[tuple[int, int]] = []
    regions: set[str | None] = set()
    orders: dict[Key, OrderOutcome] = {}
    for run_id in chain:
        run_dir = runs_dir / run_id
        try:
            meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
            lines = (run_dir / "orders.jsonl").read_text(encoding="utf-8").splitlines()
        except OSError as error:
            raise CheckError(f"run {run_id}: {error}") from error
        if meta.get("from_day") is None or meta.get("to_day") is None:
            raise CheckError(f"run {run_id}: meta.json has no from_day/to_day")
        days.append((int(meta["from_day"]), int(meta["to_day"])))
        regions.add(meta.get("region"))
        for number, text in enumerate(lines, start=1):
            if not text.strip():
                continue
            try:
                line = json.loads(text)
            except json.JSONDecodeError as error:
                raise CheckError(f"run {run_id}: line {number} is not JSON") from error
            if line.get("kind") != "order":
                continue
            key = (channel, str(line["external_order_id"]))
            outcome = classify_line(line)
            seen = orders.get(key)
            if seen is not None and _RANK[seen.outcome] >= _RANK[outcome]:
                continue
            acked_at = None
            if outcome == ACCEPTED:
                acked_at = _parse_time(line.get("acked_at")) or _wall_acked_at(line, meta)
            orders[key] = OrderOutcome(outcome, line.get("region_id"), acked_at)
    if len(regions) > 1:
        raise CheckError(
            "the runs disagree on the region they replayed: "
            f"{', '.join(sorted(str(r) for r in regions))}"
        )
    return RunSet(
        run_ids=chain,
        from_day=min(day for day, _ in days),
        to_day=max(day for _, day in days),
        region=next(iter(regions)),
        orders=orders,
    )


# --- the facts the databases hold -------------------------------------------


@dataclass
class HoldingRow:
    region_id: str
    delivered_at: datetime | None
    delivered_outcome: str | None


@dataclass
class ClusterFacts:
    # region -> every stored (channel, external_order_id), duplicates kept
    stored: dict[str, list[Key]]
    # every analytics_orders row: (channel, external_order_id, region_id)
    hq: list[tuple[str, str, str]]
    # inbox rows with outcome 'refused', across the regions
    refused: set[Key]
    holding: dict[Key, HoldingRow]
    failovers: list[tuple[str, datetime]] = field(default_factory=list)
    downgrades: list[tuple[str, datetime]] = field(default_factory=list)
    synchronous: bool = False


# --- the evaluation (pure) ---------------------------------------------------


def _lost_at_failover(facts: ClusterFacts, region: str | None, acked_at: datetime | None) -> bool:
    """True if `region` failed over after `acked_at` while replicating
    asynchronously - the one explained way an acked order goes missing."""
    if region is None:
        return False
    for failed_region, at in facts.failovers:
        if failed_region != region or (acked_at is not None and at <= acked_at):
            continue
        downgraded = any(r == region and when <= at for r, when in facts.downgrades)
        if not facts.synchronous or downgraded:
            return True
    return False


def evaluate(runs: RunSet, facts: ClusterFacts, *, allow_unacked: bool = False) -> dict[str, Any]:
    """The verdict, in the Section 6.3 result shape (plus stored_unacked)."""
    unaccounted: list[dict[str, Any]] = []

    def note(key: Key, reason: str, region: str | None = None) -> None:
        unaccounted.append({"external_order_id": key[1], "reason": reason, "region_id": region})

    accepted = runs.keys(ACCEPTED)
    parked = runs.keys(PARKED)
    failed = runs.keys(FAILED)
    expected_refused = set(runs.keys(REFUSED))

    acked = set(accepted)
    region_of: dict[Key, str | None] = {key: runs.orders[key].region_id for key in runs.orders}
    acked_at: dict[Key, datetime | None] = {key: runs.orders[key].acked_at for key in runs.orders}
    for key in sorted(parked):
        row = facts.holding.get(key)
        if row is None:
            note(key, "parked_without_holding_row")
        elif row.delivered_at is None:
            note(key, "parked_not_delivered", row.region_id)
        elif row.delivered_outcome == "accepted":
            acked.add(key)
            region_of[key] = row.region_id
            acked_at[key] = row.delivered_at
        else:
            expected_refused.add(key)

    stored_counts: Counter[Key] = Counter()
    stored_regions: dict[Key, list[str]] = {}
    for region, keys in facts.stored.items():
        for key in keys:
            stored_counts[key] += 1
            stored_regions.setdefault(key, []).append(region)
    stored = set(stored_counts)
    hq_counts: Counter[Key] = Counter((channel, ext) for channel, ext, _ in facts.hq)
    hq_region = {(channel, ext): region for channel, ext, region in facts.hq}
    hq = set(hq_counts)

    duplicates = [
        {"external_order_id": key[1], "where": "regions", "regions": sorted(stored_regions[key])}
        for key, count in sorted(stored_counts.items())
        if count > 1
    ] + [
        {"external_order_id": key[1], "where": "hq", "count": count}
        for key, count in sorted(hq_counts.items())
        if count > 1
    ]

    lost: list[str] = []
    for key in sorted(acked - stored):
        region = region_of.get(key) or hq_region.get(key)
        if _lost_at_failover(facts, region, acked_at.get(key)):
            lost.append(key[1])
        else:
            note(key, "acked_not_stored", region)
    for key in sorted(hq - stored - acked):
        region = hq_region[key]
        if key in failed and _lost_at_failover(facts, region, None):
            lost.append(key[1])
        else:
            note(key, "in_hq_not_in_region", region)
    for key in sorted(stored - hq):
        note(key, "stored_not_in_hq", stored_regions[key][0])

    stored_unacked: list[str] = []
    for key in sorted(stored - acked):
        if key in failed:
            stored_unacked.append(key[1])
        elif key in expected_refused:
            note(key, "refused_but_stored", stored_regions[key][0])
        elif key in runs.orders:
            note(key, "stored_not_acked", stored_regions[key][0])
        else:
            note(key, "stored_not_in_run", stored_regions[key][0])

    for key in sorted(expected_refused - facts.refused):
        note(key, "refused_not_in_inbox", region_of.get(key))

    passed = not duplicates and not unaccounted and (allow_unacked or not stored_unacked)
    return {
        "passed": passed,
        "acked": len(acked),
        "stored": len(stored),
        "hq": len(hq),
        "lost_at_failover": {"count": len(lost), "order_ids": sorted(lost)},
        "duplicates": duplicates,
        "unaccounted": unaccounted,
        "stored_unacked": {
            "count": len(stored_unacked),
            "order_ids": stored_unacked,
            "allowed": allow_unacked,
        },
        "runs": runs.run_ids,
        "days": [runs.from_day, runs.to_day],
    }


# --- reading the databases ---------------------------------------------------


def _connect(cluster: Cluster, node: str):
    import psycopg

    return psycopg.connect(host_conninfo(cluster, node), connect_timeout=5)


def hq_primary(cluster: Cluster) -> str:
    """The HQ node that is not in recovery (HQ may have been promoted)."""
    candidates = [HQ_PRIMARY, HQ_STANDBY] if cluster.standbys else [HQ_PRIMARY]
    primaries: list[str] = []
    errors: list[str] = []
    for node in candidates:
        try:
            with _connect(cluster, node) as conn:
                (in_recovery,) = conn.execute("SELECT pg_is_in_recovery()").fetchone()
        except Exception as error:  # noqa: BLE001 - a down node is a fact here
            errors.append(f"{node}: {error}")
            continue
        if not in_recovery:
            primaries.append(node)
    if len(primaries) != 1:
        found = ", ".join(primaries) or "none"
        raise CheckError(
            f"expected exactly one HQ primary, found {found}"
            + (f" ({'; '.join(errors)})" if errors else "")
        )
    return primaries[0]


def region_primaries(cluster: Cluster, hq: str, scope: str | None) -> dict[str, str]:
    """region -> its current primary, from HQ's region_state (a failover
    moves it; the bootstrap name `<rid>-primary` is wrong after one)."""
    with _connect(cluster, hq) as conn:
        rows = conn.execute("SELECT region_id, primary_node FROM region_state").fetchall()
    primaries = {region: node for region, node in rows}
    if scope is not None:
        if scope not in primaries:
            raise CheckError(f"region {scope} is not in HQ's region_state")
        primaries = {scope: primaries[scope]}
    return primaries


def wait_for_drain(
    cluster: Cluster,
    *,
    timeout_s: float,
    poll_s: float,
    scope: str | None = None,
    sleep=time.sleep,
    clock=time.monotonic,
) -> None:
    """Wait until no outbox row is unrelayed on any region's current
    primary and no holding-queue row is undelivered. A real condition,
    never a fixed sleep; gives up with what is left."""
    deadline = clock() + timeout_s
    while True:
        hq = hq_primary(cluster)
        primaries = region_primaries(cluster, hq, scope)
        left: dict[str, int] = {}
        for region, node in primaries.items():
            with _connect(cluster, node) as conn:
                (count,) = conn.execute(
                    "SELECT count(*) FROM outbox WHERE relayed_at IS NULL"
                ).fetchone()
            if count:
                left[f"outbox on {node} ({region})"] = count
        with _connect(cluster, hq) as conn:
            (held,) = conn.execute(
                "SELECT count(*) FROM holding_queue WHERE delivered_at IS NULL"
            ).fetchone()
        if held:
            left[f"holding_queue on {hq}"] = held
        if not left:
            return
        if clock() >= deadline:
            detail = ", ".join(f"{where}: {count} left" for where, count in left.items())
            raise CheckError(f"the queues did not drain in {timeout_s:g} s - {detail}")
        sleep(poll_s)


def collect(cluster: Cluster, runs: RunSet) -> ClusterFacts:
    hq = hq_primary(cluster)
    primaries = region_primaries(cluster, hq, runs.region)
    window = (runs.from_day, runs.to_day)
    stored: dict[str, list[Key]] = {}
    refused: set[Key] = set()
    for region, node in primaries.items():
        with _connect(cluster, node) as conn:
            stored[region] = [
                (channel, ext)
                for channel, ext in conn.execute(
                    "SELECT channel, external_order_id FROM orders "
                    "WHERE order_day BETWEEN %s AND %s",
                    window,
                )
            ]
            refused.update(
                (channel, ext)
                for channel, ext in conn.execute(
                    "SELECT channel, external_order_id FROM inbox "
                    "WHERE outcome = 'refused' AND order_day BETWEEN %s AND %s",
                    window,
                )
            )
    parked_ids = sorted(ext for _channel, ext in runs.keys(PARKED))
    with _connect(cluster, hq) as conn:
        hq_rows = [
            (channel, ext, region)
            for channel, ext, region in conn.execute(
                "SELECT channel, external_order_id, region_id FROM analytics_orders "
                "WHERE order_day BETWEEN %s AND %s",
                window,
            )
            if runs.region is None or region == runs.region
        ]
        holding = {
            (channel, ext): HoldingRow(region, delivered_at, outcome)
            for channel, ext, region, delivered_at, outcome in conn.execute(
                "SELECT channel, external_order_id, region_id, delivered_at, "
                "delivered_outcome FROM holding_queue WHERE external_order_id = ANY(%s)",
                (parked_ids,),
            )
        }
        failovers: list[tuple[str, datetime]] = []
        downgrades: list[tuple[str, datetime]] = []
        for kind, region, at in conn.execute(
            "SELECT kind, region_id, occurred_at FROM cluster_events "
            "WHERE kind IN ('failover', 'sync_downgrade')"
        ):
            (failovers if kind == "failover" else downgrades).append((region, at))
    return ClusterFacts(
        stored=stored,
        hq=hq_rows,
        refused=refused,
        holding=holding,
        failovers=failovers,
        downgrades=downgrades,
        synchronous=cluster.synchronous,
    )


def run_check(
    cluster: Cluster,
    run_ids: Sequence[str],
    *,
    runs_dir: Path,
    channel: str,
    allow_unacked: bool = False,
    drain_timeout_s: float = 300.0,
    poll_s: float = 1.0,
) -> dict[str, Any]:
    """Drain, read, evaluate. Blocking: run it in a thread from async code."""
    runs = read_runs(runs_dir, run_ids, channel)
    wait_for_drain(cluster, timeout_s=drain_timeout_s, poll_s=poll_s, scope=runs.region)
    return evaluate(runs, collect(cluster, runs), allow_unacked=allow_unacked)


# --- the report --------------------------------------------------------------


def _listing(items: Iterable[str], limit: int = 20) -> list[str]:
    items = list(items)
    shown = [f"  - {item}" for item in items[:limit]]
    if len(items) > limit:
        shown.append(f"  ... and {len(items) - limit} more (--json lists every one)")
    return shown


def format_report(result: dict[str, Any]) -> str:
    verdict = "PASSED" if result["passed"] else "FAILED"
    days = result.get("days") or ["?", "?"]
    lines = [
        f"check: {verdict} (runs {', '.join(result.get('runs', []))}; days {days[0]}-{days[1]})",
        f"acked {result['acked']}, stored {result['stored']}, hq {result['hq']}",
        f"lost at failover: {result['lost_at_failover']['count']}",
    ]
    lines += _listing(result["lost_at_failover"]["order_ids"])
    lines.append(f"duplicates: {len(result['duplicates'])}")
    lines += _listing(
        f"{entry['external_order_id']} ({entry['where']}: "
        f"{entry.get('regions') or entry.get('count')})"
        for entry in result["duplicates"]
    )
    lines.append(f"unaccounted: {len(result['unaccounted'])}")
    lines += _listing(
        f"{entry['external_order_id']} {entry['reason']}"
        + (f" ({entry['region_id']})" if entry.get("region_id") else "")
        for entry in result["unaccounted"]
    )
    unacked = result["stored_unacked"]
    rule = "allowed" if unacked["allowed"] else "fails the check; --allow-unacked for fault runs"
    lines.append(f"stored but never answered: {unacked['count']} ({rule})")
    lines += _listing(unacked["order_ids"])
    return "\n".join(lines)

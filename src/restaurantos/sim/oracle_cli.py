"""The runtime behind the `rosctl oracle` subcommands.

`run` replays a window with the counting oracle and writes its report
under `runs/<oracle-id>/`: `report.jsonl` (one line per order, in replay
order), `stock.csv` (the final counters) and `summary.json` (the totals
and the facts the comparison checks against). `compare` reads a
playback run's log and its region's database and checks the oracle's
report against both - the replay itself stays pure and never opens a
connection, so the comparison is honest about what needs a live region.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import zip_longest
from pathlib import Path
from typing import Any

import psycopg

from restaurantos.common.cluster import Cluster, host_conninfo, load_cluster, primary_of
from restaurantos.common.config import get_settings
from restaurantos.sim.config import get_sim_settings
from restaurantos.sim.oracle import (
    OUTCOME_ACCEPTED,
    OUTCOME_REFUSED,
    REASON_OUT_OF_STOCK,
    CountingOracle,
    OracleRules,
    StockKey,
    write_report,
)
from restaurantos.sim.playback import restock_days
from restaurantos.sim.playback_cli import restock_vendors
from restaurantos.sim.regions import region_order_counts, vendor_region_map
from restaurantos.sim.replaydata import cache_paths, ensure_cache, iter_orders, require_window
from restaurantos.sim.shard import counts_by_cell
from restaurantos.sim.stock import warm_up_stock

# One path component, no traversal: the same rule the playback run id
# follows, kept here so the oracle does not reach into playback's
# private module.
_RUN_ID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

# How many mismatch lines to print (and keep) before the rest are only
# counted: enough to diagnose, bounded so a wrong window on 1.7M orders
# cannot flood the terminal.
MAX_MISMATCH_LINES = 10

# The reason pair the run log can carry where the oracle says
# out_of_stock: the region refused at step 3 because the product had
# been switched off by mark_out_of_stock, which the oracle does not
# model on purpose (it changes availability, not stock, and an order
# for the product would be refused on stock anyway).
EXPECTED_REASON_DIVERGENCE = ("unavailable", REASON_OUT_OF_STOCK)


@dataclass(frozen=True)
class RegionState:
    """What the region's primary holds after the run, read once."""

    orders: int
    """Rows in `orders`: the accepted orders, stored."""
    refused: int
    """Rows in `inbox` refused: the refused orders, recorded."""
    stock: dict[StockKey, tuple[int, int]]
    """(vendor_id, item_kind, item_id) -> (on_hand, par_level)."""


def read_region_state(cluster: Cluster, region_id: str) -> RegionState:
    """Orders, refusals and every stock counter of the region's primary.

    A batch read straight from the host, like `rosctl baseline sync`:
    plain psycopg, `host_conninfo`, no pool, no Toxiproxy.
    """
    with psycopg.connect(host_conninfo(cluster, primary_of(region_id))) as conn:
        (orders,) = conn.execute("SELECT count(*) FROM orders").fetchone()  # type: ignore[assignment]
        (refused,) = conn.execute("SELECT count(*) FROM inbox WHERE outcome = 'refused'").fetchone()  # type: ignore[assignment]
        stock = {
            (vendor_id, item_kind, item_id): (on_hand, par_level)
            for vendor_id, item_kind, item_id, on_hand, par_level in conn.execute(
                "SELECT vendor_id, item_kind, item_id, on_hand, par_level FROM stock"
            )
        }
    return RegionState(orders=orders, refused=refused, stock=stock)


# --- run --------------------------------------------------------------------


def run_command(args: argparse.Namespace) -> int:
    common = get_settings()
    sim = get_sim_settings()
    data_dir = common.data_dir

    region = args.region
    cluster: Cluster | None = None
    cluster_file: Path | None = None
    # The region's share of the whole cache, the number playback's
    # region mode records as `region_cached_orders` in meta.json: compare
    # checks the two, so a report scoped by a different cluster file than
    # the run's is caught as a fact, not as 1.7M per-order mismatches.
    region_cached_orders: int | None = None
    if region is not None:
        cluster_file = Path(args.config or common.cluster_file)
        try:
            cluster = load_cluster(cluster_file)
        except (OSError, ValueError) as error:
            print(f"rosctl oracle run: {error}", file=sys.stderr)
            return 1
        if region not in cluster.region_ids:
            print(
                f"rosctl oracle run: no region {region!r} in "
                f"{args.config or common.cluster_file}; this cluster has "
                f"{', '.join(cluster.region_ids)}",
                file=sys.stderr,
            )
            return 1

    meta = ensure_cache(data_dir)
    from_day = (
        args.from_day if args.from_day is not None else max(meta.warm_up_days, meta.first_day)
    )
    try:
        window = require_window(meta, from_day, args.to_day)
    except ValueError as error:
        print(f"rosctl oracle run: {error}", file=sys.stderr)
        return 1
    if window.shortened_from is not None:
        print(
            f"rosctl oracle run: data only goes to day {meta.last_day}; "
            f"running to day {meta.last_day} instead of {window.shortened_from}",
            file=sys.stderr,
        )

    restock_every = args.restock_days if args.restock_days is not None else sim.stock_restock_days
    try:
        run_restock_days = restock_days(
            meta.warm_up_days, restock_every, window.from_day, window.to_day
        )
        rules = OracleRules.from_data_dir(data_dir)
        # The starting stock, recomputed the way bootstrap computed it:
        # same files, same W, C and F, deterministic, no database needed.
        stock_rows = warm_up_stock(
            data_dir,
            warm_days=sim.stock_warm_up_days,
            cover_days=sim.stock_cover_days,
            floor=sim.stock_floor,
        )
        pairs = restock_vendors(data_dir)
        scope: set[str] = {vendor for vendor, _geohash in pairs}
        where: Any = None
        if region is not None:
            assert cluster is not None
            vendor_region = vendor_region_map(pairs, counts_by_cell(pairs), cluster)
            share = region_order_counts(
                cache_paths(data_dir)[0],
                vendor_region,
                region,
                from_day=window.from_day,
                to_day=window.to_day,
            )
            region_cached_orders = share.region_orders
            if share.unknown_orders:
                print(
                    f"rosctl oracle run: {share.unknown_orders} cached orders have a "
                    f"vendor outside the vendor-to-region map (example: "
                    f"{share.unknown_example!r}); vendors.csv and the order files disagree",
                    file=sys.stderr,
                )
                return 1
            print(
                f"region {region}: {share.region_orders} of {meta.orders} cached orders "
                f"({share.window_orders} in days {window.from_day}-{window.to_day})",
                file=sys.stderr,
            )
            scope = {vendor for vendor, home in vendor_region.items() if home == region}

            def in_region(order: dict[str, Any]) -> bool:
                return vendor_region.get(order["vendor_id"]) == region

            where = in_region
            stock_rows = [row for row in stock_rows if row[0] in scope]
    except (OSError, RuntimeError, ValueError) as error:
        print(f"rosctl oracle run: {error}", file=sys.stderr)
        return 1

    oracle_id = args.run_id or f"oracle-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    if not oracle_id or any(char not in _RUN_ID_CHARS for char in oracle_id):
        print(
            f"rosctl oracle run: report id {oracle_id!r} may only hold letters, "
            "digits, dots, dashes and underscores",
            file=sys.stderr,
        )
        return 1
    run_dir = Path("runs") / oracle_id
    try:
        run_dir.mkdir(parents=True)
    except FileExistsError:
        print(
            f"rosctl oracle run: report {oracle_id!r} already exists; choose another id",
            file=sys.stderr,
        )
        return 1

    oracle = CountingOracle(rules, stock_rows, restock_days=run_restock_days, vendors=scope)
    orders = iter_orders(
        cache_paths(data_dir)[0],
        from_day=window.from_day,
        to_day=window.to_day,
        where=where,
    )
    with (run_dir / "report.jsonl").open("w", encoding="utf-8") as f:
        for record in oracle.replay(orders):
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
    write_report(run_dir, oracle)

    summary = {
        "oracle_id": oracle_id,
        "region": region,
        "region_cached_orders": region_cached_orders,
        "cluster": {"config": str(cluster_file)} if cluster_file is not None else None,
        "from_day": window.from_day,
        "to_day": window.to_day,
        "restock_days": run_restock_days,
        "restock_every": restock_every,
        "warm_up_days": sim.stock_warm_up_days,
        "cover_days": sim.stock_cover_days,
        "floor": sim.stock_floor,
        "cache": {
            "orders_sha256": meta.orders_sha256,
            "order_items_sha256": meta.order_items_sha256,
            "warm_up_days": meta.warm_up_days,
            "cached_orders": meta.orders,
        },
        "orders": oracle.orders,
        "accepted": oracle.accepted,
        "refused": oracle.refused,
        "refusals_by_reason": dict(oracle.refusals),
        "stock_rows": len(oracle.on_hand),
        "counters_ever_zero": len(oracle.counters_ever_zero),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    region_note = f", region {region}" if region is not None else ""
    print(
        f"oracle {oracle_id}: {oracle.orders} orders, days {window.from_day} to "
        f"{window.to_day}{region_note}: {oracle.accepted} accepted, {oracle.refused} refused, "
        f"{len(run_restock_days)} restock day(s) x {len(scope)} vendors; "
        f"{len(oracle.on_hand)} counters, {len(oracle.counters_ever_zero)} ever reached zero; "
        f"report: runs/{oracle_id}/"
    )
    return 0


# --- compare ----------------------------------------------------------------


@dataclass
class OrderComparison:
    """Per-order outcomes, log against oracle report."""

    log_orders: int = 0
    report_orders: int = 0
    both_accepted: int = 0
    both_refused: int = 0
    reason_divergences: int = 0
    """The expected pair: the log's `unavailable` against the oracle's
    `out_of_stock` - documented, not a mismatch."""
    mismatch_count: int = 0
    mismatch_lines: list[str] = field(default_factory=list)

    def add_mismatch(self, line: str) -> None:
        self.mismatch_count += 1
        if len(self.mismatch_lines) < MAX_MISMATCH_LINES:
            self.mismatch_lines.append(line)


def _order_lines(lines: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Only the order lines; the log also carries restocks and status events."""
    return (line for line in lines if line.get("kind") == "order")


def compare_order_lines(
    log_lines: Iterable[dict[str, Any]], report_lines: Iterable[dict[str, Any]]
) -> OrderComparison:
    """Walk both files' order lines in lockstep and compare the outcomes.

    Both sides carry one line per order, in send order - the log's
    `kind` field also carries restocks and status events, which are
    skipped. The log line must hold a decided order outcome: a parked,
    failed or cancelled send is a mismatch, because the oracle predicts
    accepted and refused, not what a broken link does.

    Both inputs are consumed one line at a time and never held in
    memory: a full-window run is 1.7M lines a side (the brief's "stream
    the files, do not build Python objects for all of them up front").
    """
    comparison = OrderComparison()
    pairs = zip_longest(_order_lines(log_lines), _order_lines(report_lines))
    for position, (log, report) in enumerate(pairs):
        if log is not None:
            comparison.log_orders += 1
        if report is not None:
            comparison.report_orders += 1
        if log is None:
            # zip_longest pads only the shorter side, so the report's
            # line is there by construction
            assert report is not None
            comparison.add_mismatch(
                f"position {position}: the log has no order; the oracle reports "
                f"{report['external_order_id']} {report['outcome']}"
            )
            continue
        if report is None:
            comparison.add_mismatch(
                f"position {position}: log order {log['external_order_id']} has no oracle line"
            )
            continue
        where = f"position {position} ({log.get('external_order_id')})"
        if "error" in log:
            comparison.add_mismatch(f"{where}: the run's send failed ({log['error']})")
            continue
        log_outcome = log.get("outcome")
        if log_outcome not in (OUTCOME_ACCEPTED, OUTCOME_REFUSED):
            comparison.add_mismatch(
                f"{where}: the run's answer is {log_outcome!r} (http "
                f"{log.get('http_status')}); the oracle predicts accepted or refused only"
            )
            continue
        report_id = str(report.get("external_order_id"))
        if report_id != str(log.get("external_order_id")):
            comparison.add_mismatch(f"{where}: ids disagree (oracle says {report_id})")
            continue
        report_outcome = report.get("outcome")
        if log_outcome != report_outcome:
            comparison.add_mismatch(
                f"{where}: run says {log_outcome}, oracle says {report_outcome}"
            )
            continue
        if log_outcome == OUTCOME_ACCEPTED:
            comparison.both_accepted += 1
        else:
            comparison.both_refused += 1
            log_reason = log.get("refusal_reason")
            report_reason = report.get("refusal_reason")
            if log_reason == report_reason:
                continue
            if (log_reason, report_reason) == EXPECTED_REASON_DIVERGENCE:
                comparison.reason_divergences += 1
                continue
            comparison.add_mismatch(
                f"{where}: both refused, but the run says {log_reason} "
                f"and the oracle says {report_reason}"
            )
    return comparison


def _jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def _read_stock_csv(path: Path) -> dict[StockKey, tuple[int, int]]:
    """The oracle's final counters, keyed the way the region's rows are."""
    rows: dict[StockKey, tuple[int, int]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # the header
        for number, row in enumerate(reader, start=2):
            if len(row) != 5:
                raise ValueError(f"stock.csv line {number}: expected 5 values, got {len(row)}")
            key = (row[0], row[1], row[2])
            rows[key] = (int(row[3]), int(row[4]))
    return rows


def _newest_report_id(runs_dir: Path) -> str | None:
    """The newest complete `oracle-*` report, or None.

    Age is the write time of `summary.json`, the file `oracle run`
    writes last, not the name: a `--run-id oracle-...` the user chose
    does not sort by age. The name only breaks a tie.
    """
    candidates = [
        entry
        for entry in runs_dir.glob("oracle-*")
        if entry.is_dir() and (entry / "summary.json").is_file()
    ]
    if not candidates:
        return None
    newest = max(
        candidates, key=lambda entry: ((entry / "summary.json").stat().st_mtime_ns, entry.name)
    )
    return newest.name


def compare_command(args: argparse.Namespace) -> int:
    runs_dir = Path("runs")
    run_dir = runs_dir / args.run_id
    if not (run_dir / "meta.json").is_file():
        print(f"rosctl oracle compare: no run {args.run_id!r} under runs/", file=sys.stderr)
        return 1
    run_meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    region = run_meta.get("region")
    if region is None:
        print(
            f"rosctl oracle compare: run {args.run_id!r} was not a single-region run; "
            "the comparison needs the run's region",
            file=sys.stderr,
        )
        return 1
    if not run_meta.get("sequential"):
        print(
            f"rosctl oracle compare: run {args.run_id!r} was not sequential; concurrent "
            "sends land in a different order than the stream, and no oracle can match that",
            file=sys.stderr,
        )
        return 1
    report_id = args.report_id or _newest_report_id(runs_dir)
    if report_id is None:
        print(
            "rosctl oracle compare: no oracle report under runs/ (run "
            "`rosctl oracle run` first, or pass --report-id)",
            file=sys.stderr,
        )
        return 1
    report_dir = runs_dir / report_id
    try:
        summary = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        print(
            f"rosctl oracle compare: report {report_id!r} is incomplete: {error}", file=sys.stderr
        )
        return 1

    # The report must describe the same window, region, cache and
    # restock days as the run, or the comparison would be against the
    # wrong replay.
    facts_wrong: list[str] = []
    if summary.get("region") != region:
        facts_wrong.append(
            f"the run's region is {region}, the report says {summary.get('region')!r}"
        )
    if (summary.get("from_day"), summary.get("to_day")) != (
        run_meta.get("from_day"),
        run_meta.get("to_day"),
    ):
        facts_wrong.append(
            f"the run covered days {run_meta.get('from_day')}-{run_meta.get('to_day')}, "
            f"the report says {summary.get('from_day')}-{summary.get('to_day')}"
        )
    # The cache is bound to both source files (replaydata.meta_is_current):
    # orders.csv and order_items.csv. A run logged before playback recorded
    # the second digest gets a warning for it, not a failure.
    run_cache = run_meta.get("cache") or {}
    report_cache = summary.get("cache") or {}
    if run_cache.get("orders_sha256") != report_cache.get("orders_sha256"):
        facts_wrong.append("the run and the report replayed different order caches")
    elif report_cache.get("order_items_sha256") is None:
        facts_wrong.append(
            f"report {report_id!r} does not record order_items.csv's digest "
            f"(written before compare checked it); rerun `rosctl oracle run`"
        )
    elif run_cache.get("order_items_sha256") is None:
        print(
            f"rosctl oracle compare: warning: run {args.run_id!r} does not record "
            "order_items.csv's digest (logged before playback recorded it); only "
            "orders.csv is checked",
            file=sys.stderr,
        )
    elif run_cache["order_items_sha256"] != report_cache["order_items_sha256"]:
        facts_wrong.append(
            "the run and the report replayed different order caches (order_items.csv differs)"
        )
    if (run_meta.get("restock") or {}).get("days") != summary.get("restock_days"):
        facts_wrong.append(
            "the run restocked on "
            f"{(run_meta.get('restock') or {}).get('days')}, the report says "
            f"{summary.get('restock_days')}"
        )
    run_share = run_meta.get("region_cached_orders")
    report_share = summary.get("region_cached_orders")
    if report_share is None:
        facts_wrong.append(
            f"report {report_id!r} does not record its region's cached-order count "
            f"(written before compare checked it); rerun `rosctl oracle run --region {region}`"
        )
    elif run_share != report_share:
        run_cluster = (run_meta.get("cluster") or {}).get("config")
        report_cluster = (summary.get("cluster") or {}).get("config")
        facts_wrong.append(
            f"the run's region {region} holds {run_share} cached orders, the report's "
            f"holds {report_share}: the two were scoped with different cluster files "
            f"(run: {run_cluster}, report: {report_cluster}); rerun `rosctl oracle run "
            f"--region {region} --config {run_cluster}`"
        )
    if facts_wrong:
        for line in facts_wrong:
            print(f"rosctl oracle compare: {line}", file=sys.stderr)
        return 1

    comparison = compare_order_lines(
        _jsonl(run_dir / "orders.jsonl"), _jsonl(report_dir / "report.jsonl")
    )
    stock_rows = _read_stock_csv(report_dir / "stock.csv")

    cluster_file = (run_meta.get("cluster") or {}).get("config")
    if not cluster_file:
        print(
            f"rosctl oracle compare: run {args.run_id!r} does not name its cluster file; "
            "cannot find the region's primary",
            file=sys.stderr,
        )
        return 1
    try:
        cluster = load_cluster(Path(cluster_file))
    except (OSError, ValueError) as error:
        print(f"rosctl oracle compare: {error}", file=sys.stderr)
        return 1
    state = read_region_state(cluster, region)

    # Final stock, counter for counter.
    stock_mismatch_lines: list[str] = []
    stock_mismatch_count = 0
    if len(state.stock) != len(stock_rows):
        stock_mismatch_count += 1
        stock_mismatch_lines.append(
            f"the region holds {len(state.stock)} counters, the oracle's report has "
            f"{len(stock_rows)}"
        )
    for key in sorted(stock_rows):
        on_hand, par = stock_rows[key]
        region_row = state.stock.get(key)
        if region_row is None:
            stock_mismatch_count += 1
            if len(stock_mismatch_lines) < MAX_MISMATCH_LINES:
                stock_mismatch_lines.append(f"{key}: the region has no such counter")
        elif region_row != (on_hand, par):
            stock_mismatch_count += 1
            if len(stock_mismatch_lines) < MAX_MISMATCH_LINES:
                stock_mismatch_lines.append(
                    f"{key}: oracle {on_hand} (par {par}), "
                    f"region {region_row[0]} (par {region_row[1]})"
                )
    extra = sorted(set(state.stock) - set(stock_rows))
    if extra:
        stock_mismatch_count += len(extra)
        for key in extra[: MAX_MISMATCH_LINES - len(stock_mismatch_lines)]:
            stock_mismatch_lines.append(f"{key}: the region has a counter the oracle does not")

    counts_wrong: list[str] = []
    if state.orders != summary.get("accepted"):
        accepted = summary.get("accepted")
        counts_wrong.append(
            f"the region stored {state.orders} orders, the oracle accepted {accepted}"
        )
    if state.refused != summary.get("refused"):
        refused = summary.get("refused")
        counts_wrong.append(
            f"the region recorded {state.refused} refusals, the oracle refused {refused}"
        )

    print(
        f"orders: {comparison.log_orders} in the run log, {comparison.report_orders} in the "
        f"report; {comparison.both_accepted} accepted and {comparison.both_refused} refused "
        "on both sides"
    )
    print(f"stock: {len(state.stock)} counters on the region, {len(stock_rows)} in the report")
    print(f"region {region}: {state.orders} orders stored, {state.refused} refusals recorded")
    if comparison.reason_divergences:
        print(
            f"reason divergences (expected, {EXPECTED_REASON_DIVERGENCE[0]} vs "
            f"{EXPECTED_REASON_DIVERGENCE[1]}): {comparison.reason_divergences}"
        )
    # The report's own lines must add up to its summary, the numbers the
    # region counts above are checked against. Only checked when every
    # line agreed: a per-line mismatch already explains any difference.
    report_wrong: list[str] = []
    report_totals = (summary.get("accepted"), summary.get("refused"))
    if (
        not comparison.mismatch_count
        and (
            comparison.both_accepted,
            comparison.both_refused,
        )
        != report_totals
    ):
        report_wrong.append(
            f"report {report_id!r}: its lines hold {comparison.both_accepted} accepted and "
            f"{comparison.both_refused} refused, its summary says {report_totals[0]} and "
            f"{report_totals[1]}; the report is damaged, rerun `rosctl oracle run`"
        )
    failed = bool(
        comparison.mismatch_count
        or stock_mismatch_count
        or counts_wrong
        or report_wrong
        or comparison.log_orders != comparison.report_orders
    )
    for line in comparison.mismatch_lines:
        print(f"mismatch: {line}", file=sys.stderr)
    if comparison.mismatch_count > len(comparison.mismatch_lines):
        print(
            f"mismatch: ... and {comparison.mismatch_count - len(comparison.mismatch_lines)} "
            "more per-order mismatches",
            file=sys.stderr,
        )
    for line in stock_mismatch_lines:
        print(f"stock mismatch: {line}", file=sys.stderr)
    if stock_mismatch_count > len(stock_mismatch_lines):
        print(
            f"stock mismatch: ... and {stock_mismatch_count - len(stock_mismatch_lines)} more",
            file=sys.stderr,
        )
    for line in counts_wrong:
        print(f"count mismatch: {line}", file=sys.stderr)
    for line in report_wrong:
        print(f"report mismatch: {line}", file=sys.stderr)
    if counts_wrong:
        # The counts are every row on the region, not this run's rows: a
        # region that took any other order since bootstrap cannot match.
        print(
            "count mismatch: compare counts every order and refusal on the region; "
            "it needs a freshly bootstrapped region that took only this run",
            file=sys.stderr,
        )
    verdict = "MISMATCH" if failed else "MATCH"
    print(f"oracle {report_id} vs run {args.run_id}: {verdict}")
    return 1 if failed else 0


def run(args: argparse.Namespace) -> int:
    if args.oracle_command == "run":
        return run_command(args)
    if args.oracle_command == "compare":
        return compare_command(args)
    raise AssertionError(f"unhandled oracle command {args.oracle_command!r}")  # pragma: no cover

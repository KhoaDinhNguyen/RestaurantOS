"""The rosctl command line (WP6a). pyproject.toml points the `rosctl`
script at app().

`bootstrap`, `playback` and `baseline sync` exist; the other
subcommands are the rest of WP6a and are registered so that
`rosctl --help` shows the full shape of the tool while it is being
built.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

NOT_BUILT = {
    "up": "start the services (WP6a, later)",
    "down": "stop the services (WP6a, later)",
    "control": "the control API for the fault tests (WP6b)",
}


def build_parser() -> argparse.ArgumentParser:
    """The `rosctl` parser, built separately from `app` so tests drive
    the same parser the CLI uses (public surface, not internals)."""
    parser = argparse.ArgumentParser(
        prog="rosctl",
        description="Build, run and break the RestaurantOS cluster (WP6a).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    boot = sub.add_parser(
        "bootstrap",
        help="generate infra/generated/, rebuild the cluster from empty volumes and load the data",
    )
    boot.add_argument(
        "--yes",
        action="store_true",
        help="delete existing volumes without asking; without a terminal "
        "this is the only way forward",
    )
    boot.add_argument(
        "--config",
        type=Path,
        default=Path("infra/cluster.toml"),
        help="cluster description to build from (default: infra/cluster.toml)",
    )
    # --- playback ------------------------------------------------------
    play = sub.add_parser(
        "playback",
        help="replay the dataset's orders against the gateway (or the stub)",
    )
    psub = play.add_subparsers(dest="play_command", required=True)
    pstart = psub.add_parser(
        "start",
        help="run a replay in the foreground and serve its /sim control routes",
    )
    pstart.add_argument(
        "--config",
        type=Path,
        default=None,
        help="cluster description to route kitchen calls with - use the "
        "file bootstrap ran with (default: ROS_CLUSTER_FILE setting, "
        "infra/cluster.toml)",
    )
    pstart.add_argument(
        "--speed",
        type=float,
        default=None,
        help="simulated seconds per wall-clock second (default ROS_SIM_PLAYBACK_SPEED)",
    )
    pstart.add_argument(
        "--from-day",
        type=int,
        default=None,
        help="first replay day (default: the first day after the warm-up window)",
    )
    pstart.add_argument(
        "--to-day",
        type=int,
        default=None,
        help="last replay day (default: the last day in the cache)",
    )
    pstart.add_argument("--seed", type=int, default=0, help="run seed (default 0)")
    pstart.add_argument("--run-id", type=str, default=None, help="runs/<run_id>/ to write")
    pstart.add_argument(
        "--region",
        type=str,
        default=None,
        help="only send this region's orders (the vendor-to-region split bootstrap uses)",
    )
    pstart.add_argument(
        "--target",
        type=str,
        default=None,
        help="API base to POST orders to, or 'edge' for the chosen region's own "
        "edge API (needs --region) (default ROS_SIM_PLAYBACK_TARGET_URL)",
    )
    pstart.add_argument(
        "--sequential",
        action="store_true",
        help="one order at a time: wait for each response before the next (inflight 1)",
    )
    pstart.add_argument(
        "--kitchen-target",
        type=str,
        default=None,
        help="send every status event and restock to this one API base "
        "instead of each region's own edge - required for stub runs "
        "(pass the stub's URL); default: the per-region edge APIs",
    )
    pstart.add_argument(
        "--inflight",
        type=int,
        default=None,
        help="requests kept in flight at once (default ROS_SIM_PLAYBACK_INFLIGHT)",
    )
    pstart.add_argument(
        "--events",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="send each accepted order its four status events "
        "(default ROS_SIM_PLAYBACK_STATUS_EVENTS, on)",
    )
    pstart.add_argument(
        "--event-sample-rate",
        type=float,
        default=None,
        help="share of orders that get their full event set, 0 to 1 "
        "(default ROS_SIM_PLAYBACK_EVENT_SAMPLE_RATE)",
    )
    pstart.add_argument(
        "--restock-days",
        type=int,
        default=None,
        help="restock every R days at 00:00:00 of days W+R, W+2R, ... "
        "(default ROS_SIM_STOCK_RESTOCK_DAYS)",
    )
    pstart.add_argument(
        "--port",
        type=int,
        default=None,
        help="control-route port (default ROS_SIM_CONTROL_PORT)",
    )
    for name, helptext in (
        ("pause", "pause the live run"),
        ("resume", "resume the paused run"),
        ("stop", "stop the run, wait for in-flight requests"),
    ):
        stopper = psub.add_parser(name, help=helptext)
        stopper.add_argument("--port", type=int, default=None, help="control-route port")
    pspeed = psub.add_parser("speed", help="change speed while the run is live")
    pspeed.add_argument("value", type=float, help="new speed (simulated seconds per wall second)")
    pspeed.add_argument("--port", type=int, default=None, help="control-route port")
    pstatus = psub.add_parser("status", help="progress of the live run")
    pstatus.add_argument("--port", type=int, default=None, help="control-route port")
    pcache = psub.add_parser(
        "build-cache",
        help="build the sorted order cache now instead of at the first run",
    )
    pcache.add_argument(
        "--warm-up-days",
        type=int,
        default=None,
        help="warm-up window W (default ROS_SIM_STOCK_WARM_UP_DAYS)",
    )
    # --- oracle ---------------------------------------------------------
    oracle = sub.add_parser(
        "oracle",
        help="the counting oracle: replay the stock rules with no database",
    )
    osub = oracle.add_subparsers(dest="oracle_command", required=True)
    orun = osub.add_parser(
        "run",
        help="replay a window with the same rules place_order applies",
    )
    orun.add_argument(
        "--config",
        type=Path,
        default=None,
        help="cluster description to resolve the region with - use the "
        "file bootstrap ran with (default ROS_CLUSTER_FILE setting, "
        "infra/cluster.toml); needed with --region",
    )
    orun.add_argument(
        "--region",
        type=str,
        default=None,
        help="only replay this region's orders, the way a sequential run does",
    )
    orun.add_argument(
        "--from-day",
        type=int,
        default=None,
        help="first replay day (default: the first day after the warm-up window)",
    )
    orun.add_argument(
        "--to-day",
        type=int,
        default=None,
        help="last replay day (default: the last day in the cache)",
    )
    orun.add_argument(
        "--restock-days",
        type=int,
        default=None,
        help="restock every R days, as the run being checked did "
        "(default ROS_SIM_STOCK_RESTOCK_DAYS)",
    )
    orun.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="runs/<run_id>/ to write report.jsonl, stock.csv and summary.json into "
        "(default: oracle-<timestamp>)",
    )
    ocmp = osub.add_parser(
        "compare",
        help="check an oracle report against a sequential run's log and its region "
        "(bootstrap fresh before that run: compare counts every row on the region)",
    )
    ocmp.add_argument(
        "--run-id",
        type=str,
        required=True,
        help="the playback run to compare against (runs/<run_id>/)",
    )
    ocmp.add_argument(
        "--report-id",
        type=str,
        default=None,
        help="the oracle report to check (default: the newest runs/oracle-*)",
    )
    # --- baseline -------------------------------------------------------
    base = sub.add_parser(
        "baseline",
        help="the single-node comparison database",
    )
    bsub = base.add_subparsers(dest="baseline_command", required=True)
    bsync = bsub.add_parser(
        "sync",
        help="copy the regions' orders, order_items, stock and "
        "product_availability into the baseline node",
    )
    bsync.add_argument(
        "--config",
        type=Path,
        default=Path("infra/cluster.toml"),
        help="cluster description to copy from (default: infra/cluster.toml)",
    )

    # D10 is implemented by WP5; rosctl only registers the command.
    hq = sub.add_parser("hq", help="HQ database operator commands (WP5)")
    hsub = hq.add_subparsers(dest="hq_command", required=True)
    for operation, helptext in (
        ("promote", "fence the old HQ primary and promote its standby"),
        ("rejoin", "rewind or rebuild the fenced old HQ primary as a standby"),
    ):
        hp = hsub.add_parser(operation, help=helptext)
        hp.add_argument(
            "--compose-file", type=Path, required=True, help="the running cluster's Compose file"
        )
        hp.add_argument(
            "--config",
            type=Path,
            default=None,
            help="cluster description (default ROS_CLUSTER_FILE)",
        )
        hp.add_argument(
            "--yes",
            action="store_true",
            help="authorize this HQ operation and possible async data loss",
        )

    for name, helptext in NOT_BUILT.items():
        sub.add_parser(name, help=helptext)

    return parser


def app(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "hq":
        from restaurantos.coordinator.hq_promotion import run as run_hq_promotion

        return run_hq_promotion(args)
    if args.command == "bootstrap":
        from restaurantos.sim.bootstrap import BootstrapError, bootstrap
        from restaurantos.sim.datacheck import DataCheckError

        try:
            return asyncio.run(bootstrap(yes=args.yes, config=args.config))
        except (DataCheckError, BootstrapError) as error:
            # a failed data check, teardown or input validation: the user
            # should see the message, not a traceback. Real bugs keep
            # their own exception and their traceback.
            print(f"rosctl bootstrap: {error}", file=sys.stderr)
            return 1
        except FileNotFoundError as error:
            hint = " (is the docker command installed?)" if "docker" in str(error) else ""
            print(f"rosctl bootstrap: {error}{hint}", file=sys.stderr)
            return 1
    if args.command == "playback":
        from restaurantos.sim.playback_cli import run as run_playback

        try:
            return run_playback(args)
        except ValueError as error:
            # a bad window or a bad cache: the user should see the
            # message, not a traceback.
            print(f"rosctl playback {args.play_command}: {error}", file=sys.stderr)
            return 1
    if args.command == "oracle":
        from restaurantos.sim.oracle_cli import run as run_oracle

        try:
            return run_oracle(args)
        except ValueError as error:
            # a bad window, a bad cache or a broken report: the message,
            # not a traceback. Genuine bugs keep their exception.
            print(f"rosctl oracle {args.oracle_command}: {error}", file=sys.stderr)
            return 1
    if args.command == "baseline" and args.baseline_command == "sync":
        from restaurantos.common.cluster import load_cluster
        from restaurantos.sim.baseline_sync import BaselineSyncError, format_report, sync
        from restaurantos.sim.config import get_sim_settings

        try:
            counts, warning = sync(load_cluster(args.config), get_sim_settings())
        except BaselineSyncError as error:
            # playback still live, a region down, a sick control
            # endpoint: the message says what to do about it.
            print(f"rosctl baseline sync: {error}", file=sys.stderr)
            return 1
        if warning is not None:
            print(warning, file=sys.stderr)
        print(format_report(counts))
        return 0
    print(f"rosctl {args.command}: not built yet ({NOT_BUILT[args.command]})", file=sys.stderr)
    return 2

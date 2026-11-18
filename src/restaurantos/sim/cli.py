"""The rosctl command line (WP6a). pyproject.toml points the `rosctl`
script at app().

`bootstrap` and `playback` exist; the other subcommands are the rest of
WP6a and are registered so that `rosctl --help` shows the full shape of
the tool while it is being built.
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
    "baseline": "sync the baseline node (WP6a, later)",
    "control": "the control API for the fault tests (WP6b)",
}


def app(argv: Sequence[str] | None = None) -> int:
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
        "--target",
        type=str,
        default=None,
        help="API base to POST orders to (default ROS_SIM_PLAYBACK_TARGET_URL)",
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

    for name, helptext in NOT_BUILT.items():
        sub.add_parser(name, help=helptext)

    args = parser.parse_args(argv)
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
    print(f"rosctl {args.command}: not built yet ({NOT_BUILT[args.command]})", file=sys.stderr)
    return 2

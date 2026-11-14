"""The rosctl command line (WP6a). pyproject.toml points the `rosctl`
script at app().

Only `bootstrap` exists so far; the other subcommands are the rest of
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
    "playback": "replay orders (WP6a, later)",
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
    for name, helptext in NOT_BUILT.items():
        sub.add_parser(name, help=helptext)

    args = parser.parse_args(argv)
    if args.command == "bootstrap":
        from restaurantos.sim.bootstrap import bootstrap
        from restaurantos.sim.datacheck import DataCheckError

        try:
            return asyncio.run(bootstrap(yes=args.yes, config=args.config))
        except (DataCheckError, RuntimeError, ValueError) as error:
            # a failed data check, teardown or input validation: the user
            # should see the message, not a traceback
            print(f"rosctl bootstrap: {error}", file=sys.stderr)
            return 1
    print(f"rosctl {args.command}: not built yet ({NOT_BUILT[args.command]})", file=sys.stderr)
    return 2

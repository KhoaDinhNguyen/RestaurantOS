"""`rosctl check` and `rosctl snapshot save|restore|list`: the same code the
control process's job routes run, called directly and waited for."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from restaurantos.common.config import get_settings
from restaurantos.sim.checker import CheckError, format_report, run_check
from restaurantos.sim.config import get_sim_settings
from restaurantos.sim.snapshot import SnapshotError, default_context, list_snapshots, restore, save

RUNS_DIR = Path("runs")


def _cluster_file(args: argparse.Namespace) -> Path:
    return Path(args.config or get_settings().cluster_file)


def check_command(args: argparse.Namespace) -> int:
    from restaurantos.common.cluster import load_cluster
    from restaurantos.sim.replaydata import CHANNEL

    sim = get_sim_settings()
    try:
        result = run_check(
            load_cluster(_cluster_file(args)),
            args.runs,
            runs_dir=RUNS_DIR,
            channel=CHANNEL,
            allow_unacked=args.allow_unacked,
            drain_timeout_s=sim.check_drain_timeout_s,
            poll_s=sim.check_poll_s,
        )
    except (CheckError, OSError, ValueError) as error:
        print(f"rosctl check: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 - psycopg errors: a node down, a bad query
        print(f"rosctl check: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(format_report(result))
    return 0 if result["passed"] else 1


def snapshot_command(args: argparse.Namespace) -> int:
    sim = get_sim_settings()
    command = args.snapshot_command
    if command == "list":
        print(json.dumps(list_snapshots(sim.snapshot_dir), indent=2, sort_keys=True))
        return 0
    ctx = default_context(_cluster_file(args), sim, port=args.port)
    try:
        if command == "save":
            result = asyncio.run(save(args.name, ctx))
        else:
            result = asyncio.run(
                restore(args.name, ctx, replace_cluster_file=args.replace_cluster_file)
            )
    except (SnapshotError, OSError, ValueError) as error:
        print(f"rosctl snapshot {command}: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 - docker, Toxiproxy or HTTP failures
        print(f"rosctl snapshot {command}: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    warnings = list(result.get("warnings") or [])
    resume = result.get("resume") or {}
    warnings += resume.get("warnings") or []
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)
    return 0

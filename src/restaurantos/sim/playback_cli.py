"""The runtime behind the `rosctl playback` subcommands.

`start` runs one whole run in the foreground: it makes sure the sorted
order cache exists, opens the run, and serves the /sim control routes
until the run is over. The other subcommands (pause, resume, stop,
speed, status) are thin HTTP clients for those routes, so the terminal,
a test script and the WP7 GUI all steer a run through the same
contract.

Ctrl-C is a stop: the engine finishes what is in flight, closes the
run log, and the process exits 0.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx

from restaurantos.common.config import get_settings
from restaurantos.common.models import OrderRequest
from restaurantos.sim.config import SimSettings, get_sim_settings
from restaurantos.sim.playback import Playback, StateError
from restaurantos.sim.replaydata import (
    CacheMeta,
    ensure_cache,
    iter_orders,
    require_window,
    to_order_request,
)


def _orders_fn(cache_file: Path):
    def make(from_day: int, to_day: int | None) -> Iterator[OrderRequest]:
        return (
            to_order_request(order)
            for order in iter_orders(cache_file, from_day=from_day, to_day=to_day)
        )

    return make


async def start_command(args: argparse.Namespace) -> int:
    common = get_settings()
    sim = get_sim_settings()
    data_dir = common.data_dir
    meta: CacheMeta = ensure_cache(data_dir)
    from_day = (
        args.from_day if args.from_day is not None else max(meta.warm_up_days, meta.first_day)
    )
    to_day = args.to_day
    try:
        require_window(meta, from_day, to_day)
    except ValueError as error:
        print(f"rosctl playback start: {error}", file=sys.stderr)
        return 1

    speed = args.speed or sim.playback_speed
    inflight = args.inflight or sim.playback_inflight
    target = args.target or sim.playback_target_url
    playback = Playback(
        _orders_fn(data_dir / "cached" / "orders-sorted.jsonl"),
        target_url=target,
        inflight=inflight,
        http_timeout_s=sim.http_timeout_s,
        drain_timeout_s=sim.playback_drain_timeout_s,
        runs_dir=Path("runs"),
        default_speed=sim.playback_speed,
        # the window is refused, never clamped, for API starts as well
        window_check=lambda f, t: require_window(meta, f, t),
    )

    from restaurantos.sim.control import make_control_app

    app = make_control_app(playback)
    import uvicorn

    config = uvicorn.Config(
        app, host=sim.control_host, port=args.port or sim.control_port, log_level="warning"
    )
    server = uvicorn.Server(config)
    serve = asyncio.create_task(server.serve())
    # Give the server a moment to bind, so a taken port fails here and
    # not deep inside a run.
    while not server.started:
        if serve.done():
            break
        await asyncio.sleep(0.01)
    if not server.started:
        server.should_exit = True
        await serve
        print(
            "rosctl playback start: cannot listen on "
            f"{sim.control_host}:{args.port or sim.control_port}",
            file=sys.stderr,
        )
        return 1

    try:
        await playback.start(
            seed=args.seed,
            from_day=from_day,
            to_day=to_day,
            speed=speed,
            run_id=args.run_id,
            meta_extra={
                "cache": {
                    "orders_sha256": meta.orders_sha256,
                    "warm_up_days": meta.warm_up_days,
                    "cached_orders": meta.orders,
                },
                "argv": sys.argv[1:],
            },
        )
        print(
            f"run {playback.status()['run_id']} started: {from_day} -> "
            f"{to_day if to_day is not None else meta.last_day}, speed {speed}x, "
            f"{inflight} in flight, control on {sim.control_host}:"
            f"{args.port or sim.control_port}",
            file=sys.stderr,
        )

        async def _ctrl_c_stop() -> None:
            # Ctrl-C is a stop; a second one while already stopping is
            # harmless.
            with contextlib.suppress(StateError):
                await playback.stop()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            # Windows has no loop.add_signal_handler; SIGINT still works
            # there through KeyboardInterrupt.
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, lambda: asyncio.ensure_future(_ctrl_c_stop()))
        await playback.wait_done()
    finally:
        if playback.state in ("running", "paused", "stopping"):
            await playback.stop()
        server.should_exit = True
        await serve
    status = playback.status()
    print(
        "run {run_id} {state}: {orders_sent} sent, {orders_acked} acked, "
        "{orders_parked} parked, {orders_refused} refused, {orders_failed} failed; "
        "log: runs/{run_id}/orders.jsonl".format(**status)
    )
    return 0 if status["state"] in ("done", "stopped") else 1


def _control_url(args: argparse.Namespace, sim: SimSettings) -> str:
    host = getattr(args, "host", None) or sim.control_host
    port = getattr(args, "port", None) or sim.control_port
    return f"http://{host}:{port}"


def _control_timeout_s(sim: SimSettings, *, stop: bool = False) -> float:
    """A stop request outlives the server's drain, which can wait the
    full drain timeout before answering; everything else uses the plain
    HTTP timeout."""
    if stop:
        return sim.playback_drain_timeout_s + sim.http_timeout_s + 5.0
    return sim.http_timeout_s


async def control_command(args: argparse.Namespace, *, method: str, body: dict[str, Any]) -> int:
    sim = get_sim_settings()
    url = _control_url(args, sim)
    command = getattr(args, "play_command", None)
    stop = method == "POST" and body.get("action") == "stop"
    try:
        async with httpx.AsyncClient(timeout=_control_timeout_s(sim, stop=stop)) as client:
            if method == "POST":
                response = await client.post(f"{url}/sim/playback", json=body)
            else:
                response = await client.patch(f"{url}/sim/playback", json=body)
    except httpx.ConnectError:
        print(
            f"rosctl playback {command}: no playback control server at {url} - "
            "is a run live (`rosctl playback start`)?",
            file=sys.stderr,
        )
        return 1
    except httpx.HTTPError as error:
        print(f"rosctl playback {command}: {error}", file=sys.stderr)
        return 1
    if response.status_code == 409:
        print(f"rosctl playback {command}: {response.json()['detail']}", file=sys.stderr)
        return 1
    if response.status_code >= 400:
        print(f"rosctl playback {command}: {response.text}", file=sys.stderr)
        return 1
    print(json.dumps(response.json(), indent=2, sort_keys=True))
    return 0


async def status_command(args: argparse.Namespace) -> int:
    sim = get_sim_settings()
    url = _control_url(args, sim)
    try:
        async with httpx.AsyncClient(
            timeout=_control_timeout_s(sim)  # a slow answer is a message, not a traceback
        ) as client:
            response = await client.get(f"{url}/sim/playback")
            clock = await client.get(f"{url}/sim/clock")
    except httpx.ConnectError:
        print(
            f"rosctl playback status: no playback control server at {url} - "
            "is a run live (`rosctl playback start`)?",
            file=sys.stderr,
        )
        return 1
    except httpx.HTTPError as error:
        print(f"rosctl playback status: {error}", file=sys.stderr)
        return 1
    body = {"clock": clock.json(), "playback": response.json()}
    print(json.dumps(body, indent=2, sort_keys=True))
    return 0


async def build_cache_command(args: argparse.Namespace) -> int:
    common = get_settings()
    try:
        ensure_cache(common.data_dir, warm_up_days=args.warm_up_days)
    except Exception as error:  # noqa: BLE001 - the CLI reports, not a traceback
        print(f"rosctl playback build-cache: {error}", file=sys.stderr)
        return 1
    return 0


def run(args: argparse.Namespace) -> int:
    command = args.play_command
    if command == "start":
        return asyncio.run(start_command(args))
    if command == "pause":
        return asyncio.run(control_command(args, method="POST", body={"action": "pause"}))
    if command == "resume":
        return asyncio.run(control_command(args, method="POST", body={"action": "resume"}))
    if command == "stop":
        return asyncio.run(control_command(args, method="POST", body={"action": "stop"}))
    if command == "speed":
        if args.value <= 0:
            print("rosctl playback speed: speed must be positive", file=sys.stderr)
            return 1
        return asyncio.run(control_command(args, method="PATCH", body={"speed": args.value}))
    if command == "status":
        return asyncio.run(status_command(args))
    if command == "build-cache":
        return asyncio.run(build_cache_command(args))
    raise AssertionError(f"unhandled playback command {command!r}")  # pragma: no cover

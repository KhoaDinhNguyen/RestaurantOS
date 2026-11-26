"""The runtime behind the `rosctl playback` subcommands.

`start` runs one whole run in the foreground: it makes sure the sorted
order cache exists, opens the run, and serves the /sim control routes
until the run is over. The other subcommands (pause, resume, stop,
speed, status) are thin HTTP clients for those routes, so the terminal,
a test script and the WP7 GUI all steer a run through the same
contract.

`rosctl control` (WP6b) is the same engine set-up kept alive with no run
of its own: it serves the playback routes AND the control process's
links, faults, nodes and jobs routes on port 8002, and every run on it
is started through `POST /sim/playback` (`rosctl playback start
--via-control`). One process owns the port either way; a standalone
`playback start` finds it taken and says to use `--via-control`.

Ctrl-C is a stop: the engine finishes what is in flight, closes the
run log, and the process exits 0.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import math
import signal
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from restaurantos.common.cluster import Cluster, load_cluster, service_port
from restaurantos.common.config import get_settings
from restaurantos.common.models import OrderRequest
from restaurantos.sim.config import SimSettings, get_sim_settings
from restaurantos.sim.jobs import JobRunner
from restaurantos.sim.playback import Playback, StateError, restock_days
from restaurantos.sim.regions import (
    edge_target_url,
    region_order_counts,
    vendor_region_map,
)
from restaurantos.sim.replaydata import (
    CacheMeta,
    ensure_cache,
    iter_orders,
    manifest_rows,
    require_window,
    to_order_request,
)
from restaurantos.sim.shard import counts_by_cell, vendor_pairs
from restaurantos.sim.toxiproxy import Toxiproxy


def _orders_fn(cache_file: Path, *, where: Callable[[dict], bool] | None = None):
    def make(from_day: int, to_day: int | None) -> Iterator[OrderRequest]:
        return (
            to_order_request(order)
            for order in iter_orders(cache_file, from_day=from_day, to_day=to_day, where=where)
        )

    return make


def restock_vendors(data_dir: Path) -> list[tuple[str, str]]:
    """(vendor_id, geohash) for every vendor, sorted: restock order must
    not depend on the CSV's row order, and the geohash routes each
    restock to its vendor's region.

    One read, checked (review round 3, item 11): the file's sha256
    against its MANIFEST.csv entry and the header, then the rows
    parsed from those same bytes - the cell counts and the region map
    both build from what this returns, so nothing is derived from a
    second, unchecked read. The row checks (value count first, then
    the empty vendor_id, each with its own message and the line
    number) are `shard.vendor_pairs`, shared with bootstrap's
    `vendor_counts` (review round 4, item 7), so the two paths give
    the same errors."""
    pairs = vendor_pairs(manifest_rows(data_dir, "vendors.csv"))
    return sorted(pairs)


def kitchen_urls(cluster: Cluster, edge_host: str) -> dict[str, str]:
    """region -> its own edge API base: the edge of `region` listens on
    8010 + n (`common.cluster.service_port`), and the kitchen is on the
    region side (architecture 7.2)."""
    return {
        rid: f"http://{edge_host}:{service_port(cluster, f'edge-{rid}')}/api/v1"
        for rid in cluster.region_ids
    }


async def start_command(
    args: argparse.Namespace,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    control: bool = False,
) -> int:
    # The run's diagnostics (the shortening warning, a failed run) go
    # through the restaurantos.sim.playback logger; give it a plain
    # stderr handler here so the operator sees the same text as before
    # (review round 3, item 9). The `rosctl ...` lines this command
    # prints itself stay print: they are the command's own output, not
    # diagnostics of the running process.
    _stderr_handler = logging.StreamHandler(sys.stderr)
    _stderr_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    _logger = logging.getLogger("restaurantos.sim.playback")
    _logger.addHandler(_stderr_handler)
    # removed on every exit after the install, the early returns
    # included (review round 4, item 5): two starts in one process
    # must never stack handlers
    try:
        return await _run_start(args, transport, control=control)
    finally:
        _logger.removeHandler(_stderr_handler)


async def _run_start(
    args: argparse.Namespace,
    transport: httpx.AsyncBaseTransport | None,
    *,
    control: bool = False,
) -> int:
    # The CLI override skips SimSettings validation, so it is checked
    # here, before anything is built: a NaN or a share outside [0, 1]
    # would otherwise silently drop or keep every event (review item 6).
    rate_arg = args.event_sample_rate
    if rate_arg is not None and not (math.isfinite(rate_arg) and 0.0 <= rate_arg <= 1.0):
        print(
            f"rosctl playback start: --event-sample-rate {rate_arg} must be a number in [0, 1]",
            file=sys.stderr,
        )
        return 1
    # The duplicates/shuffle flags are checked before anything is built
    # (the same discipline as the sample rate): a NaN or a share outside
    # [0, 1] would silently drop or keep every resend. The settings-side
    # values are pydantic-validated; only the CLI overrides need this.
    duplicate_rate_arg = args.duplicate_rate
    if duplicate_rate_arg is not None and not (
        math.isfinite(duplicate_rate_arg) and 0.0 <= duplicate_rate_arg <= 1.0
    ):
        print(
            f"rosctl playback start: --duplicate-rate {duplicate_rate_arg} "
            "must be a number in [0, 1]",
            file=sys.stderr,
        )
        return 1
    duplicate_offset_arg = args.duplicate_offset_s
    if duplicate_offset_arg is not None and not (
        math.isfinite(duplicate_offset_arg) and duplicate_offset_arg >= 0.0
    ):
        print(
            f"rosctl playback start: --duplicate-offset-s {duplicate_offset_arg} "
            "must be a number >= 0",
            file=sys.stderr,
        )
        return 1
    shuffle_window_arg = args.shuffle_window
    if shuffle_window_arg is not None and shuffle_window_arg < 1:
        print(
            f"rosctl playback start: --shuffle-window {shuffle_window_arg} "
            "must be at least 1 (1 means no reorder)",
            file=sys.stderr,
        )
        return 1
    common = get_settings()

    # --- single-region mode: --region, --target edge, --sequential -----
    # All of it before the cache is built (review round 4, item 2): a
    # bad flag combination or cluster file must not pay for a cache
    # first, and must not crash - `load_cluster` raises on a missing
    # file or bad TOML, so it is handled here like every other start
    # error, with one line and rc 1.
    region = args.region
    sequential = args.sequential
    cluster: Cluster | None = None
    if args.target == "edge" and region is None:
        print(
            "rosctl playback start: --target edge needs --region: "
            "which region's edge API should the orders go to?",
            file=sys.stderr,
        )
        return 1
    if region is not None:
        try:
            cluster = load_cluster(args.config or common.cluster_file)
        except (OSError, ValueError) as error:
            print(f"rosctl playback start: {error}", file=sys.stderr)
            return 1
        if region not in cluster.region_ids:
            print(
                f"rosctl playback start: no region {region!r} in "
                f"{args.config or common.cluster_file}; this cluster has "
                f"{', '.join(cluster.region_ids)}",
                file=sys.stderr,
            )
            return 1
    if sequential and args.inflight not in (None, 1):
        print(
            "rosctl playback start: --sequential sends one order at a time; "
            "drop --inflight, or pass 1",
            file=sys.stderr,
        )
        return 1

    sim = get_sim_settings()
    # resolve the duplicates/shuffle knobs (flag over setting) and refuse
    # the one combination that quietly breaks a mode: shuffle reorders
    # delivery, sequential mode's whole point is in-order one-at-a-time
    # delivery the oracle can match. The engine refuses it too; the CLI
    # says it here, before the cache is built or anything starts.
    duplicate_rate = (
        args.duplicate_rate if args.duplicate_rate is not None else sim.playback_duplicate_rate
    )
    duplicate_offset_s = (
        args.duplicate_offset_s
        if args.duplicate_offset_s is not None
        else sim.playback_duplicate_offset_s
    )
    shuffle_on = args.shuffle if args.shuffle is not None else sim.playback_shuffle
    shuffle_window = args.shuffle_window if args.shuffle_window is not None else sim.shuffle_window
    if shuffle_on and sequential:
        print(
            "rosctl playback start: --shuffle cannot run with --sequential: "
            "sequential mode delivers one order at a time in stream order so "
            "the oracle can match it; run shuffle without --sequential "
            "(duplicates are allowed in any mode)",
            file=sys.stderr,
        )
        return 1
    data_dir = common.data_dir
    meta: CacheMeta = ensure_cache(data_dir)
    from_day = (
        args.from_day if args.from_day is not None else max(meta.warm_up_days, meta.first_day)
    )
    to_day = args.to_day
    try:
        window = require_window(meta, from_day, to_day)
    except ValueError as error:
        print(f"rosctl playback start: {error}", file=sys.stderr)
        return 1
    if window.shortened_from is not None:
        # before the banner: the run that follows is not the one asked for
        print(
            f"rosctl playback start: data only goes to day {meta.last_day}; "
            f"running to day {meta.last_day} instead of {window.shortened_from}",
            file=sys.stderr,
        )

    speed = args.speed or sim.playback_speed
    if args.target == "edge":
        assert cluster is not None
        target = edge_target_url(cluster, region)
    else:
        target = args.target or sim.playback_target_url
    inflight = 1 if sequential else (args.inflight or sim.playback_inflight)
    events_on = sim.playback_status_events if args.events is None else args.events
    event_rate = (
        args.event_sample_rate
        if args.event_sample_rate is not None
        else sim.playback_event_sample_rate
    )
    restock_every = args.restock_days if args.restock_days is not None else sim.stock_restock_days
    try:
        # the banner's days come from the effective window, so a
        # shortened to_day never announces restocks outside the data
        banner_days = restock_days(meta.warm_up_days, restock_every, window.from_day, window.to_day)
        if getattr(args, "skip_first_restock", False):
            # the engine drops the from_day restock the same way
            banner_days = [day for day in banner_days if day != window.from_day]
        pairs = restock_vendors(data_dir)
        vendors = [vendor for vendor, _geohash in pairs]
        # the region map shards these same rows, not a second read
        cell_counts = counts_by_cell(pairs)
        # kitchen routing: resolve the override first - a stub run
        # sends every kitchen call to the one URL and never reads a
        # cluster file, so it does not need one. Only a run routing
        # per region loads the cluster (the file bootstrap ran with,
        # so the region count and the edge ports match the shards) and
        # builds the vendor -> region map the way bootstrap shards it.
        kitchen_override = (args.kitchen_target or "").rstrip("/") or None
        vendor_regions: dict[str, str] | None = None
        region_map: dict[str, str] | None = None
        edges = None
        cluster_file: Path | None = None
        if kitchen_override is not None:
            vendor_regions = {}
        if kitchen_override is None or region is not None:
            # region mode needs the vendor -> region map for the order
            # filter even when the kitchen calls go to an override URL
            cluster_file = args.config or common.cluster_file
            routing_cluster = cluster if cluster is not None else load_cluster(cluster_file)
            region_map = vendor_region_map(pairs, cell_counts, routing_cluster)
            if kitchen_override is None:
                vendor_regions = region_map
                edges = kitchen_urls(routing_cluster, sim.playback_edge_host)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"rosctl playback start: {error}", file=sys.stderr)
        return 1

    # --- the region's share, validated BEFORE anything starts ---------
    # Runs before the Playback engine and the control server exist, so
    # no control request can arrive mid-validation (round 3's race).
    # Inline on purpose: no server is bound yet, so blocking the loop
    # costs nothing, and a worker thread is worse for Ctrl-C.
    cache_file = data_dir / "cached" / "orders-sorted.jsonl"
    where: Callable[[dict], bool] | None = None
    region_cached_orders: int | None = None
    region_window_orders: int | None = None
    if region is not None:
        assert region_map is not None
        vendor_region = region_map

        def region_where(order: dict) -> bool:
            return vendor_region.get(order["vendor_id"]) == region

        where = region_where
        # One full pass over the cache before anything is sent: the
        # region's share of the whole cache, next to the total, is what
        # makes a wrong split visible before the run's outcome is.
        share = region_order_counts(
            cache_file,
            vendor_region,
            region,
            from_day=from_day,
            to_day=window.to_day,
        )
        if share.unknown_orders:
            print(
                f"rosctl playback start: {share.unknown_orders} cached orders have a "
                f"vendor outside the vendor-to-region map (example: "
                f"{share.unknown_example!r}); vendors.csv and the order files disagree",
                file=sys.stderr,
            )
            return 1
        region_cached_orders = share.region_orders
        region_window_orders = share.window_orders
        print(
            f"region {region}: {region_cached_orders} of {meta.orders} cached orders "
            f"({region_window_orders} in days {from_day}-{window.to_day})",
            file=sys.stderr,
        )
    # --- the engine: every run on it shares the mode's facts -----------
    # `run_meta` carries what holds for every run on this engine,
    # including one started through the control API mid-process: the
    # cache, the cluster file, and - in region mode - the region, its
    # cached share and the sequential flag. What describes only the
    # first CLI run (argv, its window count) stays in `meta_extra`.
    run_meta: dict[str, Any] = {
        "cache": {
            "orders_sha256": meta.orders_sha256,
            "order_items_sha256": meta.order_items_sha256,
            "warm_up_days": meta.warm_up_days,
            "cached_orders": meta.orders,
        },
    }
    if cluster_file is not None:
        # routing per region: which cluster file decided the edges
        run_meta["cluster"] = {"config": str(cluster_file)}
    if region is not None:
        run_meta["region"] = region
        run_meta["region_cached_orders"] = region_cached_orders
    if sequential:
        run_meta["sequential"] = True

    playback = Playback(
        _orders_fn(cache_file, where=where),
        target_url=target,
        inflight=inflight,
        http_timeout_s=sim.http_timeout_s,
        drain_timeout_s=sim.playback_drain_timeout_s,
        runs_dir=Path("runs"),
        default_speed=sim.playback_speed,
        transport=transport,
        # the window is shortened, never refused, for API starts as
        # well (a to_day past the data runs to the last cached day,
        # and the engine records both days in meta.json)
        window_check=lambda f, t: require_window(meta, f, t),
        status_events=events_on,
        event_sample_rate=event_rate,
        event_jitter_s=sim.playback_event_jitter_s,
        # kitchen-call retries: a timeout, connection error or 5xx is
        # retried with the same event id (review round 3, item 1)
        kitchen_attempts=sim.playback_kitchen_attempts,
        # the engine computes the restock days per run from its own
        # window; the banner below prints the first run's count
        restock_every=restock_every,
        warm_up_days=meta.warm_up_days,
        restock_vendors=vendors,
        # kitchen-side routing (review round 2, item 6): events and
        # restocks go to the region's own edge API - the override URL is
        # how a stub run keeps them on the one stub process
        kitchen_url=kitchen_override,
        kitchen_urls=None if kitchen_override is not None else edges,
        vendor_regions=vendor_regions,
        run_meta=run_meta,
        sequential=sequential,
        # duplicates/shuffle engine facts: the resend offset bound and
        # the shuffle window hold for every run on this engine; the rate
        # and the switch themselves are per-run start parameters, like
        # the seed
        duplicate_offset_s=duplicate_offset_s,
        shuffle_window=shuffle_window,
    )

    from restaurantos.sim.control import make_control_app

    app = make_control_app(playback)
    extras: _ControlExtras | None = None
    if control:
        try:
            extras = _add_control_extras(
                app,
                args.config or common.cluster_file,
                port=args.port,
                scenario_engine={
                    "playback": playback,
                    "api_base": target,
                    "default_from_day": from_day,
                    "last_day": meta.last_day,
                    "default_speed": sim.playback_speed,
                    "http_timeout_s": sim.http_timeout_s,
                    "port": args.port or sim.control_port,
                },
            )
        except (OSError, ValueError) as error:
            print(f"rosctl control: {error}", file=sys.stderr)
            return 1
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
        if extras is not None:
            await extras.aclose()
            print(
                f"rosctl control: cannot listen on {sim.control_host}:"
                f"{args.port or sim.control_port} - is another control or playback "
                "process running?",
                file=sys.stderr,
            )
            return 1
        print(
            "rosctl playback start: cannot listen on "
            f"{sim.control_host}:{args.port or sim.control_port} - if `rosctl control` "
            "owns the port, start the run through it: rosctl playback start "
            "--via-control --from-day N",
            file=sys.stderr,
        )
        return 1
    if extras is not None:
        return await _serve_control(playback, server, serve, extras, sim, args)

    try:
        # The original to_day goes to the engine: it records it as
        # to_day_requested in meta.json and its window check does the
        # same shortening this command already reported. `cache`,
        # `cluster`, `region` and `sequential` travel through the
        # engine's `run_meta` (see above); this dict carries what
        # describes the first CLI run only.
        meta_extra: dict[str, Any] = {"argv": sys.argv[1:]}
        if region is not None:
            # the first run's window count: a control-API start has its
            # own window, so it cannot be an engine-wide fact
            meta_extra["region_window_orders"] = region_window_orders
        try:
            await playback.start(
                seed=args.seed,
                from_day=from_day,
                to_day=to_day,
                speed=speed,
                run_id=args.run_id,
                duplicate_rate=duplicate_rate,
                shuffle=shuffle_on,
                meta_extra=meta_extra,
                skip_first_restock=getattr(args, "skip_first_restock", False),
            )
        except StateError:
            # A run was started through the control API in the window
            # between the bind and our own start: keep that run and
            # bow out cleanly; the finally below stops it and closes
            # its log.
            print(
                "rosctl playback start: a run was started through the "
                "control API before this one; not starting a second",
                file=sys.stderr,
            )
            return 1
        events_note = "on" if events_on else "off"
        if events_on and event_rate < 1.0:
            events_note += f" (sample rate {event_rate})"
        region_note = f", region {region}" if region is not None else ""
        duplicates_note = (
            f", duplicates {duplicate_rate} (offset <= {duplicate_offset_s} s)"
            if duplicate_rate > 0
            else ""
        )
        shuffle_note = f", shuffle (window {shuffle_window})" if shuffle_on else ""
        print(
            f"run {playback.status()['run_id']} started: {from_day} -> "
            f"{window.to_day}{region_note}, speed {speed}x, "
            f"{inflight} in flight, events {events_note}{duplicates_note}{shuffle_note}, "
            f"restock on {len(banner_days)} day(s) x {len(vendors)} vendors, control on "
            f"{sim.control_host}:{args.port or sim.control_port}",
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
        "{orders_parked} parked, {orders_refused} refused, {orders_failed} failed, "
        "{orders_resent} resent; "
        "log: runs/{run_id}/orders.jsonl".format(**status)
    )
    # the kitchen results too (review round 3, item 6): the operator
    # should not have to open the log to see how events and restocks
    # ended
    print(
        "events: {events_sent} sent, {events_acked} acked, {events_duplicate} duplicate, "
        "{events_conflicted} conflicted, {events_failed} failed, {events_skipped} skipped; "
        "restocks: {restocks_sent} sent, {restocks_acked} acked, {restocks_failed} failed".format(
            **status
        )
    )
    if status["state"] not in ("done", "stopped"):
        return 1
    # a run that sent orders but never got an API answer for any of them
    # is a config mistake wearing a success costume: refuse to call it 0
    answered = status["orders_acked"] + status["orders_parked"] + status["orders_refused"]
    if status["orders_sent"] > 0 and answered == 0:
        print(
            "rosctl playback start: no order was acked, parked or refused: "
            f"all {status['orders_failed']} sends to {target} failed "
            f"(see runs/{status['run_id']}/orders.jsonl)",
            file=sys.stderr,
        )
        return 1
    return 0


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
        # the control plane's project error body carries `message`
        # ({"error": "invalid_state", "state": ..., "message": ...});
        # fall back to detail for any other 409 shape
        body = response.json()
        print(
            f"rosctl playback {command}: {body.get('message') or body.get('detail')}",
            file=sys.stderr,
        )
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


# --- the control process (WP6b) -------------------------------------------


@dataclass
class _ControlExtras:
    toxiproxy: Toxiproxy
    jobs: JobRunner
    cluster_file: Path
    groups: int

    async def aclose(self) -> None:
        await self.jobs.aclose()
        await self.toxiproxy.aclose()


def _add_control_extras(
    app: Any,
    cluster_file: Path,
    *,
    port: int | None = None,
    scenario_engine: dict[str, Any] | None = None,
) -> _ControlExtras:
    """Mount the links, faults, nodes and jobs routes beside the playback
    routes. The cluster file decides the link groups and the nodes - use
    the file bootstrap ran with. `port` is the one the control process
    listens on: its check and snapshot jobs reach its playback there.
    With `scenario_engine` (the playback engine and what the scenarios
    need to know about it) the scenario routes come too."""
    from restaurantos.common.cluster import link_groups
    from restaurantos.sim.control_api import add_control_routes, add_sim_jobs
    from restaurantos.sim.faults import FaultLayer
    from restaurantos.sim.nodes import Nodes

    cluster = load_cluster(cluster_file)
    groups = link_groups(cluster)
    toxiproxy = Toxiproxy()
    jobs = JobRunner()
    faults = FaultLayer(toxiproxy, groups)
    nodes = Nodes(cluster)
    add_control_routes(app, faults=faults, nodes=nodes, jobs=jobs)
    add_sim_jobs(app, port=port)
    if scenario_engine is not None:
        from restaurantos.sim.control_jobs import add_job_routes
        from restaurantos.sim.scenarios import default_env

        engine = dict(scenario_engine)
        engine.pop("port")
        env = default_env(cluster, cluster_file, faults=faults, nodes=nodes, **engine)
        add_job_routes(app, jobs=jobs, scenarios=env)
    return _ControlExtras(toxiproxy, jobs, cluster_file, len(groups))


async def _wait_for_stop() -> None:
    """Block until Ctrl-C or SIGTERM. A seam the tests replace."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.remove_signal_handler(sig)


async def _serve_control(
    playback: Playback,
    server: Any,
    serve: asyncio.Task,
    extras: _ControlExtras,
    sim: SimSettings,
    args: argparse.Namespace,
) -> int:
    print(
        f"control on {sim.control_host}:{args.port or sim.control_port}: playback, "
        f"{extras.groups} link groups from {extras.cluster_file}, nodes, jobs. "
        "Start a run with `rosctl playback start --via-control --from-day N`; "
        "Ctrl-C stops the process (a live run is stopped first).",
        file=sys.stderr,
    )
    try:
        await _wait_for_stop()
    finally:
        if playback.state in ("running", "paused", "stopping"):
            with contextlib.suppress(StateError):
                await playback.stop()
        await extras.aclose()
        server.should_exit = True
        await serve
    print(
        "rosctl control: stopped. Faults stay in Toxiproxy after this process "
        "exits; `rosctl fault heal-all` from a new control process clears them.",
        file=sys.stderr,
    )
    return 0


# the flags `rosctl control` fixes for every run it hosts: a run started
# through it cannot change them
_ENGINE_FLAGS = {
    "config": None,
    "region": None,
    "target": None,
    "sequential": False,
    "kitchen_target": None,
    "inflight": None,
    "events": None,
    "event_sample_rate": None,
    "restock_days": None,
    "duplicate_offset_s": None,
    "shuffle_window": None,
}


async def start_via_control(args: argparse.Namespace) -> int:
    """`rosctl playback start --via-control`: the start body, posted to the
    control process. Only the run's own flags travel."""
    given = [
        "--" + name.replace("_", "-")
        for name, unset in _ENGINE_FLAGS.items()
        if getattr(args, name, unset) != unset
    ]
    if given:
        print(
            f"rosctl playback start: {', '.join(given)} belong to `rosctl control` "
            "(they hold for every run it hosts); drop them with --via-control",
            file=sys.stderr,
        )
        return 1
    if args.from_day is None:
        print(
            "rosctl playback start: --via-control needs --from-day "
            "(the control process picks no default window)",
            file=sys.stderr,
        )
        return 1
    body: dict[str, Any] = {"action": "start", "from_day": args.from_day, "seed": args.seed}
    for key in ("to_day", "speed", "run_id", "duplicate_rate", "shuffle"):
        value = getattr(args, key)
        if value is not None:
            body[key] = value
    if getattr(args, "skip_first_restock", False):
        # a snapshot restore's replay sets this; it is the run's own
        # flag even though a plain start leaves it off
        body["skip_first_restock"] = True
    return await control_command(args, method="POST", body=body)


def run_control(args: argparse.Namespace) -> int:
    return asyncio.run(start_command(args, control=True))


def run(args: argparse.Namespace) -> int:
    command = args.play_command
    if command == "start":
        if getattr(args, "via_control", False):
            return asyncio.run(start_via_control(args))
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

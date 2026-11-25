"""The control process's own `/sim` routes (WP6b, architecture Section 6.3):
links, faults, nodes and jobs. The playback routes come from
`sim/control.py`; `rosctl control` serves both on port 8002.

Errors use the project body `{"error": ..., "message": ...}` (like
`invalid_state` in `sim/control.py`), not FastAPI's `{"detail": ...}`;
body-validation 422s keep FastAPI's default shape, as on the gateway.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from restaurantos.sim.config import get_sim_settings, normalise_origin, parse_allowed_origins
from restaurantos.sim.faults import FaultError, FaultExistsError, FaultLayer
from restaurantos.sim.jobs import BusyError, JobRunner, UnknownJobError
from restaurantos.sim.nodes import NodeError, Nodes, UnknownNodeError
from restaurantos.sim.toxiproxy import ToxiproxyError


class FaultBody(BaseModel):
    target: str
    fault: str
    params: dict[str, Any] = Field(default_factory=dict)


def error_response(status: int, code: str, message: str, **extra: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message, **extra})


def _toxiproxy_down(error: ToxiproxyError) -> JSONResponse:
    return error_response(502, "toxiproxy_error", str(error))


# --- cross-site protection -------------------------------------------------

# Every method the control routes use; CORSMiddleware needs them listed
# explicitly (its default allows GET only, which would fail every POST
# preflight). A drift test in test_control_api.py walks app.routes and
# fails when a new route uses a method missing here.
CONTROL_CORS_METHODS = ("GET", "POST", "PATCH", "DELETE")

_STATE_CHANGING = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _rejected() -> JSONResponse:
    return JSONResponse(status_code=403, content={"detail": "cross-origin request rejected"})


def add_origin_guard(app: FastAPI) -> None:
    """Reject cross-origin state changes on every route of this app.

    A browser fires kill/start/heal-all as a "simple" cross-origin POST
    (no preflight), so the check cannot live in a route: it runs as a
    middleware for every POST/PUT/PATCH/DELETE, including the embedded
    playback routes. GET/HEAD/OPTIONS are unaffected, and requests with
    no Origin (the CLI clients) pass: the allowlist is only consulted
    when a browser sent one."""
    if getattr(app.state, "origin_guard", False):
        return
    app.state.origin_guard = True

    @app.middleware("http")
    async def reject_cross_site(request: Request, call_next: Any) -> Any:
        if request.method in _STATE_CHANGING:
            if request.headers.get("sec-fetch-site", "").lower() == "cross-site":
                return _rejected()
            origin = request.headers.get("origin")
            if origin is not None:
                allowlist = parse_allowed_origins(get_sim_settings().control_allowed_origins)
                # the literal "null" origin is never allowlisted: it is
                # what a sandboxed iframe or a redirected form sends
                try:
                    normalised = normalise_origin(origin)
                except ValueError:
                    # a malformed Origin never matches an allowlist entry
                    normalised = None
                if origin == "null" or normalised not in allowlist:
                    return _rejected()
        return await call_next(request)


def configure_control_cors(app: FastAPI) -> None:
    """Exact-allowlist CORS for browser clients (WP7), same allowlist as
    the origin guard: `parse_allowed_origins` validates the setting here,
    so a bad entry stops the process at startup.

    CORSMiddleware only decides whether the browser may *read* answers
    and answers preflights itself; it does not block real requests -
    the origin guard does that. It must sit outside the guard (be added
    after it), so the guard's 403s carry CORS headers and the UI can
    show the message instead of a vague "CORS error". An empty allowlist
    (the default, CLI-only) registers no CORS middleware at all."""
    if getattr(app.state, "control_cors", False):
        return
    app.state.control_cors = True
    origins = parse_allowed_origins(get_sim_settings().control_allowed_origins)
    if not origins:
        return
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(origins),
        allow_methods=list(CONTROL_CORS_METHODS),
        # JSON bodies make every browser POST/PATCH a non-simple request,
        # so its preflight always asks for content-type; this Starlette
        # (1.10) checks requested headers strictly against this list
        allow_headers=["content-type"],
        allow_credentials=False,  # no cookies involved
        expose_headers=[],
        max_age=600,
    )


def add_control_routes(app: FastAPI, *, faults: FaultLayer, nodes: Nodes, jobs: JobRunner) -> None:
    """Mount the links, faults, nodes and jobs routes on `app`. The three
    objects are also left on `app.state`, so later routes (the check,
    snapshot and scenario jobs) reach the same instances."""
    add_origin_guard(app)
    app.state.faults = faults
    app.state.nodes = nodes
    app.state.jobs = jobs

    @app.get("/sim/links")
    async def sim_links() -> Any:
        try:
            return await faults.links()
        except ToxiproxyError as error:
            return _toxiproxy_down(error)

    @app.post("/sim/faults", status_code=201)
    async def sim_fault_create(body: FaultBody) -> Any:
        try:
            fault = await faults.create(body.target, body.fault, body.params)
        except FaultExistsError as error:
            return error_response(error.status, error.code, str(error), fault_id=error.fault_id)
        except FaultError as error:
            return error_response(error.status, error.code, str(error))
        except ToxiproxyError as error:
            return _toxiproxy_down(error)
        response = fault.as_dict()
        if fault.fault == "cut":
            # proxies already disabled before the cut: not ours to re-enable
            response["already_disabled"] = list(fault.preexisting)
        return response

    # declared before the {fault_id} route: a literal path wins
    @app.post("/sim/faults/heal-all")
    async def sim_fault_heal_all() -> Any:
        try:
            return {"healed": await faults.heal_all()}
        except ToxiproxyError as error:
            return _toxiproxy_down(error)

    @app.delete("/sim/faults/{fault_id}")
    async def sim_fault_heal(fault_id: str) -> Any:
        try:
            healed = await faults.heal(fault_id)
        except FaultError as error:
            return error_response(error.status, error.code, str(error))
        except ToxiproxyError as error:
            return _toxiproxy_down(error)
        response = healed.fault.as_dict()
        if healed.fault.fault == "cut":
            # proxies that stay disabled: they were already disabled
            # before their cut, and the state is not ours to change
            response["left_disabled"] = list(healed.left_disabled)
        return response

    async def _node(node_id: str, action: str) -> Any:
        try:
            if action == "kill":
                return await nodes.kill(node_id)
            return await nodes.start(node_id)
        except UnknownNodeError as error:
            return error_response(404, "unknown_node", str(error.args[0]))
        except NodeError as error:
            return error_response(502, "docker_error", str(error))

    @app.post("/sim/nodes/{node_id}/kill")
    async def sim_node_kill(node_id: str) -> Any:
        return await _node(node_id, "kill")

    @app.post("/sim/nodes/{node_id}/start")
    async def sim_node_start(node_id: str) -> Any:
        return await _node(node_id, "start")

    @app.get("/sim/jobs/{job_id}")
    async def sim_job(job_id: str) -> Any:
        try:
            return jobs.get(job_id).as_dict()
        except UnknownJobError:
            return error_response(404, "unknown_job", f"no job {job_id!r}")

    # added after the guard: CORS must be the outermost middleware
    configure_control_cors(app)


class CheckBody(BaseModel):
    """POST /sim/check: the runs to check and whether parked-but-unacked
    orders count as failures (the CLI's --allow-unacked)."""

    runs: list[str]
    allow_unacked: bool = False


class SaveSnapshotBody(BaseModel):
    # the same shape a run id allows: one path component, no traversal
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class RestoreSnapshotBody(BaseModel):
    replace_cluster_file: bool = False


def add_sim_jobs(app: FastAPI) -> None:
    """Mount the check and snapshot job routes: `rosctl check` and
    `rosctl snapshot save|restore` as jobs on the runner that
    `add_control_routes` left on `app.state` (one job at a time;
    GET /sim/jobs/{job_id} polls). `GET /sim/snapshots` is a plain read.
    The jobs run the same code the CLI commands run, against the
    checkout the control process was started from."""
    import asyncio
    from pathlib import Path

    from restaurantos.common.cluster import load_cluster
    from restaurantos.common.config import get_settings
    from restaurantos.sim.checker import run_check
    from restaurantos.sim.replaydata import CHANNEL
    from restaurantos.sim.snapshot import (
        LIVE_STATES,
        PlaybackClient,
        default_context,
        list_snapshots,
        restore,
        save,
    )

    sim = get_sim_settings()

    async def _live_run() -> str | None:
        """The run this process's own playback host is still running,
        pausing or stopping; None when there is none or no answer."""
        client = PlaybackClient(
            f"http://{sim.control_host}:{sim.control_port}", timeout_s=sim.http_timeout_s
        )
        try:
            status = await client.status()
        except Exception:  # noqa: BLE001 - reported through the job instead
            return None
        if status and status.get("state") in (*LIVE_STATES, "stopping"):
            return status.get("run_id")
        return None

    def _submit(kind: str, name: str | None, work) -> Any:
        try:
            return app.state.jobs.submit(kind, name, work).as_dict()
        except BusyError as error:
            return error_response(409, "busy", str(error), job_id=error.job_id)

    @app.post("/sim/check", status_code=202)
    async def sim_check(body: CheckBody) -> Any:
        async def work() -> Any:
            # read inside the job: an earlier job in the queue (a
            # restore) may have replaced the cluster file or moved the
            # live run on
            cluster = load_cluster(Path(get_settings().cluster_file))
            return await asyncio.to_thread(
                run_check,
                cluster,
                body.runs,
                runs_dir=Path("runs"),
                channel=CHANNEL,
                allow_unacked=body.allow_unacked,
                drain_timeout_s=sim.check_drain_timeout_s,
                poll_s=sim.check_poll_s,
                live_run=await _live_run(),
            )

        return _submit("check", None, work)

    @app.get("/sim/snapshots")
    async def sim_snapshots() -> Any:
        return {"snapshots": list_snapshots(sim.snapshot_dir)}

    @app.post("/sim/snapshots", status_code=202)
    async def sim_snapshot_save(body: SaveSnapshotBody) -> Any:
        async def work() -> Any:
            ctx = default_context(Path(get_settings().cluster_file), sim)
            return await save(body.name, ctx)

        return _submit("snapshot", body.name, work)

    @app.post("/sim/snapshots/{name}/restore", status_code=202)
    async def sim_snapshot_restore(name: str, body: RestoreSnapshotBody | None = None) -> Any:
        replace = bool(body and body.replace_cluster_file)

        async def work() -> Any:
            ctx = default_context(Path(get_settings().cluster_file), sim)
            return await restore(name, ctx, replace_cluster_file=replace)

        return _submit("restore", name, work)

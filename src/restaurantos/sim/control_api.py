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
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from restaurantos.sim.config import get_sim_settings
from restaurantos.sim.faults import FaultError, FaultExistsError, FaultLayer
from restaurantos.sim.jobs import JobRunner, UnknownJobError
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

_STATE_CHANGING = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _normalise_origin(origin: str) -> str:
    """Normalise for exact comparison: scheme and host compare
    case-insensitively, a trailing slash is dropped."""
    return origin.strip().rstrip("/").lower()


def allowed_origins(raw: str) -> set[str]:
    """The configured allowlist as normalised origins."""
    return {_normalise_origin(origin) for origin in raw.split(",") if origin.strip()}


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
                allowlist = allowed_origins(get_sim_settings().control_allowed_origins)
                # the literal "null" origin is never allowlisted: it is
                # what a sandboxed iframe or a redirected form sends
                if origin == "null" or _normalise_origin(origin) not in allowlist:
                    return _rejected()
        return await call_next(request)


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

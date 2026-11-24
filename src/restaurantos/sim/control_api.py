"""The control process's own `/sim` routes (WP6b, architecture Section 6.3):
links, faults, nodes and jobs. The playback routes come from
`sim/control.py`; `rosctl control` serves both on port 8002.

Errors use the project body `{"error": ..., "message": ...}` (like
`invalid_state` in `sim/control.py`), not FastAPI's `{"detail": ...}`;
body-validation 422s keep FastAPI's default shape, as on the gateway.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

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


def add_control_routes(app: FastAPI, *, faults: FaultLayer, nodes: Nodes, jobs: JobRunner) -> None:
    """Mount the links, faults, nodes and jobs routes on `app`. The three
    objects are also left on `app.state`, so later routes (the check,
    snapshot and scenario jobs) reach the same instances."""
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
        return fault.as_dict()

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
            fault = await faults.heal(fault_id)
        except FaultError as error:
            return error_response(error.status, error.code, str(error))
        except ToxiproxyError as error:
            return _toxiproxy_down(error)
        return fault.as_dict()

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

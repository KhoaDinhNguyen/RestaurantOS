"""The control process's scenario routes (WP6b, architecture Section 6.3),
for the Ops view.

Each `POST` validates what it can at once (unknown scenario, bad
parameters, a live run) and answers `202` with a job; the work runs in
the control process's job runner, one job at a time, and
`GET /sim/jobs/{job_id}` reports on it. The scenarios are built here
(`sim/scenarios.py`); the snapshot, restore and check routes live in
`control_api.add_sim_jobs` (PR #33) and are not mounted here.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from restaurantos.sim.control_api import error_response
from restaurantos.sim.jobs import BusyError, JobRunner
from restaurantos.sim.scenarios import (
    ScenarioEnv,
    ScenarioError,
    describe,
    parse_params,
    refuse_reason,
    run_scenario,
)


def add_job_routes(
    app: FastAPI,
    *,
    jobs: JobRunner,
    scenarios: ScenarioEnv,
) -> None:
    """Mount the scenario routes. `scenarios` holds the control process's
    own playback engine, fault layer and nodes."""

    @app.get("/sim/scenarios")
    async def sim_scenarios() -> Any:
        return describe()

    @app.post("/sim/scenarios/{name}", status_code=202)
    async def sim_scenario(name: str, body: Annotated[dict[str, Any] | None, Body()] = None) -> Any:
        try:
            spec, _params = parse_params(name, body, scenarios.cluster)
        except ScenarioError as error:
            return error_response(error.status, error.code, str(error))
        reason = await refuse_reason(scenarios)
        if reason is not None:
            status, code, message = reason
            return error_response(status, code, message)
        holder: dict[str, str] = {}

        async def work() -> dict[str, Any]:
            return await run_scenario(
                spec.name,
                body,
                scenarios,
                progress=lambda text: jobs.set_progress(holder["job_id"], text),
            )

        try:
            job = jobs.submit("scenario", spec.name, work)
        except BusyError as error:
            return error_response(409, "busy", str(error), job_id=error.job_id)
        # the job's task starts on the loop's next turn, after this line
        holder["job_id"] = job.job_id
        return JSONResponse(status_code=202, content=job.as_dict())

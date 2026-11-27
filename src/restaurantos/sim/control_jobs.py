"""The control process's scenario routes (WP6b, architecture Section 6.3),
for the Ops view.

Each `POST` validates what it can at once (unknown scenario, bad
parameters, a live run, and the scenario's own `prepare` checks: no
standby to promote, no product to copy, a region HQ does not know) and
answers `202` with a job; the work runs in the control process's job
runner, one job at a time, and `GET /sim/jobs/{job_id}` reports on it.
The scenarios are built here (`sim/scenarios.py`); the snapshot, restore
and check routes live in `control_api.add_sim_jobs` (PR #33) and are not
mounted here.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from restaurantos.sim.control_api import error_response
from restaurantos.sim.jobs import BusyError, JobRunner
from restaurantos.sim.scenarios import (
    SCRIPTS,
    ScenarioEnv,
    ScenarioError,
    Session,
    describe,
    engine_limits,
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
            spec, params = parse_params(name, body, scenarios.cluster)
            engine_limits(scenarios, params)
        except ScenarioError as error:
            return error_response(error.status, error.code, str(error))
        reason = await refuse_reason(scenarios)
        if reason is not None:
            status, code, message = reason
            return error_response(status, code, message)
        # the scenario's own pre-start checks, at the POST: a refusal
        # here costs nothing (no run, no job). The job runs prepare again
        # when it starts - nothing else changes. A probe that fails or
        # times out (10 s) is not a refusal: the databases may just be
        # busy, and the job gets to decide.
        probe_session = Session(scenarios, spec, params, lambda text: None)
        try:
            await asyncio.wait_for(SCRIPTS[spec.name]().prepare(probe_session), timeout=10)
        except ScenarioError as error:
            return error_response(error.status, error.code, str(error))
        except Exception:  # noqa: BLE001 - the job re-runs prepare and judges
            pass
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

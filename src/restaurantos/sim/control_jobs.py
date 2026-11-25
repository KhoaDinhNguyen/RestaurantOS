"""The control process's job routes (WP6b, architecture Section 6.3):
scenarios, snapshots, restore and the checker over HTTP, for the Ops view.

Each `POST` validates what it can at once (unknown scenario or snapshot,
bad parameters, a live run) and answers `202` with a job; the work runs
in the control process's job runner, one job at a time, and
`GET /sim/jobs/{job_id}` reports on it. Scenarios are built here
(`sim/scenarios.py`); snapshots and the checker run the same code as
`rosctl snapshot` and `rosctl check` (`sim/snapshot.py`,
`sim/checker.py`).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated, Any

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from restaurantos.sim.config import get_sim_settings
from restaurantos.sim.control_api import error_response
from restaurantos.sim.jobs import BusyError, JobRunner
from restaurantos.sim.scenarios import (
    LIVE_STATES,
    ScenarioEnv,
    ScenarioError,
    describe,
    parse_params,
    refuse_reason,
    run_scenario,
)

RUNS_DIR = Path("runs")


class SnapshotBody(BaseModel):
    name: str


class RestoreBody(BaseModel):
    replace_cluster_file: bool = False


class CheckBody(BaseModel):
    runs: list[str] = Field(min_length=1)
    allow_unacked: bool = False


def _not_built(what: str) -> JSONResponse:
    return error_response(
        501, "not_built", f"{what} is not on this branch yet (sim/snapshot.py, sim/checker.py)"
    )


def add_job_routes(
    app: FastAPI,
    *,
    jobs: JobRunner,
    scenarios: ScenarioEnv,
    cluster_file: Path,
    control_port: int,
    runs_dir: Path = RUNS_DIR,
) -> None:
    """Mount the job routes. `scenarios` holds the control process's own
    playback engine, fault layer and nodes; snapshots call this process's
    playback routes over HTTP on `control_port`, like `rosctl snapshot`."""

    def submit(kind: str, name: str | None, work: Any) -> JSONResponse:
        try:
            job = jobs.submit(kind, name, work)
        except BusyError as error:
            return error_response(409, "busy", str(error), job_id=error.job_id)
        return JSONResponse(status_code=202, content=job.as_dict())

    @app.get("/sim/scenarios")
    async def sim_scenarios() -> Any:
        return describe()

    @app.post("/sim/scenarios/{name}", status_code=202)
    async def sim_scenario(name: str, body: Annotated[dict[str, Any] | None, Body()] = None) -> Any:
        try:
            spec, _params = parse_params(name, body, scenarios.cluster)
        except ScenarioError as error:
            return error_response(error.status, error.code, str(error))
        reason = refuse_reason(scenarios)
        if reason is not None:
            return error_response(409, "scenario_refused", reason)
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

    @app.get("/sim/snapshots")
    async def sim_snapshots() -> Any:
        try:
            from restaurantos.sim.snapshot import list_snapshots
        except ImportError:
            return _not_built("snapshots")
        return list_snapshots(get_sim_settings().snapshot_dir)

    @app.post("/sim/snapshots", status_code=202)
    async def sim_snapshot_save(body: SnapshotBody) -> Any:
        try:
            from restaurantos.sim.snapshot import SnapshotError, check_name, default_context, save
        except ImportError:
            return _not_built("snapshots")
        sim = get_sim_settings()
        try:
            check_name(body.name)
        except SnapshotError as error:
            return error_response(422, "invalid_name", str(error))
        if (sim.snapshot_dir / body.name).exists():
            return error_response(409, "snapshot_exists", f"snapshot {body.name!r} exists")

        async def work() -> dict[str, Any]:
            return await save(body.name, default_context(cluster_file, sim, port=control_port))

        return submit("snapshot", body.name, work)

    @app.post("/sim/snapshots/{name}/restore", status_code=202)
    async def sim_snapshot_restore(name: str, body: RestoreBody | None = None) -> Any:
        try:
            from restaurantos.sim.snapshot import default_context, restore
        except ImportError:
            return _not_built("snapshots")
        sim = get_sim_settings()
        if not (sim.snapshot_dir / name).is_dir():
            return error_response(404, "unknown_snapshot", f"no snapshot {name!r}")
        replace = body.replace_cluster_file if body is not None else False

        async def work() -> dict[str, Any]:
            return await restore(
                name,
                default_context(cluster_file, sim, port=control_port),
                replace_cluster_file=replace,
            )

        return submit("restore", name, work)

    @app.post("/sim/check", status_code=202)
    async def sim_check(body: CheckBody) -> Any:
        try:
            from restaurantos.sim.checker import run_check
        except ImportError:
            return _not_built("the checker")
        missing = [run for run in body.runs if not (runs_dir / run / "meta.json").is_file()]
        if missing:
            return error_response(404, "unknown_run", f"no run {', '.join(missing)} under runs/")
        status = scenarios.playback.status()
        if status.get("state") in LIVE_STATES and status.get("run_id") in body.runs:
            return error_response(
                409, "run_live", f"run {status['run_id']} is still {status['state']}"
            )
        sim = get_sim_settings()

        async def work() -> dict[str, Any]:
            from restaurantos.common.cluster import load_cluster
            from restaurantos.sim.replaydata import CHANNEL

            return await asyncio.to_thread(
                run_check,
                load_cluster(cluster_file),
                body.runs,
                runs_dir=runs_dir,
                channel=CHANNEL,
                allow_unacked=body.allow_unacked,
                drain_timeout_s=getattr(sim, "check_drain_timeout_s", 300.0),
                poll_s=getattr(sim, "check_poll_s", 1.0),
            )

        return submit("check", None, work)

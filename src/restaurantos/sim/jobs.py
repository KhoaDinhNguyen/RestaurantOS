"""The control process's job runner (architecture Section 6.3).

A scenario, a snapshot, a restore or a check takes seconds to minutes,
too long for one request: its `POST` answers `202` with a job, and
`GET /sim/jobs/{job_id}` reports on it. One job runs at a time - a
snapshot stops every database, and two scenarios would break each
other - so a second submit while one runs is refused with the running
job's id. `rosctl` runs the same work directly and waits; the runner is
only the HTTP path.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

JOB_KINDS = ("scenario", "snapshot", "restore", "check")


class BusyError(Exception):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"job {job_id} is still running; one job at a time")
        self.job_id = job_id


class UnknownJobError(KeyError):
    pass


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Job:
    job_id: str
    kind: str
    name: str | None
    started_at: str
    state: str = "running"
    finished_at: str | None = None
    result: Any = None
    error: str | None = None
    # a short line on where a running job is (a scenario's phase), for
    # the Ops view and `rosctl scenario`; None when the work sets none
    progress: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "kind": self.kind,
            "name": self.name,
            "state": self.state,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "error": self.error,
            "progress": self.progress,
        }


class JobRunner:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._ids = itertools.count(1)

    def running(self) -> Job | None:
        return next((job for job in self._jobs.values() if job.state == "running"), None)

    def submit(self, kind: str, name: str | None, work: Callable[[], Awaitable[Any]]) -> Job:
        """Start `work` as a job and return it at once. Needs a running
        event loop (the control process's)."""
        if kind not in JOB_KINDS:
            raise ValueError(f"unknown job kind {kind!r}; one of {', '.join(JOB_KINDS)}")
        current = self.running()
        if current is not None:
            raise BusyError(current.job_id)
        job = Job(job_id=f"j{next(self._ids)}", kind=kind, name=name, started_at=_now())
        self._jobs[job.job_id] = job
        self._tasks[job.job_id] = asyncio.create_task(
            self._run(job, work), name=f"job-{job.job_id}"
        )
        return job

    def get(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise UnknownJobError(job_id)
        return job

    def set_progress(self, job_id: str, text: str) -> None:
        """Called by a job's own work to say where it is."""
        job = self._jobs.get(job_id)
        if job is not None and job.state == "running":
            job.progress = text

    async def wait(self, job_id: str) -> Job:
        task = self._tasks.get(job_id)
        if task is not None:
            await asyncio.shield(task)
        return self.get(job_id)

    async def aclose(self) -> None:
        """Cancel a job still running (the control process is exiting)."""
        for task in self._tasks.values():
            task.cancel()
        for task in list(self._tasks.values()):
            with contextlib.suppress(asyncio.CancelledError):
                await task

    @staticmethod
    async def _run(job: Job, work: Callable[[], Awaitable[Any]]) -> None:
        try:
            job.result = await work()
            job.state = "done"
        except asyncio.CancelledError:
            job.state = "failed"
            job.error = "cancelled: the control process stopped"
            raise
        except Exception as error:  # noqa: BLE001 - a failed job is a reported state
            job.state = "failed"
            job.error = f"{type(error).__name__}: {error}"
        finally:
            job.finished_at = _now()

"""Node kill/start against a fake `docker compose`, and the job runner."""

from __future__ import annotations

import asyncio
import subprocess

import pytest
from fake_compose import FakeCompose

from restaurantos.common.cluster import Cluster
from restaurantos.sim.jobs import BusyError, JobRunner, UnknownJobError
from restaurantos.sim.nodes import NodeError, Nodes, UnknownNodeError, parse_running


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", False),
        ('{"Service": "r1-primary", "State": "running"}\n', True),
        ('{"Service": "r1-primary", "State": "exited"}', False),
        ('[{"Service": "r1-primary", "State": "running"}]', True),
        ('{"State": "exited"}\n{"State": "running"}\n', True),
    ],
)
def test_parse_running_reads_both_compose_formats(text, expected) -> None:
    assert parse_running(text) is expected


async def test_kill_and_start_change_the_node_once() -> None:
    compose = FakeCompose({"r1-primary"})
    nodes = Nodes(Cluster(regions=1), compose=compose)
    assert await nodes.kill("r1-primary") == {"node_id": "r1-primary", "running": False}
    assert ("kill", "r1-primary") in compose.calls
    # killing a stopped node changes nothing
    compose.calls.clear()
    assert await nodes.kill("r1-primary") == {"node_id": "r1-primary", "running": False}
    assert not any(call[0] == "kill" for call in compose.calls)
    assert await nodes.start("r1-primary") == {"node_id": "r1-primary", "running": True}
    compose.calls.clear()
    await nodes.start("r1-primary")
    assert not any(call[0] == "start" for call in compose.calls)


async def test_unknown_nodes_and_the_baseline_are_refused() -> None:
    nodes = Nodes(Cluster(regions=1), compose=FakeCompose(set()))
    with pytest.raises(UnknownNodeError):
        await nodes.kill("r2-primary")
    with pytest.raises(UnknownNodeError):
        await nodes.kill("baseline")


async def test_a_docker_failure_is_a_node_error() -> None:
    def broken(*args: str) -> subprocess.CompletedProcess:
        raise RuntimeError("docker compose ps failed (1): no such service")

    nodes = Nodes(Cluster(regions=1), compose=broken)
    with pytest.raises(NodeError, match="no such service"):
        await nodes.running("r1-primary")


async def test_a_job_runs_and_reports_its_result() -> None:
    runner = JobRunner()

    async def work() -> dict:
        await asyncio.sleep(0)
        return {"passed": True}

    job = runner.submit("check", None, work)
    assert job.as_dict()["state"] == "running"
    done = await runner.wait(job.job_id)
    assert done.state == "done" and done.result == {"passed": True}
    assert done.finished_at is not None


async def test_one_job_at_a_time() -> None:
    runner = JobRunner()
    release = asyncio.Event()

    async def slow() -> None:
        await release.wait()

    first = runner.submit("snapshot", "before-t2", slow)
    with pytest.raises(BusyError) as caught:
        runner.submit("check", None, slow)
    assert caught.value.job_id == first.job_id
    release.set()
    await runner.wait(first.job_id)
    # the slot is free again
    second = runner.submit("check", None, slow)
    await runner.wait(second.job_id)


async def test_a_failing_job_ends_failed_and_frees_the_slot() -> None:
    runner = JobRunner()

    async def broken() -> None:
        raise RuntimeError("the drain timed out")

    job = runner.submit("check", None, broken)
    done = await runner.wait(job.job_id)
    assert done.state == "failed"
    assert done.error == "RuntimeError: the drain timed out"
    assert runner.running() is None


def test_unknown_jobs_and_kinds() -> None:
    runner = JobRunner()
    with pytest.raises(UnknownJobError):
        runner.get("j9")
    with pytest.raises(ValueError, match="unknown job kind"):
        runner.submit("dance", None, lambda: None)  # type: ignore[arg-type,return-value]


async def test_aclose_cancels_a_running_job() -> None:
    runner = JobRunner()
    job = runner.submit("scenario", "T2", lambda: asyncio.sleep(3600))
    await asyncio.sleep(0)
    await runner.aclose()
    assert runner.get(job.job_id).state == "failed"
    assert "cancelled" in (runner.get(job.job_id).error or "")


async def test_a_job_reports_its_progress_while_it_runs() -> None:
    runner = JobRunner()
    release = asyncio.Event()

    async def work() -> None:
        await release.wait()

    job = runner.submit("scenario", "T2", work)
    assert job.as_dict()["progress"] is None
    runner.set_progress(job.job_id, "T2: fault held, 30 s left")
    assert runner.get(job.job_id).as_dict()["progress"] == "T2: fault held, 30 s left"
    release.set()
    await runner.wait(job.job_id)
    # a finished job keeps its last line and takes no new one
    runner.set_progress(job.job_id, "late")
    assert runner.get(job.job_id).progress == "T2: fault held, 30 s left"
    runner.set_progress("j99", "unknown jobs are ignored")

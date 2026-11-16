"""The baseline sync's quiet gate and row-count report, plus the CLI
wiring. The copy itself is tested against the real generated cluster
(tests/cluster/test_baseline_sync.py); here everything runs against a
faked control endpoint or the in-process control app."""

from __future__ import annotations

import socket

import httpx
import pytest
from conftest import FakeWallClock, make_playback, sample_orders

from restaurantos.sim.baseline_sync import (
    BaselineSyncError,
    format_report,
    playback_refusal,
)
from restaurantos.sim.cli import app
from restaurantos.sim.control import make_control_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://control.test")


def status_client(body: object, status: int = 200) -> httpx.Client:
    return _client(lambda request: httpx.Response(status, json=body))


# --- a quiet system ------------------------------------------------------------


def test_no_playback_process_is_quiet():
    """Connection refused: nothing is bound on the control port, so no
    playback process exists and nothing is running."""
    with httpx.Client(base_url=f"http://127.0.0.1:{_free_port()}", timeout=5) as client:
        assert playback_refusal(client) is None


@pytest.mark.parametrize("state", ["idle", "paused", "done", "stopped", "failed"])
def test_settled_states_with_nothing_in_flight_are_quiet(state):
    with status_client({"state": state, "run_id": "probe", "inflight": 0}) as client:
        assert playback_refusal(client) is None


async def test_the_gate_reads_a_real_run_status(tmp_path):
    """The contract end to end: the engine's own status body, served by
    the control routes this command is the named consumer of, drives
    the gate. A live run refuses and names the run; a paused run with
    nothing in flight is quiet."""
    playback = make_playback(sample_orders(10), tmp_path, clock=FakeWallClock())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(playback)),
        base_url="http://control.test",
    ) as client:
        await playback.start(seed=0, from_day=14, to_day=15, speed=1000.0, run_id="gate-contract")
        running = (await client.get("/sim/playback")).json()
        assert running["state"] == "running"
        await playback.pause()
        paused = (await client.get("/sim/playback")).json()
        assert paused["state"] == "paused"

    with status_client(running) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "gate-contract" in refusal
    assert "running" in refusal

    # paused with nothing in flight is exactly the quiet the spec allows
    with status_client(paused) as client:
        assert playback_refusal(client) is None


# --- what is still running ------------------------------------------------------


def test_running_refuses_and_names_the_run():
    with status_client({"state": "running", "run_id": "probe-1", "inflight": 0}) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "probe-1" in refusal
    assert "running" in refusal
    assert "0 request(s)" in refusal
    assert "pause or stop" in refusal


def test_stopping_refuses():
    with status_client({"state": "stopping", "run_id": "probe-1", "inflight": 0}) as client:
        assert playback_refusal(client) is not None


def test_paused_with_requests_still_in_flight_refuses():
    """PAUSED with requests in flight is NOT quiet: the rows those
    requests will write would be missing from the baseline."""
    with status_client({"state": "paused", "run_id": "probe-2", "inflight": 3}) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "probe-2" in refusal
    assert "paused" in refusal
    assert "3 request(s)" in refusal


def test_a_live_run_without_an_id_still_refuses():
    with status_client({"state": "running", "run_id": None, "inflight": 0}) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "'<?>'" in refusal


# --- a sick control endpoint ------------------------------------------------------


def test_a_timeout_is_not_quiet():
    """A bound port that does not answer may be a hung-but-live run:
    exactly the case the gate exists for."""

    def handler(request):
        raise httpx.ConnectTimeout("connection timed out")

    with _client(handler) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "cannot reach" in refusal


def test_an_http_error_status_refuses():
    with status_client({"state": "idle", "inflight": 0}, status=503) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "503" in refusal


def test_a_body_that_is_not_json_refuses():
    with _client(lambda request: httpx.Response(200, text="look at me")) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "not JSON" in refusal


def test_a_status_that_is_not_an_object_refuses():
    with status_client(["running"]) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "not an object" in refusal


def test_an_unknown_state_refuses():
    with status_client({"state": "teleporting", "run_id": "probe", "inflight": 0}) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "unknown state" in refusal


def test_a_bad_in_flight_count_refuses():
    with status_client({"state": "idle", "run_id": "probe", "inflight": "many"}) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "in-flight" in refusal


def test_a_missing_status_refuses():
    with status_client({}) as client:
        assert playback_refusal(client) is not None


# --- the report ----------------------------------------------------------------


def test_report_lists_each_region_and_a_total():
    counts = {
        "r2": {"orders": 5, "order_items": 9, "stock": 40, "product_availability": 2},
        "r1": {"orders": 7, "order_items": 12, "stock": 30, "product_availability": 0},
    }
    assert format_report(counts) == "\n".join(
        [
            "r1: orders 7, order_items 12, stock 30, product_availability 0",
            "r2: orders 5, order_items 9, stock 40, product_availability 2",
            "total: orders 12, order_items 21, stock 70, product_availability 2",
        ]
    )


def test_report_covers_every_synced_table():
    counts = {"r1": {table: 1 for table in ("orders", "order_items", "stock")}}
    assert "product_availability 0" in format_report(counts)


# --- the CLI --------------------------------------------------------------------


def test_baseline_help_works(capsys):
    for args in (["baseline", "--help"], ["baseline", "sync", "--help"]):
        with pytest.raises(SystemExit) as caught:
            app(args)
        assert caught.value.code == 0, args


def test_baseline_needs_a_subcommand(capsys):
    with pytest.raises(SystemExit) as caught:
        app(["baseline"])
    assert caught.value.code == 2


def test_sync_refusal_is_printed_as_a_message(capsys, monkeypatch):
    def refuse(cluster, settings, **kwargs):
        raise BaselineSyncError(
            "refusing to sync the baseline: playback run 'probe-1' is running "
            "with 0 request(s) still in flight"
        )

    monkeypatch.setattr("restaurantos.sim.baseline_sync.sync", refuse)
    assert app(["baseline", "sync"]) == 1
    err = capsys.readouterr().err
    assert "rosctl baseline sync:" in err
    assert "probe-1" in err
    assert "Traceback" not in err


def test_sync_prints_the_report_and_exits_zero(capsys, monkeypatch):
    monkeypatch.setattr(
        "restaurantos.sim.baseline_sync.sync",
        lambda cluster, settings, **kwargs: {"r1": {"orders": 3, "order_items": 4, "stock": 5}},
    )
    assert app(["baseline", "sync"]) == 0
    out = capsys.readouterr().out
    assert "r1: orders 3, order_items 4, stock 5, product_availability 0" in out
    assert "total: orders 3, order_items 4, stock 5, product_availability 0" in out

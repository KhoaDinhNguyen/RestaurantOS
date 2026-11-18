"""The baseline sync's quiet gate (both reads), the re-check before
commit, the row-count report, and the CLI wiring. The copy itself is
tested against the real generated cluster
(tests/cluster/test_baseline_sync.py); here everything runs against a
faked control endpoint, the in-process control app, or fake database
connections."""

from __future__ import annotations

import socket

import httpx
import psycopg
import pytest
from conftest import FakeWallClock, make_playback, sample_orders

from restaurantos.common.cluster import BASELINE, Cluster, host_port
from restaurantos.sim import baseline_sync
from restaurantos.sim.baseline_sync import (
    SYNC_TABLES,
    BaselineSyncError,
    _control_client,
    failed_sends_warning,
    format_report,
    gate_recheck,
    playback_refusal,
    sync,
)
from restaurantos.sim.cli import app
from restaurantos.sim.config import SimSettings
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


@pytest.mark.parametrize("bad_state", [["running"], {"a": 1}])
def test_a_non_string_state_is_a_refusal_not_a_crash(bad_state):
    """A list or dict state must not TypeError the gate (a regression
    the round 1 refactor introduced); it is an unknown state."""
    body = {"state": bad_state, "run_id": "probe", "inflight": 0}
    with (
        status_client(body) as client,
        pytest.raises(BaselineSyncError) as caught,
    ):
        baseline_sync.read_playback_status(client)
    assert "unknown state" in str(caught.value)
    with status_client(body) as client:
        refusal = playback_refusal(client)
    assert "unknown state" in refusal


def test_a_bad_in_flight_count_refuses():
    with status_client({"state": "idle", "run_id": "probe", "inflight": "many"}) as client:
        refusal = playback_refusal(client)
    assert refusal is not None
    assert "in-flight" in refusal


def test_a_missing_status_refuses():
    with status_client({}) as client:
        assert playback_refusal(client) is not None


def test_the_gate_client_ignores_the_proxy_environment(monkeypatch):
    """A dead HTTP_PROXY/ALL_PROXY fails with ECONNREFUSED, which the
    gate would misread as "no playback process = quiet". The client must
    not honour the environment."""
    recorded: dict = {}
    real_client = httpx.Client

    def spy(**kwargs):
        recorded.update(kwargs)
        return real_client(**kwargs)

    monkeypatch.setattr(baseline_sync.httpx, "Client", spy)
    _control_client(SimSettings(_env_file=None))
    assert recorded["trust_env"] is False


# --- the second gate read -------------------------------------------------------


def _status(**overrides) -> dict:
    body = {
        "state": "idle",
        "run_id": "run-1",
        "inflight": 0,
        "orders_sent": 10,
        "orders_acked": 9,
        "orders_parked": 0,
        "orders_refused": 1,
        "orders_failed": 0,
    }
    body.update(overrides)
    return body


def test_recheck_passes_when_nothing_changed():
    assert gate_recheck(None, None) is None
    assert gate_recheck(_status(), _status()) is None


def test_recheck_aborts_when_a_playback_process_appeared():
    message = gate_recheck(None, _status())
    assert message is not None
    assert "appeared" in message
    assert "left as it was" in message


def test_recheck_aborts_when_the_playback_process_disappeared():
    message = gate_recheck(_status(), None)
    assert message is not None
    assert "disappeared" in message
    assert "'run-1'" in message


def test_recheck_aborts_when_the_run_changed():
    message = gate_recheck(_status(), _status(run_id="run-2"))
    assert message is not None
    assert "'run-1' -> 'run-2'" in message


def test_recheck_aborts_when_a_counter_advanced():
    message = gate_recheck(_status(), _status(orders_sent=11, orders_acked=10))
    assert message is not None
    assert "orders_sent 10 -> 11" in message
    assert "orders_acked 9 -> 10" in message


def test_recheck_aborts_when_only_status_events_advanced():
    """The extras PR adds events_* and restocks_* counters; both kinds
    of request write to the regions, so they abort the sync even with
    no orders_* change."""
    message = gate_recheck(_status(events_sent=5), _status(events_sent=6))
    assert message is not None
    assert "events_sent 5 -> 6" in message


def test_recheck_aborts_when_only_a_restock_landed():
    message = gate_recheck(_status(restocks_acked=0), _status(restocks_acked=1))
    assert message is not None
    assert "restocks_acked 0 -> 1" in message


def test_recheck_aborts_when_a_counter_appeared_or_vanished():
    appeared = gate_recheck(_status(), _status(events_sent=3))
    assert appeared is not None
    assert "events_sent missing -> 3" in appeared
    vanished = gate_recheck(_status(events_sent=3), _status())
    assert vanished is not None
    assert "events_sent 3 -> missing" in vanished


def test_recheck_aborts_when_the_second_read_is_not_quiet():
    """State running, counters unchanged: the not-quiet branch, not the
    counter branch (a changed inflight is caught as a counter first)."""
    message = gate_recheck(_status(), _status(state="running"))
    assert message is not None
    assert "running" in message
    assert "left as it was" in message


def test_no_warning_for_a_clean_or_absent_body():
    assert failed_sends_warning(None) is None
    assert failed_sends_warning(_status()) is None


def test_warning_names_every_failed_and_parked_count():
    body = _status(orders_failed=2, restocks_failed=1, orders_parked=5)
    message = failed_sends_warning(body)
    assert message is not None
    assert "orders_failed 2" in message
    assert "restocks_failed 1" in message
    assert "orders_parked 5" in message
    assert "healed" in message


def test_warning_ignores_zero_missing_and_non_int_counts():
    assert failed_sends_warning(_status(orders_failed=0)) is None
    assert failed_sends_warning(_status(orders_parked=0)) is None
    assert failed_sends_warning(_status(restocks_failed="many")) is None
    assert failed_sends_warning({"state": "idle"}) is None


# --- sync, end to end against fake databases ------------------------------------


class _FakeCopy:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        if not getattr(self, "_served", False):
            self._served = True
            return b"one chunk of rows"
        return b""

    def write(self, data):
        pass


class _FakeCursor:
    def __init__(self):
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def copy(self, statement):
        if "FROM STDIN" in statement:
            self.rowcount = 3
        return _FakeCopy()


class _FakeConnection:
    def __init__(self, label: str, commit_error: Exception | None = None):
        self.label = label
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.commit_error = commit_error

    def execute(self, *args, **kwargs):
        return _FakeCursor()

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        if self.commit_error is not None:
            raise self.commit_error
        self.committed = True

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc):
        # like psycopg: leaving the with-block closes the connection
        self.closed = True
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False


def _fake_databases(
    monkeypatch, cluster: Cluster, baseline_commit_error: Exception | None = None
) -> list[_FakeConnection]:
    connections: list[_FakeConnection] = []
    baseline_port = f"port={host_port(cluster, BASELINE)}"

    def connect(dsn, **kwargs):
        if baseline_port in dsn:
            connection = _FakeConnection("baseline", commit_error=baseline_commit_error)
        else:
            connection = _FakeConnection("region")
        connections.append(connection)
        return connection

    monkeypatch.setattr(baseline_sync.psycopg, "connect", connect)
    return connections


def _two_reads(first: dict | None, second: dict | int | None) -> httpx.MockTransport:
    """Serve `first` on the gate's first read and `second` on the second:
    a dict is a 200 status body, an int is that HTTP status code."""
    bodies = iter([first, second])

    def handler(request):
        body = next(bodies)
        if isinstance(body, int):
            return httpx.Response(body)
        return httpx.Response(200, json=body)

    return httpx.MockTransport(handler)


def _cluster() -> Cluster:
    return Cluster(regions=1, standbys=False)


def test_sync_with_an_unchanged_gate_commits(monkeypatch):
    cluster = _cluster()
    connections = _fake_databases(monkeypatch, cluster)
    counts, warning = sync(
        cluster,
        SimSettings(_env_file=None),
        transport=_two_reads(_status(), _status()),
    )
    baseline = [c for c in connections if c.label == "baseline"][0]
    assert baseline.committed and not baseline.rolled_back
    assert all(c.closed for c in connections), "every connection must be closed"
    assert counts == {"r1": {table: 3 for table in SYNC_TABLES}}
    assert warning is None


def test_sync_returns_the_failed_sends_warning(monkeypatch):
    """Failed and parked counters do not refuse the sync; the warning is
    returned for the CLI to print."""
    cluster = _cluster()
    _fake_databases(monkeypatch, cluster)
    body = _status(orders_failed=2, orders_parked=1)
    counts, warning = sync(
        cluster,
        SimSettings(_env_file=None),
        transport=_two_reads(body, body),
    )
    assert counts == {"r1": {table: 3 for table in SYNC_TABLES}}
    assert warning is not None
    assert "orders_failed 2" in warning
    assert "orders_parked 1" in warning


def test_a_commit_failure_reports_an_unknown_outcome(monkeypatch):
    """An error from the commit itself cannot claim a rollback: the
    baseline holds either its old rows or the new copy. The message
    says unknown, and the region connections are still closed."""
    cluster = _cluster()
    connections = _fake_databases(
        monkeypatch, cluster, baseline_commit_error=psycopg.OperationalError("connection lost")
    )
    with pytest.raises(BaselineSyncError) as caught:
        sync(
            cluster,
            SimSettings(_env_file=None),
            transport=_two_reads(_status(), _status()),
        )
    message = str(caught.value)
    assert "unknown" in message
    assert "rolled back" not in message
    assert "safe to repeat" in message
    regions = [c for c in connections if c.label == "region"]
    assert regions and all(c.closed for c in regions)


def test_sync_aborts_and_rolls_back_when_playback_advanced_during_the_copy(monkeypatch):
    cluster = _cluster()
    connections = _fake_databases(monkeypatch, cluster)
    with pytest.raises(BaselineSyncError) as caught:
        sync(
            cluster,
            SimSettings(_env_file=None),
            transport=_two_reads(_status(), _status(orders_sent=11)),
        )
    assert "orders_sent 10 -> 11" in str(caught.value)
    assert "left as it was" in str(caught.value)
    baseline = [c for c in connections if c.label == "baseline"][0]
    assert baseline.rolled_back and not baseline.committed


def test_a_failed_second_read_rolls_back_and_says_so(monkeypatch):
    """A timeout, 503 or bad body on the second read aborts the sync;
    the user must be told the baseline was left as it was, because the
    transaction did roll back."""
    cluster = _cluster()
    connections = _fake_databases(monkeypatch, cluster)
    with pytest.raises(BaselineSyncError) as caught:
        sync(
            cluster,
            SimSettings(_env_file=None),
            transport=_two_reads(_status(), 503),
        )
    assert "HTTP 503" in str(caught.value)
    assert "left as it was" in str(caught.value)
    baseline = [c for c in connections if c.label == "baseline"][0]
    assert baseline.rolled_back and not baseline.committed


def test_sync_refuses_before_touching_any_database(monkeypatch):
    def no_connect(*args, **kwargs):
        raise AssertionError("the gate refused, so no connection may be opened")

    monkeypatch.setattr(baseline_sync.psycopg, "connect", no_connect)
    with pytest.raises(BaselineSyncError) as caught:
        sync(
            _cluster(),
            SimSettings(_env_file=None),
            transport=_two_reads(_status(state="running", inflight=4), None),
        )
    assert str(caught.value).startswith("refusing to sync the baseline:")
    assert "'run-1'" in str(caught.value)


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
        lambda cluster, settings, **kwargs: (
            {"r1": {"orders": 3, "order_items": 4, "stock": 5}},
            None,
        ),
    )
    assert app(["baseline", "sync"]) == 0
    out = capsys.readouterr().out
    assert "r1: orders 3, order_items 4, stock 5, product_availability 0" in out
    assert "total: orders 3, order_items 4, stock 5, product_availability 0" in out


def test_sync_prints_the_failed_sends_warning_to_stderr(capsys, monkeypatch):
    monkeypatch.setattr(
        "restaurantos.sim.baseline_sync.sync",
        lambda cluster, settings, **kwargs: (
            {"r1": {table: 3 for table in SYNC_TABLES}},
            "warning: the last playback run counted sends that may not be settled "
            "(orders_failed 2); sync once faults are healed",
        ),
    )
    assert app(["baseline", "sync"]) == 0
    captured = capsys.readouterr()
    assert "orders_failed 2" in captured.err
    assert "warning" not in captured.out
    assert "total:" in captured.out

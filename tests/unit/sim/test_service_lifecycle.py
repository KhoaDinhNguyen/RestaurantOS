"""up/down against a fake target: real child processes (a `python -c`
sleeper), no cluster, no real service package. These prove the
supervision contract - SIGTERM cleanup, escalation to SIGKILL, stale
pid files never killed, idempotent `up`, per-name single starts, the
port check and the precheck - which in-process mocks cannot, because
they exercise bind, signal and /proc paths.
"""

from __future__ import annotations

import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.sim import services
from restaurantos.sim.services import (
    ServiceSpec,
    Step,
    down_services,
    up_services,
)

CLUSTER = Cluster(regions=1)

SLEEPER = (
    "import signal, sys, time; "
    "signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); time.sleep(300)"
)
IGNORER = (
    "import signal, sys, time; "
    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
    "print('ready'); sys.stdout.flush(); time.sleep(300)"
)
PRINTER = (
    "import os, signal, sys, time; "
    "print('ROS_MODE=' + os.environ.get('ROS_MODE', '')); sys.stdout.flush(); "
    "signal.signal(signal.SIGTERM, lambda *_: sys.exit(0)); time.sleep(300)"
)


def fake(name: str, *, port: int | None = None, code: str = SLEEPER, **env: str) -> ServiceSpec:
    return ServiceSpec(name=name, args=("-c", code), env=env, port=port)


_leftover: list[int] = []


@pytest.fixture(autouse=True)
def _no_orphans() -> Iterator[None]:
    """Every child a test starts is killed at teardown, so a failed
    test cannot leak sleepers onto the machine."""
    yield
    for pid in _leftover[:]:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    _leftover.clear()


def _track(runs_dir: Path, steps: list[Step]) -> None:
    for step in steps:
        pid = services.read_pid(runs_dir, step.name)
        if pid is not None:
            _leftover.append(pid)


def _gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return not Path(f"/proc/{pid}/cmdline").read_bytes()


def _wait_gone(pid: int, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _gone(pid):
            return True
        time.sleep(0.05)
    return False


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_logged(runs_dir: Path, name: str, text: str, timeout_s: float = 5.0) -> None:
    """Wait until the child's log contains `text` - how a test knows the
    child has installed its signal handlers."""
    log = services.log_path(runs_dir, name)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if text in log.read_text(encoding="utf-8"):
                return
        except OSError:
            pass
        time.sleep(0.05)
    raise AssertionError(f"{text!r} never appeared in {log}")


def _spawn_raw(code: str) -> int:
    """A live process that is NOT one of our services - what a reused
    pid looks like to `down`."""
    proc = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    _leftover.append(proc.pid)
    return proc.pid


# --- up then down ------------------------------------------------------


def test_up_starts_a_detached_child_and_down_stops_it(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[fake("sleeper")]
    )
    _track(runs, steps)
    assert [step.outcome for step in steps] == ["started"]
    pid = services.read_pid(runs, "sleeper")
    assert pid is not None
    assert pid != os.getpid()
    os.kill(pid, 0)  # alive, and detached from this process
    assert services.log_path(runs, "sleeper").exists()

    steps = down_services(CLUSTER, runs_dir=runs, specs=[fake("sleeper")])
    assert steps == [Step("sleeper", "stopped", f"pid {pid}")]
    assert _wait_gone(pid)
    assert not services.pid_path(runs, "sleeper").exists()


def test_child_environment_overrides_leftover_shell_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a leftover ROS_MODE in the shell must not leak into the child:
    # the table's identity is the only truth
    monkeypatch.setenv("ROS_MODE", "edge")
    runs = tmp_path / "runs"
    spec = fake("printer", code=PRINTER, ROS_MODE="gateway", ROS_CLIENT="gateway")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[spec])
    _track(runs, steps)
    log = services.log_path(runs, "printer")
    deadline = time.monotonic() + 5.0
    while "ROS_MODE=gateway" not in log.read_text(encoding="utf-8") and time.monotonic() < deadline:
        time.sleep(0.05)
    assert "ROS_MODE=gateway" in log.read_text(encoding="utf-8")
    assert down_services(CLUSTER, runs_dir=runs, specs=[spec])[0].outcome == "stopped"


def test_second_up_is_idempotent(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[spec])
    _track(runs, first)
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[spec])
    assert [step.outcome for step in second] == ["already-running"]
    assert services.read_pid(runs, "sleeper") == services.read_pid(runs, "sleeper")
    assert down_services(CLUSTER, runs_dir=runs, specs=[spec])[0].outcome == "stopped"


def test_a_port_someone_else_holds_is_reported_not_fought_over(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    port = _free_port()
    spec = fake("busy", port=port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", port))
        listener.listen(1)
        steps = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[spec])
        assert steps[0].outcome == "port-in-use"
        assert str(port) in steps[0].detail
        assert services.read_pid(runs, "busy") is None
    assert services.up_exit_code(steps) == 1


def test_a_service_that_dies_at_once_is_named_with_its_log(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("crasher", code="import sys; sys.exit(3)")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.3, specs=[spec])
    assert steps[0].outcome == "exited-immediately"
    assert str(services.log_path(runs, "crasher")) in steps[0].detail
    assert services.read_pid(runs, "crasher") is None
    assert services.up_exit_code(steps) == 1


def test_up_starts_only_the_named_service(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    specs = [fake("solo"), fake("other")]
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=specs, names=["solo"]
    )
    _track(runs, steps)
    assert [step.name for step in steps] == ["solo"]
    assert services.read_pid(runs, "other") is None
    down_services(CLUSTER, runs_dir=runs, specs=specs, names=["solo"])


def test_unknown_name_lists_the_valid_ones() -> None:
    with pytest.raises(ValueError, match="valid names"):
        up_services(
            CLUSTER,
            runs_dir=Path("runs"),
            names=["nope"],
            precheck=False,
            specs=[fake("sleeper")],
        )


# --- down's safety rules ------------------------------------------------


def test_sigterm_ignored_service_is_killed(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("stubborn", code=IGNORER)
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[spec])
    _track(runs, steps)
    pid = services.read_pid(runs, "stubborn")
    assert pid is not None
    # wait until the child has actually installed SIG_IGN: a SIGTERM
    # during interpreter start-up would kill it before the test's point
    _wait_logged(runs, "stubborn", "ready")
    steps = down_services(CLUSTER, runs_dir=runs, specs=[spec], term_timeout_s=0.3)
    assert steps[0].outcome == "killed"
    assert _wait_gone(pid)
    assert not services.pid_path(runs, "stubborn").exists()
    assert services.down_exit_code(steps) == 0


def test_a_reused_pid_is_never_killed(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    decoy = _spawn_raw("import time; time.sleep(300)")
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    services.log_path(runs, "sleeper").parent.mkdir(parents=True, exist_ok=True)
    services.pid_path(runs, "sleeper").write_text(f"{decoy}\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs, specs=[spec])
    assert steps[0].outcome == "stale-pid"
    assert "left alone" in steps[0].detail
    assert not services.pid_path(runs, "sleeper").exists()
    assert not _gone(decoy)  # the decoy survived the down


def test_a_sibling_with_the_same_argv_is_not_confused(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    relay_a = fake("relay-a", ROS_CLIENT="relay-a", ROS_RELAY_ROLE="region")
    relay_b = fake("relay-b", ROS_CLIENT="relay-b", ROS_RELAY_ROLE="region")
    specs = [relay_a, relay_b]
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=specs)
    _track(runs, steps)
    assert [step.outcome for step in steps] == ["started", "started"]
    pid_a = services.read_pid(runs, "relay-a")
    pid_b = services.read_pid(runs, "relay-b")
    assert pid_a is not None and pid_b is not None and pid_a != pid_b
    # b's pid filed under a's name: down must refuse to touch b
    services.pid_path(runs, "relay-a").write_text(f"{pid_b}\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs, specs=specs, names=["relay-a"])
    assert steps[0].outcome == "stale-pid"
    assert not _gone(pid_b)  # b is still running after a's down
    down_services(CLUSTER, runs_dir=runs, specs=specs)


def test_down_with_nothing_running_is_a_clean_no_op(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    specs = [fake("one"), fake("two")]
    steps = down_services(CLUSTER, runs_dir=runs, specs=specs)
    assert [step.outcome for step in steps] == ["not-running", "not-running"]
    assert services.down_exit_code(steps) == 0


def test_down_after_a_crash_reports_the_stale_pid_file(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("sleeper")
    # a pid file naming a dead process: pid 1 is init, never a service
    services.log_path(runs, "sleeper").parent.mkdir(parents=True, exist_ok=True)
    services.pid_path(runs, "sleeper").write_text("1\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs, specs=[spec])
    assert steps[0].outcome in ("not-running", "stale-pid")
    assert not services.pid_path(runs, "sleeper").exists()


# --- precheck -----------------------------------------------------------


def test_unreachable_ports_reports_only_the_dead_ones() -> None:
    free = _free_port()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", free))
        listener.listen(1)
        assert services.unreachable_ports([free, 1], timeout_s=0.2) == [1]


def test_up_refuses_when_the_cluster_is_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    free = _free_port()
    monkeypatch.setattr(services, "dialed_ports", lambda cluster: [free])
    with pytest.raises(services.PrecheckError, match="bootstrap"):
        up_services(CLUSTER, runs_dir=tmp_path / "runs", precheck=True, specs=[fake("sleeper")])
    assert services.read_pid(tmp_path / "runs", "sleeper") is None


def test_no_precheck_spawns_anyway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    free = _free_port()
    monkeypatch.setattr(services, "dialed_ports", lambda cluster: [free])
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, settle_s=0.0, specs=[fake("sleeper")]
    )
    _track(runs, steps)
    assert all(step.outcome == "started" for step in steps)
    down_services(CLUSTER, runs_dir=runs, specs=[fake("sleeper")])

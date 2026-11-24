"""up/down against a fake target: real child processes (a `python -c`
sleeper or HTTP server, or a shadowed restaurantos package), no cluster,
no real service code. These prove the supervision contract - SIGINT
cleanup, escalation to SIGKILL, readiness waits (a never-ready child
is stopped, not orphaned), stale and mismatched pid files never
killed, idempotent `up`, per-name single starts and locks, the port
check, the precheck of exactly the selected services' dials, the
/proc platform gate, malformed pid files and launch failures - which
in-process mocks cannot, because they exercise bind, signal, pidfd
and /proc paths.

Every test that spawns a child or reads /proc is skipped where there
is no /proc (the platform-refusal test and the pure helper tests are
not: they monkeypatch or avoid it on purpose)."""

from __future__ import annotations

import contextlib
import dataclasses
import os
import signal
import socket
import subprocess
import sys
import threading
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

# After installing its SIGINT handler, a -c fake drops a marker file
# named by its pid into ROS_TEST_READY_DIR (when set): a SIGINT that
# reaches a python child during interpreter start-up is swallowed by
# the default handler inside import machinery, so the stop tests wait
# for this marker before they signal (see _wait_ready).
READY_MARK = (
    "_d = os.environ.get('ROS_TEST_READY_DIR'); "
    "open(os.path.join(_d, str(os.getpid())), 'w') if _d else None; "
)

SLEEPER = (
    "import os, signal, sys, time; "
    "signal.signal(signal.SIGINT, lambda *_: sys.exit(0)); " + READY_MARK + "time.sleep(300)"
)
IGNORER = (
    "import signal, sys, time; "
    "signal.signal(signal.SIGINT, signal.SIG_IGN); "
    "print('ready'); sys.stdout.flush(); time.sleep(300)"
)
PRINTER = (
    "import os, signal, sys, time; "
    "print('ROS_MODE=' + os.environ.get('ROS_MODE', '')); sys.stdout.flush(); "
    "signal.signal(signal.SIGINT, lambda *_: sys.exit(0)); " + READY_MARK + "time.sleep(300)"
)

# A minimal HTTP server on PORT that answers 200 on the health path it
# was given and 404 everywhere else. It records the first request
# arrival as a 'hit' file (so a test can prove the probe really
# connected), then optionally sleeps before answering, and optionally
# sleeps before it binds.
HTTP_SERVER = """import os, time
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        _d = os.environ.get('ROS_TEST_READY_DIR')
        if _d:
            open(os.path.join(_d, 'hit'), 'w')
        {answer_delay}
        if self.path == {health!r}:
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *a): pass
{delay}
HTTPServer(("127.0.0.1", {port}), H).serve_forever()"""

# A fake restaurantos package: `python -m restaurantos.relay` (the exact
# argv of the real table) runs this sleeper instead of the real relay.
# Used to prove the pid-file-driven `down`, which rebuilds specs from
# names and so checks the canonical argv.
SHADOW_MODULE = (
    "import os, signal, sys, time\n"
    "signal.signal(signal.SIGINT, lambda *_: sys.exit(0))\n"
    "_d = os.environ.get('ROS_TEST_READY_DIR')\n"
    "if _d:\n"
    "    open(os.path.join(_d, str(os.getpid())), 'w')\n"
    "time.sleep(300)\n"
)

requires_proc = pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs Linux /proc")


def fake(
    name: str,
    *,
    port: int | None = None,
    ready: str | None = None,
    code: str = SLEEPER,
    **env: str,
) -> ServiceSpec:
    return ServiceSpec(name=name, args=("-c", code), env=env, port=port, ready=ready)


_leftover: list[tuple[int, int]] = []


@pytest.fixture(autouse=True)
def _no_orphans() -> Iterator[None]:
    """Every child a test starts is killed at teardown, so a failed
    test cannot leak sleepers onto the machine. The kill goes through
    a pidfd and only when the start ticks still match - the same
    guard `down` uses (A1), so teardown can never hit a recycled pid."""
    yield
    for pid, ticks in _leftover[:]:
        try:
            fd = os.pidfd_open(pid)
        except OSError:
            continue  # already gone
        try:
            if services._start_ticks(pid) == ticks:
                with contextlib.suppress(ProcessLookupError):
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
        except OSError, IndexError, ValueError:
            pass  # exited while we were looking: gone is the goal
        finally:
            os.close(fd)
    _leftover.clear()


@pytest.fixture
def ready_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The folder -c fakes drop their ready marker in once their SIGINT
    handler is installed. The stop tests wait for it: a SIGINT that
    reaches a python child during interpreter start-up is swallowed by
    the default handler inside import machinery, the child then
    installs its handler and never sees another signal - and the test
    would see `killed` after a 10 s escalation instead of `stopped`
    (this is the CI failure of round 4)."""
    folder = tmp_path / "ready"
    folder.mkdir()
    monkeypatch.setenv("ROS_TEST_READY_DIR", str(folder))
    return folder


@pytest.fixture
def shadow_services(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ready_dir: Path) -> None:
    """A restaurantos package on PYTHONPATH that shadows the real one:
    the canonical `-m restaurantos.<component>` argv runs a sleeper.
    The fake also drops a ready marker (see `ready_dir`)."""
    pkg = tmp_path / "shadow"
    (pkg / "restaurantos").mkdir(parents=True)
    (pkg / "restaurantos" / "__init__.py").write_text("", encoding="utf-8")
    for module in ("relay", "gateway", "coordinator"):
        (pkg / "restaurantos" / f"{module}.py").write_text(SHADOW_MODULE, encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(pkg))


def _track(steps: list[Step]) -> None:
    """Remember every spawned child by (pid, start_ticks) taken from
    the STEP, not the pid file: after a failure the pid file is gone,
    but the step still names the process it spawned."""
    for step in steps:
        if step.pid is None:
            continue
        with contextlib.suppress(OSError, IndexError, ValueError):
            _leftover.append((step.pid, services._start_ticks(step.pid)))  # else: already gone


def _gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    try:
        # the state, not the cmdline: the kernel releases the cmdline
        # BEFORE the state becomes Z, so an empty cmdline can mean
        # "dying" rather than "gone" for a just-killed child
        state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        return state.rpartition(")")[2].strip().split(" ")[0] in ("Z", "X", "x")
    except OSError:
        return True


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
    child has installed its signal handlers. Header lines (starting
    with '# ') are ignored: the header records the whole command, so a
    marker word in a fake target's code would match its own header
    before the child ever ran."""
    log = services.log_path(runs_dir, name)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            body = "\n".join(
                line
                for line in log.read_text(encoding="utf-8").splitlines()
                if not line.startswith("# ")
            )
            if text in body:
                return
        except OSError:
            pass
        time.sleep(0.05)
    raise AssertionError(f"{text!r} never appeared in {log}")


def _wait_ready(steps: list[Step], ready: Path, timeout_s: float = 10.0) -> None:
    """Block until every fake this test started has dropped its ready
    marker (its SIGINT handler is installed); fail naming the services
    still missing if it never happens."""
    wanted = {step.pid: step.name for step in steps if step.outcome == "started" and step.pid}
    deadline = time.monotonic() + timeout_s
    missing: list[str] = []
    while time.monotonic() < deadline:
        found = {int(path.name) for path in ready.glob("*") if path.name.isdigit()}
        missing = [name for pid, name in wanted.items() if pid not in found]
        if not missing:
            return
        time.sleep(0.05)
    raise AssertionError(f"never became ready: {', '.join(missing)}")


def _spawn_raw(code: str) -> int:
    """A live process that is NOT one of our services - what a reused
    pid looks like to `down`."""
    proc = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    _leftover.append((proc.pid, services._start_ticks(proc.pid)))
    return proc.pid


def _write_pid_file(runs: Path, name: str, pid: int, ticks: int) -> None:
    services.pid_path(runs, name).parent.mkdir(parents=True, exist_ok=True)
    services._write_pid_file(services.pid_path(runs, name), pid, ticks)


def _http_fake(
    name: str,
    *,
    health: str,
    delay_s: float = 0.0,
    bad_health: bool = False,
    answer_delay_s: float = 0.0,
) -> tuple[ServiceSpec, int]:
    """A fake HTTP service: answers 200 on HEALTH (or on /other when
    bad_health, so /health gets its 404). DELAY_S sleeps before the
    bind; ANSWER_DELAY_S sleeps after a request arrives, before it is
    answered - the slow-answer case."""
    port = _free_port()
    served = "/other" if bad_health else health
    code = HTTP_SERVER.format(
        health=served,
        delay=f"time.sleep({delay_s})" if delay_s else "pass",
        answer_delay=f"time.sleep({answer_delay_s})" if answer_delay_s else "pass",
        port=port,
    )
    return fake(name, port=port, ready=health, code=code), port


# --- up then down ------------------------------------------------------


@requires_proc
def test_up_starts_a_detached_child_and_down_stops_it(tmp_path: Path, ready_dir: Path) -> None:
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[fake("sleeper")]
    )
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert [step.outcome for step in steps] == ["started"]
    pid = steps[0].pid
    assert pid is not None
    assert pid != os.getpid()
    os.kill(pid, 0)  # alive, and detached from this process
    assert services.log_path(runs, "sleeper").exists()

    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[fake("sleeper")])
    assert steps == [Step("sleeper", "stopped", f"pid {pid}")]
    assert _wait_gone(pid)
    assert not services.pid_path(runs, "sleeper").exists()


@requires_proc
def test_the_log_carries_one_header_line_per_start(tmp_path: Path, ready_dir: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_MODE="gateway")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(steps)
    _wait_ready(steps, ready_dir)
    header = services.log_path(runs, "sleeper").read_text(encoding="utf-8").splitlines()[0]
    assert header.startswith("# ")
    assert "start sleeper" in header
    assert "ROS_MODE=gateway" in header
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])


@requires_proc
def test_child_environment_overrides_leftover_shell_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ready_dir: Path
) -> None:
    # a leftover ROS_MODE in the shell must not leak into the child:
    # the table's identity is the only truth
    monkeypatch.setenv("ROS_MODE", "edge")
    runs = tmp_path / "runs"
    spec = fake("printer", code=PRINTER, ROS_MODE="gateway", ROS_CLIENT="gateway")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(steps)
    _wait_ready(steps, ready_dir)
    log = services.log_path(runs, "printer")
    deadline = time.monotonic() + 5.0
    body = ""
    while time.monotonic() < deadline:
        body = "\n".join(
            line
            for line in log.read_text(encoding="utf-8").splitlines()
            if not line.startswith("# ")
        )
        if "ROS_MODE=gateway" in body:
            break
        time.sleep(0.05)
    # the header echoes the command's identity env, so only the body
    # counts as the child's own evidence
    assert "ROS_MODE=gateway" in body
    assert (
        down_services(CLUSTER, runs_dir=runs, names=["printer"], specs=[spec])[0].outcome
        == "stopped"
    )


@requires_proc
def test_second_up_is_idempotent(tmp_path: Path, ready_dir: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(first)
    _wait_ready(first, ready_dir)
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert [step.outcome for step in second] == ["already-running"]
    assert (
        down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])[0].outcome
        == "stopped"
    )


@requires_proc
def test_a_port_someone_else_holds_is_reported_not_fought_over(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    port = _free_port()
    spec = fake("busy", port=port, ready="/health")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", port))
        listener.listen(1)
        steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
        assert steps[0].outcome == "port-in-use"
        assert str(port) in steps[0].detail
        assert services.read_pid_record(runs, "busy") is None
    assert services.up_exit_code(steps) == 1


@requires_proc
def test_two_starts_of_one_name_start_one_process(tmp_path: Path, ready_dir: Path) -> None:
    # the lock file serializes two rosctl ups racing on one name: the
    # second waits and then sees the first's pid file
    runs = tmp_path / "runs"
    spec = fake("sleeper")
    results: list[Step] = []

    def starter() -> None:
        results.append(services.start_service(spec, runs_dir=runs))

    threads = [threading.Thread(target=starter) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    _track(results)
    _wait_ready(results, ready_dir)
    assert sorted(step.outcome for step in results) == ["already-running", "started"]
    assert isinstance(services.read_pid_record(runs, "sleeper"), tuple)
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])


# --- readiness waits ----------------------------------------------------


@requires_proc
def test_a_just_spawned_child_is_alive_even_inside_exec(tmp_path: Path) -> None:
    # regression for the exec race: right after Popen returns, the
    # child can still be mid-execve with an empty /proc cmdline - a
    # naive "empty cmdline means dead" check reported the service
    # exited-immediately and orphaned it
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    _leftover.append((proc.pid, services._start_ticks(proc.pid)))
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        assert services._alive(proc.pid), "a just-spawned child must count as alive"
        if Path(f"/proc/{proc.pid}/cmdline").read_bytes():
            break  # exec finished; the window has been crossed
        time.sleep(0.01)


@requires_proc
def test_a_zombie_counts_as_dead() -> None:
    # kill(pid, 0) answers for a zombie (exited, not yet reaped); the
    # state in /proc/<pid>/stat is what tells it from a live process
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        state = Path(f"/proc/{proc.pid}/stat").read_text(encoding="utf-8")
        if state.rpartition(")")[2].strip().split(" ")[0] == "Z":
            break
        time.sleep(0.01)
    assert not services._alive(proc.pid)
    proc.wait()


@requires_proc
def test_up_waits_for_the_readiness_route_and_accepts_a_slow_binder(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    spec, port = _http_fake("slow", health="/health", delay_s=2.0)
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        ready_timeout_s=10.0,
        specs=[spec],
    )
    _track(steps)
    assert steps[0].outcome == "started"
    assert f"ready on port {port}" in steps[0].detail
    assert (
        down_services(CLUSTER, runs_dir=runs, names=["slow"], specs=[spec])[0].outcome == "stopped"
    )


@requires_proc
def test_a_service_that_never_answers_health_is_stopped_not_orphaned(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    spec, port = _http_fake("broken", health="/health", bad_health=True)
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        ready_timeout_s=1.0,
        specs=[spec],
    )
    _track(steps)
    assert steps[0].outcome == "not-ready"
    assert "still not answering /health" in steps[0].detail
    assert "the service was stopped" in steps[0].detail
    assert services.read_pid_record(runs, "broken") is None  # its pid file is cleaned
    assert steps[0].pid is not None
    assert _gone(steps[0].pid)  # ... and so is the process itself
    assert services.up_exit_code(steps) == 1


@requires_proc
def test_a_service_that_dies_inside_the_readiness_wait_is_named(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    spec = fake("midway", code="import sys, time; time.sleep(1.0); sys.exit(3)")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=3.0, specs=[spec])
    assert steps[0].outcome == "exited-immediately"
    assert services.read_pid_record(runs, "midway") is None
    assert services.up_exit_code(steps) == 1


@requires_proc
def test_a_relay_surviving_its_grace_period_is_started(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.5, specs=[fake("relay-fake")]
    )
    _track(steps)
    assert steps[0].outcome == "started"
    assert "no HTTP port" in steps[0].detail
    down_services(CLUSTER, runs_dir=runs, names=["relay-fake"], specs=[fake("relay-fake")])


@requires_proc
def test_a_service_that_dies_at_once_is_named_with_its_log(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("crasher", code="import sys; sys.exit(3)")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.5, specs=[spec])
    assert steps[0].outcome == "exited-immediately"
    assert str(services.log_path(runs, "crasher")) in steps[0].detail
    assert "the other services keep running" in steps[0].detail
    assert services.read_pid_record(runs, "crasher") is None
    assert services.up_exit_code(steps) == 1


@requires_proc
def test_an_unwritable_log_is_launch_failed_and_the_rest_still_start(
    tmp_path: Path, ready_dir: Path
) -> None:
    # one service's log cannot be opened: that service reports
    # launch-failed and the sweep carries on to the next one
    runs = tmp_path / "runs"
    services.log_path(runs, "slow").parent.mkdir(parents=True)
    services.log_path(runs, "slow").mkdir()  # a directory where the log file goes
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        specs=[fake("slow"), fake("next")],
    )
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert steps[0].outcome == "launch-failed"
    assert "could not write the service log" in steps[0].detail
    assert steps[1].outcome == "started"
    assert services.up_exit_code(steps) == 1
    down_services(CLUSTER, runs_dir=runs, names=["next"], specs=[fake("next")])


@requires_proc
def test_a_failed_pid_write_kills_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a child whose pid file cannot be written must not outlive the
    # record of it: it is killed and reaped, and nothing partial is
    # left on disk
    def exploding_replace(src: str, dst: str) -> None:
        raise OSError("no space left")

    monkeypatch.setattr(os, "replace", exploding_replace)
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[fake("sleeper")]
    )
    assert steps[0].outcome == "launch-failed"
    assert "could not write" in steps[0].detail
    assert services.read_pid_record(runs, "sleeper") is None
    assert not (runs / "services" / "sleeper.pid.tmp").exists()
    pid = steps[0].pid
    assert pid is not None
    assert _wait_gone(pid)  # killed and reaped through the Popen handle
    assert services.up_exit_code(steps) == 1


@requires_proc
def test_up_starts_only_the_named_services(tmp_path: Path, ready_dir: Path) -> None:
    runs = tmp_path / "runs"
    specs = [fake("solo"), fake("other")]
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        specs=specs,
        names=["solo"],
    )
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert [step.name for step in steps] == ["solo"]
    assert services.read_pid_record(runs, "other") is None
    down_services(CLUSTER, runs_dir=runs, names=["solo"], specs=specs)


def test_unknown_name_lists_the_valid_ones(monkeypatch: pytest.MonkeyPatch) -> None:
    # the platform check is patched so the name check is what runs,
    # on any platform
    monkeypatch.setattr(services, "_has_proc", lambda: True)
    with pytest.raises(ValueError, match="valid names"):
        up_services(
            CLUSTER,
            runs_dir=Path("runs"),
            names=["nope"],
            precheck=False,
            specs=[fake("sleeper")],
        )


# --- down's safety rules ------------------------------------------------


@requires_proc
def test_sigint_ignored_service_is_killed(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("stubborn", code=IGNORER)
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(steps)
    pid = steps[0].pid
    assert pid is not None
    # wait until the child has actually installed SIG_IGN: a SIGINT
    # during interpreter start-up would kill it before the test's point
    _wait_logged(runs, "stubborn", "ready")
    steps = down_services(
        CLUSTER, runs_dir=runs, names=["stubborn"], specs=[spec], stop_timeout_s=0.3
    )
    assert steps[0].outcome == "killed"
    assert "did not answer SIGINT" in steps[0].detail
    assert _wait_gone(pid)
    assert not services.pid_path(runs, "stubborn").exists()
    assert services.down_exit_code(steps) == 0


@requires_proc
def test_a_start_ticks_mismatch_is_never_signalled(tmp_path: Path, ready_dir: Path) -> None:
    # the pid, argv and environment all match the service - but the
    # recorded start time is one tick off, which is what a REUSED pid
    # looks like: the first proof fails and the process is left alone
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(steps)
    _wait_ready(steps, ready_dir)
    pid = steps[0].pid
    assert pid is not None
    services._write_pid_file(
        services.pid_path(runs, "sleeper"), pid, services._start_ticks(pid) + 1
    )
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])
    assert steps[0].outcome == "stale-pid"
    assert not _gone(pid)  # the live service was left alone
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])


@requires_proc
def test_a_reused_pid_is_never_killed(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    decoy = _spawn_raw("import time; time.sleep(300)")
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    _write_pid_file(runs, "sleeper", decoy, services._start_ticks(decoy))
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])
    assert steps[0].outcome == "stale-pid"
    assert "left alone" in steps[0].detail
    assert not services.pid_path(runs, "sleeper").exists()
    assert not _gone(decoy)  # the decoy survived the down


@requires_proc
def test_a_sibling_with_the_same_argv_is_not_confused(tmp_path: Path, ready_dir: Path) -> None:
    runs = tmp_path / "runs"
    relay_a = fake("relay-a", ROS_CLIENT="relay-a", ROS_RELAY_ROLE="region")
    relay_b = fake("relay-b", ROS_CLIENT="relay-b", ROS_RELAY_ROLE="region")
    specs = [relay_a, relay_b]
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=specs)
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert [step.outcome for step in steps] == ["started", "started"]
    pid_a = steps[0].pid
    pid_b = steps[1].pid
    assert pid_a is not None and pid_b is not None and pid_a != pid_b
    # b's pid filed under a's name: down must refuse to touch b
    services._write_pid_file(
        services.pid_path(runs, "relay-a"), pid_b, services._start_ticks(pid_b)
    )
    steps = down_services(CLUSTER, runs_dir=runs, names=["relay-a"], specs=specs)
    assert steps[0].outcome == "stale-pid"
    assert not _gone(pid_b)  # b is still running after a's down
    down_services(CLUSTER, runs_dir=runs, names=["relay-a", "relay-b"], specs=specs)


@requires_proc
def test_down_with_nothing_running_is_a_clean_no_op(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    steps = down_services(
        CLUSTER, runs_dir=runs, names=["one", "two"], specs=[fake("one"), fake("two")]
    )
    assert [step.outcome for step in steps] == ["not-running", "not-running"]
    assert services.down_exit_code(steps) == 0


@requires_proc
def test_down_after_a_crash_reports_the_stale_pid_file(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    # a pid file naming a process that has exited and been reaped
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    ticks = services._start_ticks(dead.pid)
    dead.wait()
    _write_pid_file(runs, "sleeper", dead.pid, ticks)
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[fake("sleeper")])
    assert steps[0].outcome in ("not-running", "stale-pid")
    assert not services.pid_path(runs, "sleeper").exists()


@requires_proc
def test_a_malformed_pid_file_is_reported_and_the_sweep_continues(
    tmp_path: Path, shadow_services: None, ready_dir: Path
) -> None:
    # a binary pid file once crashed the whole down sweep with a
    # UnicodeDecodeError; now it is one reported step, the file stays
    # for a person to look at, and the later services still stop. The
    # malformed record (relay-r1) comes BEFORE the live one (relay-r2)
    # in down order, so the step order proves the sweep carried on
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        specs=[services.spec_for_name("relay-r2")],
    )
    _track(steps)
    _wait_ready(steps, ready_dir)
    bad = services.pid_path(runs, "relay-r1")
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(b"\xff\xfe not a pid file")
    steps = down_services(runs_dir=runs)
    assert [step.name for step in steps] == ["relay-r1", "relay-r2"]
    assert [step.outcome for step in steps] == ["malformed-pid-file", "stopped"]
    assert "left in place" in steps[0].detail
    assert bad.exists()  # not ours to clean
    # down left something it could not deal with, so it exits 1 - the
    # sweep still carried on and the live service still stopped
    assert services.down_exit_code(steps) == 1


@requires_proc
def test_an_unreadable_record_is_malformed_in_down_and_launch_failed_in_up(
    tmp_path: Path,
) -> None:
    # a directory at the pid-file path (not chmod 000, which root
    # ignores in CI): down reports it and keeps it; up owns its records
    # and tries to replace it, and when even that fails nothing is
    # spawned - a service must never run without its record
    runs = tmp_path / "runs"
    services.pid_path(runs, "relay-r1").parent.mkdir(parents=True)
    services.pid_path(runs, "relay-r1").mkdir()
    steps = down_services(runs_dir=runs)
    assert steps[0].name == "relay-r1"
    assert steps[0].outcome == "malformed-pid-file"
    assert "unreadable" in steps[0].detail
    assert services.pid_path(runs, "relay-r1").is_dir()  # left in place
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        specs=[
            fake("relay-r1", ROS_RELAY_ROLE="region", ROS_CLIENT="relay-r1", ROS_REGION_ID="r1")
        ],
    )
    assert steps[0].outcome == "launch-failed"
    assert "could not remove the record" in steps[0].detail
    assert services.up_exit_code(steps) == 1


@requires_proc
def test_impossible_pid_values_are_malformed_and_left_alone(tmp_path: Path) -> None:
    # Linux pid_t is a signed 32-bit int: pid 0 (kill(0, 0) checks the
    # caller's own process group and so looks alive), a negative pid, a
    # pid above 2**31-1 and negative start ticks are all impossible -
    # reported, kept, nothing signalled
    runs = tmp_path / "runs"
    services_dir = runs / "services"
    services_dir.mkdir(parents=True)
    for name, text in (
        ("relay-r1", "0 1"),
        ("relay-r2", "-1 1"),
        ("relay-r3", "2147483648 1"),
        ("relay-r4", "5 -1"),
    ):
        (services_dir / f"{name}.pid").write_text(text + "\n", encoding="utf-8")
    steps = down_services(runs_dir=runs)
    assert [step.outcome for step in steps] == ["malformed-pid-file"] * 4
    for name in ("relay-r1", "relay-r2", "relay-r3", "relay-r4"):
        assert (services_dir / f"{name}.pid").exists()
    # down left them all in place: that is a failure, not a clean sweep
    assert services.down_exit_code(steps) == 1


@requires_proc
def test_an_undecidable_identity_is_reported_by_up_and_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a live record whose argv and environment cannot be read is
    # neither ours nor not-ours: up says still-starting (not a
    # failure), down says identity-unknown and keeps the record - and
    # THAT is a failure, because something that may be running was
    # not stopped
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    start = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(start)
    assert start[0].outcome == "started"
    monkeypatch.setattr(services, "_proc_fields", lambda pid: ([], {}))
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert second[0].outcome == "still-starting"
    assert services.up_exit_code(second) == 0
    assert isinstance(services.read_pid_record(runs, "sleeper"), tuple)  # kept
    stopped = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])
    assert stopped[0].outcome == "identity-unknown"
    assert services.down_exit_code(stopped) == 1


@requires_proc
def test_up_restarts_a_crashed_service(tmp_path: Path, ready_dir: Path) -> None:
    # the round-3 regression: a dead record made `up` answer
    # still-starting forever, so after any crash the service never came
    # back until someone deleted the pid file by hand. Liveness is
    # checked BEFORE the identity proofs now, so a dead record is
    # replaced and the service restarts - the most common use of up
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(first)
    _wait_ready(first, ready_dir)
    pid = first[0].pid
    assert pid is not None
    fd = os.pidfd_open(pid)
    try:
        signal.pidfd_send_signal(fd, signal.SIGKILL)
    finally:
        os.close(fd)
    assert _wait_gone(pid)

    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(second)
    _wait_ready(second, ready_dir)
    assert second[0].outcome == "started"
    assert second[0].pid not in (None, pid)  # a new child
    assert services.read_pid_record(runs, "sleeper") == (second[0].pid, second[0].start_ticks)
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])


@requires_proc
def test_a_proc_that_cannot_be_read_makes_liveness_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a hidepid-style mount: /proc/<pid>/stat exists but cannot be
    # read. Liveness answers unknown, and `up` keeps the record
    # (still-starting) instead of guessing
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(first)
    pid = first[0].pid
    assert pid is not None

    def denied(*args: object, **kwargs: object) -> str:
        raise PermissionError("hidepid")

    monkeypatch.setattr(services.Path, "read_text", denied)
    assert services._liveness(pid) == "unknown"
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert second[0].outcome == "still-starting"
    assert services.read_pid_record(runs, "sleeper") == (pid, first[0].start_ticks)  # kept


@requires_proc
def test_a_missing_stat_entry_is_not_proof_of_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # /proc/<pid>/stat can be MISSING while the pid still answers
    # (procfs visibility is not liveness): the kernel is asked again,
    # and only a pid that no longer answers kill(0) is dead. A live
    # pid with a missing entry is unknown, and `up` keeps its record
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(first)
    pid = first[0].pid
    assert pid is not None and services._alive(pid)

    def vanished(*args: object, **kwargs: object) -> str:
        raise FileNotFoundError("no stat entry")

    monkeypatch.setattr(services.Path, "read_text", vanished)
    assert services._liveness(pid) == "unknown"
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert second[0].outcome == "still-starting"
    assert services.read_pid_record(runs, "sleeper") == (pid, first[0].start_ticks)  # kept


@requires_proc
def test_a_missing_stat_entry_with_an_unsignalable_pid_is_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the SECOND kill(0) in the missing-stat branch can itself raise
    # PermissionError (another user's pid, say): that must be unknown,
    # never dead and never a traceback that ends the whole up sweep
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(first)
    pid = first[0].pid
    assert pid is not None and services._alive(pid)

    def vanished(*args: object, **kwargs: object) -> str:
        raise FileNotFoundError("no stat entry")

    real_kill = os.kill
    seen: list[int] = []

    def deny_second(pid_arg: int, sig: int) -> None:
        seen.append(pid_arg)
        if len(seen) > 1:
            raise PermissionError("not yours")
        real_kill(pid_arg, sig)

    monkeypatch.setattr(services.Path, "read_text", vanished)
    monkeypatch.setattr(os, "kill", deny_second)
    assert services._liveness(pid) == "unknown"
    assert len(seen) == 2  # asked once, then asked again
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert second[0].outcome == "still-starting"
    assert services.read_pid_record(runs, "sleeper") == (pid, first[0].start_ticks)  # kept


@requires_proc
def test_a_second_up_reports_a_running_service_that_is_not_ready(tmp_path: Path) -> None:
    # an already-running HTTP service must really answer its readiness
    # route before a second `up` calls it started: running but broken
    # is running-not-ready, a failure, with the restart hint
    runs = tmp_path / "runs"
    spec, _port = _http_fake("broken", health="/health", bad_health=True)
    first = services.start_service(spec, runs_dir=runs)
    _track([first])
    assert first.outcome == "started"
    t0 = time.monotonic()
    second = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        ready_timeout_s=0.5,
        specs=[spec],
    )
    elapsed = time.monotonic() - t0
    # the already-running re-probe runs on the CALLER'S ready timeout,
    # not the module constant: a 0.5 s budget decides in well under
    # 1.5 s even though the 404 makes the probe retry to its deadline
    assert elapsed < 1.5
    assert second[0].outcome == "running-not-ready"
    assert "rosctl down broken" in second[0].detail
    assert services.up_exit_code(second) == 1
    assert isinstance(services.read_pid_record(runs, "broken"), tuple)  # record kept
    down_services(CLUSTER, runs_dir=runs, names=["broken"], specs=[spec])


@requires_proc
def test_a_record_that_cannot_be_removed_fails_the_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # every branch that drops a record it no longer obeys (dead,
    # mismatch, malformed) goes through one guard: an unlink that
    # fails must stop the start - launch-failed, nothing spawned -
    # rather than leave a new child running beside a record that
    # names a dead launch
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(first)
    pid = first[0].pid
    assert pid is not None
    fd = os.pidfd_open(pid)
    signal.pidfd_send_signal(fd, signal.SIGKILL)
    os.close(fd)
    assert _wait_gone(pid)
    record = services.read_pid_record(runs, "sleeper")
    assert isinstance(record, tuple)

    def denied(*args: object, **kwargs: object) -> None:
        raise PermissionError("no removing")

    monkeypatch.setattr(services.Path, "unlink", denied)
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert second[0].outcome == "launch-failed"
    assert "could not remove the record" in second[0].detail
    # nothing spawned: the record still names the dead launch
    assert services.read_pid_record(runs, "sleeper") == record
    assert services.up_exit_code(second) == 1


@requires_proc
def test_a_pid_file_that_cannot_be_removed_does_not_end_the_down_sweep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a down of two fakes: down deals with the process and THEN
    # removes the record, and if the removal raises the step is
    # `record-left` (the process WAS stopped; a person must remove the
    # file), down exits 1 - and the sweep carries on, so the second
    # service is stopped too
    runs = tmp_path / "runs"
    first_spec = fake("first", ROS_CLIENT="sleeper")
    second_spec = fake("second", ROS_CLIENT="sleeper")
    up = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[first_spec, second_spec]
    )
    _track(up)
    first_pid, second_pid = up[0].pid, up[1].pid
    assert first_pid is not None and second_pid is not None
    left_path = services.pid_path(runs, "first")
    real_unlink = Path.unlink

    def deny(self: Path, *, missing_ok: bool = False) -> None:
        if self == left_path:
            raise PermissionError("no removing")
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(services.Path, "unlink", deny)
    steps = down_services(
        CLUSTER, runs_dir=runs, names=["first", "second"], specs=[first_spec, second_spec]
    )
    assert [(step.name, step.outcome) for step in steps] == [
        ("first", "record-left"),
        ("second", "stopped"),
    ]
    assert "pid" in steps[0].detail and "could not be removed" in steps[0].detail
    assert "remove it by hand" in steps[0].detail
    assert services.read_pid_record(runs, "first") is not None  # the record stays
    assert services.down_exit_code(steps) == 1
    # both processes are gone: record-left stopped its process too
    assert _gone(first_pid)
    assert _gone(second_pid)


@requires_proc
def test_a_dead_record_that_cannot_be_removed_is_record_left(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the dead-record branch of the same guard: the pid was not
    # running, and the pid file could not be removed - record-left
    # says so instead of ending the sweep
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    up = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(up)
    pid = up[0].pid
    assert pid is not None
    fd = os.pidfd_open(pid)
    signal.pidfd_send_signal(fd, signal.SIGKILL)
    os.close(fd)
    assert _wait_gone(pid)
    record = services.read_pid_record(runs, "sleeper")
    assert isinstance(record, tuple)

    def denied(*args: object, **kwargs: object) -> None:
        raise PermissionError("no removing")

    monkeypatch.setattr(services.Path, "unlink", denied)
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])
    assert steps[0].outcome == "record-left"
    assert "was not running" in steps[0].detail
    assert "remove it by hand" in steps[0].detail
    assert services.read_pid_record(runs, "sleeper") == record  # kept
    assert services.down_exit_code(steps) == 1


@requires_proc
def test_a_not_ready_service_whose_pid_file_cannot_be_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # on the up side the failure is already a failure: a not-ready
    # service whose record cannot be removed is still `not-ready`,
    # with the removal error said in the detail - and the next service
    # still starts in the same call
    runs = tmp_path / "runs"
    broken_spec, _port = _http_fake("broken", health="/health", bad_health=True)
    left_path = services.pid_path(runs, "broken")
    real_unlink = Path.unlink

    def deny(self: Path, *, missing_ok: bool = False) -> None:
        if self == left_path:
            raise PermissionError("no removing")
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(services.Path, "unlink", deny)
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        ready_timeout_s=0.5,
        specs=[broken_spec, fake("other")],
    )
    assert [(step.name, step.outcome) for step in steps] == [
        ("broken", "not-ready"),
        ("other", "started"),
    ]
    assert "the service was stopped" in steps[0].detail
    assert "could not be removed" in steps[0].detail
    assert isinstance(services.read_pid_record(runs, "broken"), tuple)  # kept
    assert services.up_exit_code(steps) == 1
    _track(steps[1:])
    down_services(CLUSTER, runs_dir=runs, names=["other"], specs=[fake("other")])


@requires_proc
def test_a_lock_that_cannot_be_opened_is_reported(tmp_path: Path) -> None:
    # a directory where the per-service lock file goes: up reports
    # launch-failed and spawns nothing, down reports lock-failed and
    # signals nothing - the lock stays mandatory, never skipped. ONE
    # up call proves the sweep carries on inside the call: the second
    # service starts although the first failed
    runs = tmp_path / "runs"
    (runs / "services").mkdir(parents=True)
    (runs / "services" / "sleeper.lock").mkdir()
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        specs=[spec, fake("other")],
    )
    assert [(s.name, s.outcome) for s in steps] == [
        ("sleeper", "launch-failed"),
        ("other", "started"),
    ]
    assert "could not open the lock" in steps[0].detail
    assert services.up_exit_code(steps) == 1
    _track(steps[1:])
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])
    assert steps[0].outcome == "lock-failed"
    assert services.down_exit_code(steps) == 1
    down_services(CLUSTER, runs_dir=runs, names=["other"], specs=[fake("other")])


@requires_proc
def test_a_record_replaced_during_the_readiness_wait_is_left_alone(
    tmp_path: Path,
) -> None:
    # the readiness cleanup acts only on the launch it started: when
    # the pid file names another launch by the time the wait gives up,
    # nothing is signalled and nothing is unlinked
    runs = tmp_path / "runs"
    spec, _port = _http_fake("slow", health="/health", delay_s=10.0)
    start = services.start_service(spec, runs_dir=runs)
    _track([start])
    assert start.outcome == "started"
    assert start.pid is not None and start.start_ticks is not None
    other = _spawn_raw("import time; time.sleep(300)")
    other_ticks = services._start_ticks(other)

    def replace() -> None:
        time.sleep(0.2)  # while the readiness wait is running
        services._write_pid_file(services.pid_path(runs, "slow"), other, other_ticks)

    rewriter = threading.Thread(target=replace)
    rewriter.start()
    step = services._ready_step(
        spec,
        runs_dir=runs,
        pid=start.pid,
        start_ticks=start.start_ticks,
        ready_timeout_s=1.0,
        grace_s=0.0,
    )
    rewriter.join()
    assert step.outcome == "not-ready"
    assert "replaced" in step.detail
    assert services.read_pid_record(runs, "slow") == (other, other_ticks)  # not unlinked
    assert not _gone(start.pid)  # and not signalled


@requires_proc
def test_a_concurrent_start_waits_for_the_stop_to_release_the_lock(
    tmp_path: Path,
) -> None:
    # the stop holds the per-service lock for its whole run; a start of
    # the same name meanwhile must WAIT for it, then find no record and
    # start a fresh child. Without the lock it would read the live
    # record mid-stop and say already-running
    runs = tmp_path / "runs"
    spec = fake("stubborn", code=IGNORER)
    start = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(start)
    assert start[0].outcome == "started"
    outcomes: list[str] = []

    def starter() -> None:
        outcomes.append(services.start_service(spec, runs_dir=runs).outcome)

    stopper = threading.Thread(
        target=lambda: down_services(
            CLUSTER, runs_dir=runs, names=["stubborn"], specs=[spec], stop_timeout_s=1.0
        )
    )
    stopper.start()
    time.sleep(0.2)  # inside the stop's SIGINT wait, the lock is held
    starter_thread = threading.Thread(target=starter)
    starter_thread.start()
    stopper.join()
    starter_thread.join()
    assert outcomes == ["started"]
    down_services(CLUSTER, runs_dir=runs, names=["stubborn"], specs=[spec])


@requires_proc
def test_the_readiness_wait_gives_up_at_the_deadline_not_the_server_s(
    tmp_path: Path, ready_dir: Path
) -> None:
    # a server that ACCEPTS the connection and then sleeps 30 s
    # before answering (the slow-answer case, not a refused
    # connection), with a 3 s readiness budget - room for the child
    # to start and bind even on a slow runner under coverage, so the
    # test cannot fail for a reason it does not mean: the probe's
    # timeout is the time REMAINING, so up gives up at its own ~3 s
    # deadline, not at the server's 30
    runs = tmp_path / "runs"
    spec, _port = _http_fake("slow", health="/health", answer_delay_s=30.0)
    t0 = time.monotonic()
    steps = up_services(
        CLUSTER,
        runs_dir=runs,
        precheck=False,
        grace_s=0.0,
        ready_timeout_s=3.0,
        specs=[spec],
    )
    elapsed = time.monotonic() - t0
    assert (ready_dir / "hit").exists()  # the probe really connected
    assert steps[0].outcome == "not-ready"
    assert "the service was stopped" in steps[0].detail
    assert elapsed < 4.5
    assert steps[0].pid is not None and _gone(steps[0].pid)


def test_log_tail_survives_bytes_that_are_not_utf8(tmp_path: Path) -> None:
    # a failure message must never end the up sweep with a
    # UnicodeDecodeError of its own
    log = services.log_path(tmp_path, "crasher")
    log.parent.mkdir(parents=True)
    log.write_bytes(b"traceback\n\xff\xfe not utf-8\n")
    tail = services.log_tail(tmp_path, "crasher")
    assert "traceback" in tail
    assert "\ufffd" in tail  # replacement characters, not an exception


def test_services_imports_where_fcntl_does_not_exist() -> None:
    # native Windows has no fcntl: the module must still import there
    # (fcntl is imported inside _service_lock, not at module top), so
    # rosctl prints its Linux/WSL2 message instead of an ImportError
    # traceback. A FRESH interpreter, not a reload: no importlib.reload
    # anywhere in the test files
    code = (
        "import sys; sys.modules['fcntl'] = None\n"
        "from pathlib import Path\n"
        "from restaurantos.common.cluster import Cluster\n"
        "from restaurantos.sim import services\n"
        "services._has_proc = lambda: False\n"
        "try:\n"
        "    services.up_services(\n"
        "        Cluster(regions=1), runs_dir=Path('runs'), precheck=False,\n"
        "        specs=[services.ServiceSpec(name='s', args=('-c', 'pass'), env={})],\n"
        "    )\n"
        "except services.PlatformError:\n"
        "    sys.exit(0)\n"
        "sys.exit(1)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr


@requires_proc
def test_a_bad_name_stops_nothing(tmp_path: Path, ready_dir: Path) -> None:
    # every name is resolved before the first stop: an invalid name
    # anywhere in the list means nothing is stopped at all
    runs = tmp_path / "runs"
    spec = fake("sleeper")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(steps)
    _wait_ready(steps, ready_dir)
    with pytest.raises(ValueError, match="no service nope"):
        down_services(CLUSTER, runs_dir=runs, names=["sleeper", "nope"], specs=[spec])
    assert isinstance(services.read_pid_record(runs, "sleeper"), tuple)  # untouched
    assert steps[0].pid is not None and not _gone(steps[0].pid)
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])


@requires_proc
def test_down_without_names_stops_what_up_started_across_cluster_files(
    tmp_path: Path, shadow_services: None, ready_dir: Path
) -> None:
    # the review's case: up with a 3-region table, then a plain `down`
    # while reading a 1-region cluster file - the pid files, not the
    # table, name the work
    runs = tmp_path / "runs"
    names = ["relay-r1", "relay-r2", "relay-r3", "edge-r1", "edge-r2", "edge-r3"]
    specs = [services.spec_for_name(name) for name in names]
    steps = up_services(Cluster(regions=3), runs_dir=runs, precheck=False, grace_s=0.0, specs=specs)
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert [step.outcome for step in steps] == ["started"] * 6

    steps = down_services(Cluster(regions=1), runs_dir=runs)
    # down order: the region side last, relays before edges. Whole-list
    # compare, so a failure names the service and the wrong outcome
    assert [(step.name, step.outcome) for step in steps] == [(name, "stopped") for name in names]
    for name in names:
        assert not services.pid_path(runs, name).exists()


@requires_proc
def test_an_out_of_cluster_pid_file_is_stopped_and_a_non_region_one_is_left_alone(
    tmp_path: Path, shadow_services: None, ready_dir: Path
) -> None:
    # relay-r9 is a canonical patterned name, so its pid file is
    # managed after the cluster shrinks; relay-not-a-region is not a
    # pattern at all, so it is reported and left alone
    runs = tmp_path / "runs"
    spec = services.spec_for_name("relay-r9")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(steps)
    _wait_ready(steps, ready_dir)
    decoy = _spawn_raw("import time; time.sleep(300)")
    _write_pid_file(runs, "relay-not-a-region", decoy, services._start_ticks(decoy))
    steps = down_services(runs_dir=runs)
    outcomes = {step.name: step.outcome for step in steps}
    assert outcomes["relay-r9"] == "stopped"
    assert outcomes["relay-not-a-region"] == "unknown"
    assert services.pid_path(runs, "relay-not-a-region").exists()
    assert not _gone(decoy)


@requires_proc
def test_a_pid_file_matching_no_pattern_is_reported_and_left_alone(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    decoy = _spawn_raw("import time; time.sleep(300)")
    _write_pid_file(runs, "mystery", decoy, services._start_ticks(decoy))
    steps = down_services(CLUSTER, runs_dir=runs)
    assert len(steps) == 1 and steps[0].name == "mystery"
    assert steps[0].outcome == "unknown"
    assert "left alone" in steps[0].detail
    assert services.pid_path(runs, "mystery").exists()  # not ours to clean
    assert not _gone(decoy)


@requires_proc
def test_named_down_stops_the_coordinator_before_the_gateway(
    tmp_path: Path, shadow_services: None, ready_dir: Path
) -> None:
    runs = tmp_path / "runs"
    # the shadow sleeper never serves /health, so start the fakes as
    # port-less: identity (argv + env) is what down checks, not ports
    specs = [
        dataclasses.replace(services.spec_for_name(name), port=None, ready=None)
        for name in ("gateway", "coordinator")
    ]
    steps = up_services(Cluster(regions=1), runs_dir=runs, precheck=False, grace_s=0.0, specs=specs)
    _track(steps)
    _wait_ready(steps, ready_dir)
    # asked for in table order; stopped in down order. Whole-list
    # compare, so a failure names the service and the wrong outcome
    steps = down_services(Cluster(regions=1), runs_dir=runs, names=["gateway", "coordinator"])
    assert [(step.name, step.outcome) for step in steps] == [
        ("coordinator", "stopped"),
        ("gateway", "stopped"),
    ]


# --- precheck and platform ----------------------------------------------


def test_unreachable_ports_reports_only_the_dead_ones() -> None:
    free = _free_port()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", free))
        listener.listen(1)
        assert services.unreachable_ports([free, 1], timeout_s=0.2) == [1]


def test_the_precheck_message_names_the_ports_and_the_fixes() -> None:
    message = services.precheck_message(Cluster(regions=1), [21010, 5500])
    assert "gateway__r1-primary (21010)" in message
    assert "hq-primary (5500)" in message
    assert "docker compose -p restaurantos -f infra/generated/compose.yaml up -d" in message
    assert "curl -X POST http://127.0.0.1:8474/reset" in message
    # bootstrap is named only for the no-cluster case - it wipes the
    # volumes, so it is never the fix for a stopped cluster
    assert message.count("bootstrap") == 1
    assert "If there is no cluster yet" in message
    assert "--no-precheck" in message


@requires_proc
def test_up_refuses_when_the_cluster_is_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    free = _free_port()
    monkeypatch.setattr(services, "dials", lambda spec, cluster: [free])
    with pytest.raises(services.PrecheckError, match="the cluster is not reachable"):
        up_services(CLUSTER, runs_dir=tmp_path / "runs", precheck=True, specs=[fake("sleeper")])
    assert services.read_pid_record(tmp_path / "runs", "sleeper") is None


@requires_proc
def test_no_precheck_spawns_anyway(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ready_dir: Path
) -> None:
    free = _free_port()
    monkeypatch.setattr(services, "dials", lambda spec, cluster: [free])
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[fake("sleeper")]
    )
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert [step.outcome for step in steps] == ["started"]
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[fake("sleeper")])


@requires_proc
def test_up_of_one_service_prechecks_only_its_own_dials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ready_dir: Path
) -> None:
    # up edge-r1 (of a 3-region cluster) checks r1's node ports and
    # nothing else: not r2's or r3's, and none of the proxies no edge
    # dials
    seen: list[list[int]] = []

    def recording_unreachable(ports: list[int], **kwargs: object) -> list[int]:
        seen.append(list(ports))
        return []

    monkeypatch.setattr(services, "unreachable_ports", recording_unreachable)
    cluster = Cluster(regions=3)
    spec = fake("edge-r1", ROS_MODE="edge", ROS_REGION_ID="r1")
    steps = up_services(
        cluster,
        runs_dir=tmp_path / "runs",
        names=["edge-r1"],
        precheck=True,
        grace_s=0.0,
        specs=[spec],
    )
    _track(steps)
    _wait_ready(steps, ready_dir)
    assert seen == [[5510, 5511]]
    down_services(cluster, runs_dir=tmp_path / "runs", names=["edge-r1"], specs=[spec])


def test_a_platform_without_proc_is_refused_before_anything_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(services, "_has_proc", lambda: False)
    with pytest.raises(services.PlatformError, match="Linux or WSL2"):
        up_services(CLUSTER, runs_dir=tmp_path / "runs", precheck=False, specs=[fake("sleeper")])
    with pytest.raises(services.PlatformError, match="Linux or WSL2"):
        down_services(CLUSTER, runs_dir=tmp_path / "runs")
    assert not (tmp_path / "runs").exists()  # nothing was touched


# --- .env identity warnings ---------------------------------------------


def test_dotenv_identity_warnings_name_the_identity_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# a comment\n"
        "ROS_MODE=edge\n"
        "DB_HOST=example\n"
        "export ROS_RELAY_ROLE=hq\n"
        "ROS_NETWORK=compose\n",
        encoding="utf-8",
    )
    # ROS_NETWORK is NOT reported: rosctl always sets it itself, and
    # "Running things" tells everyone to put it in .env
    assert services.dotenv_identity_warnings(env_file) == [
        "ROS_MODE=edge",
        "export ROS_RELAY_ROLE=hq",
    ]


def test_dotenv_identity_warnings_are_empty_without_the_keys(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=example\nROS_NETWORK=host\n", encoding="utf-8")
    assert services.dotenv_identity_warnings(env_file) == []
    assert services.dotenv_identity_warnings(tmp_path / "missing.env") == []

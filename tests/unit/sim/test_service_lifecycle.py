"""up/down against a fake target: real child processes (a `python -c`
sleeper or HTTP server, or a shadowed restaurantos package), no cluster,
no real service code. These prove the supervision contract - SIGINT
cleanup, escalation to SIGKILL, readiness waits, stale pid files never
killed, idempotent `up`, per-name single starts, the port check, the
precheck, the /proc platform gate and the pid-file-driven `down` -
which in-process mocks cannot, because they exercise bind, signal and
/proc paths.
"""

from __future__ import annotations

import contextlib
import dataclasses
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
    "signal.signal(signal.SIGINT, lambda *_: sys.exit(0)); time.sleep(300)"
)
IGNORER = (
    "import signal, sys, time; "
    "signal.signal(signal.SIGINT, signal.SIG_IGN); "
    "print('ready'); sys.stdout.flush(); time.sleep(300)"
)
PRINTER = (
    "import os, signal, sys, time; "
    "print('ROS_MODE=' + os.environ.get('ROS_MODE', '')); sys.stdout.flush(); "
    "signal.signal(signal.SIGINT, lambda *_: sys.exit(0)); time.sleep(300)"
)

# A minimal HTTP server on PORT that answers 200 on the health path it
# was given and 404 everywhere else, optionally after a start delay
# (before it binds).
HTTP_SERVER = """import time
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self):
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
    "import signal, sys, time; "
    "signal.signal(signal.SIGINT, lambda *_: sys.exit(0)); time.sleep(300)\n"
)


def fake(
    name: str,
    *,
    port: int | None = None,
    ready: str | None = None,
    code: str = SLEEPER,
    **env: str,
) -> ServiceSpec:
    return ServiceSpec(name=name, args=("-c", code), env=env, port=port, ready=ready)


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


@pytest.fixture
def shadow_services(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A restaurantos package on PYTHONPATH that shadows the real one:
    the canonical `-m restaurantos.<component>` argv runs a sleeper."""
    pkg = tmp_path / "shadow"
    (pkg / "restaurantos").mkdir(parents=True)
    (pkg / "restaurantos" / "__init__.py").write_text("", encoding="utf-8")
    for module in ("relay", "gateway", "coordinator"):
        (pkg / "restaurantos" / f"{module}.py").write_text(SHADOW_MODULE, encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(pkg))


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


def _spawn_raw(code: str) -> int:
    """A live process that is NOT one of our services - what a reused
    pid looks like to `down`."""
    proc = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    _leftover.append(proc.pid)
    return proc.pid


def _http_fake(
    name: str, *, health: str, delay_s: float = 0.0, bad_health: bool = False
) -> tuple[ServiceSpec, int]:
    """A fake HTTP service: answers 200 on HEALTH (or on /other when
    bad_health, so /health gets its 404), after DELAY_S."""
    port = _free_port()
    served = "/other" if bad_health else health
    code = HTTP_SERVER.format(
        health=served,
        delay=f"time.sleep({delay_s})" if delay_s else "pass",
        port=port,
    )
    return fake(name, port=port, ready=health, code=code), port


# --- up then down ------------------------------------------------------


def test_up_starts_a_detached_child_and_down_stops_it(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[fake("sleeper")]
    )
    _track(runs, steps)
    assert [step.outcome for step in steps] == ["started"]
    pid = services.read_pid(runs, "sleeper")
    assert pid is not None
    assert pid != os.getpid()
    os.kill(pid, 0)  # alive, and detached from this process
    assert services.log_path(runs, "sleeper").exists()

    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[fake("sleeper")])
    assert steps == [Step("sleeper", "stopped", f"pid {pid}")]
    assert _wait_gone(pid)
    assert not services.pid_path(runs, "sleeper").exists()


def test_the_log_carries_one_header_line_per_start(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("sleeper", ROS_MODE="gateway")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(runs, steps)
    header = services.log_path(runs, "sleeper").read_text(encoding="utf-8").splitlines()[0]
    assert header.startswith("# ")
    assert "start sleeper" in header
    assert "ROS_MODE=gateway" in header
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])


def test_child_environment_overrides_leftover_shell_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a leftover ROS_MODE in the shell must not leak into the child:
    # the table's identity is the only truth
    monkeypatch.setenv("ROS_MODE", "edge")
    runs = tmp_path / "runs"
    spec = fake("printer", code=PRINTER, ROS_MODE="gateway", ROS_CLIENT="gateway")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(runs, steps)
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


def test_second_up_is_idempotent(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("sleeper")
    first = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(runs, first)
    second = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    assert [step.outcome for step in second] == ["already-running"]
    assert (
        down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])[0].outcome
        == "stopped"
    )


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
        assert services.read_pid(runs, "busy") is None
    assert services.up_exit_code(steps) == 1


# --- readiness waits ----------------------------------------------------


def test_a_just_spawned_child_is_alive_even_inside_exec(tmp_path: Path) -> None:
    # regression for the exec race: right after Popen returns, the
    # child can still be mid-execve with an empty /proc cmdline - a
    # naive "empty cmdline means dead" check reported the service
    # exited-immediately and orphaned it
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    _leftover.append(proc.pid)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        assert services._alive(proc.pid), "a just-spawned child must count as alive"
        if Path(f"/proc/{proc.pid}/cmdline").read_bytes():
            break  # exec finished; the window has been crossed
        time.sleep(0.01)


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
    _track(runs, steps)
    assert steps[0].outcome == "started"
    assert f"ready on port {port}" in steps[0].detail
    assert (
        down_services(CLUSTER, runs_dir=runs, names=["slow"], specs=[spec])[0].outcome == "stopped"
    )


def test_a_service_that_never_answers_health_is_not_ready(tmp_path: Path) -> None:
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
    _track(runs, steps)
    assert steps[0].outcome == "not-ready"
    assert "still not answering /health" in steps[0].detail
    assert "the other services keep running" in steps[0].detail
    assert services.read_pid(runs, "broken") is None  # its pid file is cleaned
    assert services.up_exit_code(steps) == 1
    down_services(CLUSTER, runs_dir=runs, names=["broken"], specs=[spec])


def test_a_service_that_dies_inside_the_readiness_wait_is_named(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    spec = fake("midway", code="import sys, time; time.sleep(1.0); sys.exit(3)")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=3.0, specs=[spec])
    assert steps[0].outcome == "exited-immediately"
    assert services.read_pid(runs, "midway") is None
    assert services.up_exit_code(steps) == 1


def test_a_relay_surviving_its_grace_period_is_started(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.5, specs=[fake("relay-fake")]
    )
    _track(runs, steps)
    assert steps[0].outcome == "started"
    assert "no HTTP port" in steps[0].detail
    down_services(CLUSTER, runs_dir=runs, names=["relay-fake"], specs=[fake("relay-fake")])


def test_a_service_that_dies_at_once_is_named_with_its_log(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("crasher", code="import sys; sys.exit(3)")
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.5, specs=[spec])
    assert steps[0].outcome == "exited-immediately"
    assert str(services.log_path(runs, "crasher")) in steps[0].detail
    assert "the other services keep running" in steps[0].detail
    assert services.read_pid(runs, "crasher") is None
    assert services.up_exit_code(steps) == 1


def test_up_starts_only_the_named_services(tmp_path: Path) -> None:
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
    _track(runs, steps)
    assert [step.name for step in steps] == ["solo"]
    assert services.read_pid(runs, "other") is None
    down_services(CLUSTER, runs_dir=runs, names=["solo"], specs=specs)


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


def test_sigint_ignored_service_is_killed(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    spec = fake("stubborn", code=IGNORER)
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[spec])
    _track(runs, steps)
    pid = services.read_pid(runs, "stubborn")
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


def test_a_reused_pid_is_never_killed(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    decoy = _spawn_raw("import time; time.sleep(300)")
    spec = fake("sleeper", ROS_CLIENT="sleeper")
    services.log_path(runs, "sleeper").parent.mkdir(parents=True, exist_ok=True)
    services.pid_path(runs, "sleeper").write_text(f"{decoy}\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[spec])
    assert steps[0].outcome == "stale-pid"
    assert "left alone" in steps[0].detail
    assert not services.pid_path(runs, "sleeper").exists()
    assert not _gone(decoy)  # the decoy survived the down


def test_a_sibling_with_the_same_argv_is_not_confused(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    relay_a = fake("relay-a", ROS_CLIENT="relay-a", ROS_RELAY_ROLE="region")
    relay_b = fake("relay-b", ROS_CLIENT="relay-b", ROS_RELAY_ROLE="region")
    specs = [relay_a, relay_b]
    steps = up_services(CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=specs)
    _track(runs, steps)
    assert [step.outcome for step in steps] == ["started", "started"]
    pid_a = services.read_pid(runs, "relay-a")
    pid_b = services.read_pid(runs, "relay-b")
    assert pid_a is not None and pid_b is not None and pid_a != pid_b
    # b's pid filed under a's name: down must refuse to touch b
    services.pid_path(runs, "relay-a").write_text(f"{pid_b}\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs, names=["relay-a"], specs=specs)
    assert steps[0].outcome == "stale-pid"
    assert not _gone(pid_b)  # b is still running after a's down
    down_services(CLUSTER, runs_dir=runs, names=["relay-a", "relay-b"], specs=specs)


def test_down_with_nothing_running_is_a_clean_no_op(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    steps = down_services(
        CLUSTER, runs_dir=runs, names=["one", "two"], specs=[fake("one"), fake("two")]
    )
    assert [step.outcome for step in steps] == ["not-running", "not-running"]
    assert services.down_exit_code(steps) == 0


def test_down_after_a_crash_reports_the_stale_pid_file(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    # a pid file naming a dead process: pid 1 is init, never a service
    services.log_path(runs, "sleeper").parent.mkdir(parents=True, exist_ok=True)
    services.pid_path(runs, "sleeper").write_text("1\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[fake("sleeper")])
    assert steps[0].outcome in ("not-running", "stale-pid")
    assert not services.pid_path(runs, "sleeper").exists()


def test_down_without_names_stops_what_up_started_across_cluster_files(
    tmp_path: Path, shadow_services: None
) -> None:
    # the review's case: up with a 3-region table, then a plain `down`
    # while reading a 1-region cluster file - the pid files, not the
    # table, name the work
    runs = tmp_path / "runs"
    names = ["relay-r1", "relay-r2", "relay-r3", "edge-r1", "edge-r2", "edge-r3"]
    specs = [services.spec_for_name(name) for name in names]
    steps = up_services(Cluster(regions=3), runs_dir=runs, precheck=False, grace_s=0.0, specs=specs)
    _track(runs, steps)
    assert [step.outcome for step in steps] == ["started"] * 6

    steps = down_services(Cluster(regions=1), runs_dir=runs)
    # down order: the region side last, relays before edges
    assert [step.name for step in steps] == [
        "relay-r1",
        "relay-r2",
        "relay-r3",
        "edge-r1",
        "edge-r2",
        "edge-r3",
    ]
    assert all(step.outcome == "stopped" for step in steps)
    for name in names:
        assert not services.pid_path(runs, name).exists()


def test_a_pid_file_matching_no_pattern_is_reported_and_left_alone(
    tmp_path: Path,
) -> None:
    runs = tmp_path / "runs"
    decoy = _spawn_raw("import time; time.sleep(300)")
    services.log_path(runs, "mystery").parent.mkdir(parents=True, exist_ok=True)
    services.pid_path(runs, "mystery").write_text(f"{decoy}\n", encoding="utf-8")
    steps = down_services(CLUSTER, runs_dir=runs)
    assert len(steps) == 1 and steps[0].name == "mystery"
    assert steps[0].outcome == "unknown"
    assert "left alone" in steps[0].detail
    assert services.pid_path(runs, "mystery").exists()  # not ours to clean
    assert not _gone(decoy)


def test_named_down_stops_the_coordinator_before_the_gateway(
    tmp_path: Path, shadow_services: None
) -> None:
    runs = tmp_path / "runs"
    # the shadow sleeper never serves /health, so start the fakes as
    # port-less: identity (argv + env) is what down checks, not ports
    specs = [
        dataclasses.replace(services.spec_for_name(name), port=None, ready=None)
        for name in ("gateway", "coordinator")
    ]
    steps = up_services(Cluster(regions=1), runs_dir=runs, precheck=False, grace_s=0.0, specs=specs)
    _track(runs, steps)
    # asked for in table order; stopped in down order
    steps = down_services(Cluster(regions=1), runs_dir=runs, names=["gateway", "coordinator"])
    assert [step.name for step in steps] == ["coordinator", "gateway"]
    assert all(step.outcome == "stopped" for step in steps)


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
    assert "POST /reset" in message
    assert "rosctl bootstrap" in message
    assert "--no-precheck" in message


def test_up_refuses_when_the_cluster_is_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    free = _free_port()
    monkeypatch.setattr(services, "dialed_ports", lambda cluster: [free])
    with pytest.raises(services.PrecheckError, match="the cluster is not reachable"):
        up_services(CLUSTER, runs_dir=tmp_path / "runs", precheck=True, specs=[fake("sleeper")])
    assert services.read_pid(tmp_path / "runs", "sleeper") is None


def test_no_precheck_spawns_anyway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    free = _free_port()
    monkeypatch.setattr(services, "dialed_ports", lambda cluster: [free])
    runs = tmp_path / "runs"
    steps = up_services(
        CLUSTER, runs_dir=runs, precheck=False, grace_s=0.0, specs=[fake("sleeper")]
    )
    _track(runs, steps)
    assert all(step.outcome == "started" for step in steps)
    down_services(CLUSTER, runs_dir=runs, names=["sleeper"], specs=[fake("sleeper")])


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
    assert services.dotenv_identity_warnings(env_file) == [
        "ROS_MODE=edge",
        "export ROS_RELAY_ROLE=hq",
        "ROS_NETWORK=compose",
    ]


def test_dotenv_identity_warnings_are_empty_without_the_keys(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=example\n", encoding="utf-8")
    assert services.dotenv_identity_warnings(env_file) == []
    assert services.dotenv_identity_warnings(tmp_path / "missing.env") == []

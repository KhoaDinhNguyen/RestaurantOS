"""The service table `rosctl up` starts from, and the host-mode start
and stop around it.

This module is the ONE place that knows what the services are (WP6a
brief: keep the list in one place so host mode and the later container
mode both read it). `spec_for_name` is the table: the names follow
fixed patterns (`gateway`, `coordinator`, `hq-worker`, `relay-r<n>`,
`edge-r<n>`) with the commands, identity environment and ports of
architecture Section 4 - ports come from `common.cluster`, never typed -
and `service_specs` builds a whole cluster's list from those same
patterns. Container mode, when WP0's image exists, reads the same
patterns into the Compose generator.

Supervision model (settled with Mark, 2026-11-23): detached children,
no supervisor parent. `up` spawns each service in its own session
(`start_new_session`), writes `runs/services/<name>.pid`, and waits for
each one to be READY before reporting success - the HTTP services
answer their readiness route (/health, /admin/cluster), the relays and
the HQ worker survive a grace period. `down` works from the pid files,
not from a cluster file: what `up` started is what `down` stops, even
when the cluster file has changed in between - which is why the specs
are rebuilt from the names. A process is signalled only after it has
been proved to be the service its pid file names: same interpreter
arguments AND the identity environment (two relay children share their
argv, so the environment is what tells them apart). A missing, dead or
foreign pid is reported and its file cleaned up - never killed, which
guards against a reused pid. Stop is SIGINT first (uvicorn and the
relay's asyncio.run both shut down cleanly on it), escalating to SIGKILL
after a timeout. The control plane on 8002 (WP6b) is never touched:
`down` signals only processes whose pid file names them, and stops in
down order - the coordinator before the services it probes, the region
side last.

Before spawning, `up` checks the cluster is reachable: one TCP connect
to each port the children themselves will dial - their Toxiproxy
proxies (the request path goes through them) and the nodes' published
ports (the edge APIs and region relays go direct to their own region).
Five children dying on connection errors is the worst first-run
experience; `--no-precheck` is the documented escape for deliberately
starting a service with no cluster.

Linux or WSL2 only: the process checks read /proc, and `up`/`down`
refuse at once anywhere else. Standard library plus httpx (already a
dependency); no psutil.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from restaurantos.common.cluster import (
    HOST_ADDRESS,
    SERVICE_PORTS,
    Cluster,
    host_port,
    proxies,
    service_port,
)

RUNS_DIR = Path("runs")
"""Where playback writes run logs; the services' pid and log files go
to a `services` folder beside them."""

STOP_TIMEOUT_S = 10.0
"""How long `down` waits after SIGINT before escalating to SIGKILL."""

KILL_TIMEOUT_S = 5.0
"""How long `down` waits after SIGKILL for the process to disappear."""

READY_TIMEOUT_S = 20.0
"""How long `up` waits for an HTTP service to answer its readiness
route. A service that binds later than this (or never) is reported
not-ready instead of started."""

RELAY_GRACE_S = 3.0
"""How long `up` waits before calling a service without an HTTP port
(a relay, the HQ worker) started: their configuration errors exit
within a second, so surviving this is the test."""

IDENTITY_KEYS = ("ROS_CLIENT", "ROS_MODE", "ROS_REGION_ID", "ROS_RELAY_ROLE")
"""The settings that say who a process is. `up` strips them from the
inherited environment before applying a service's own, so a leftover
SHELL variable cannot leak into the wrong service. A .env file is a
different hole: every child also reads .env (pydantic-settings), and a
key the table does not set falls back to it - a leftover
ROS_CLIENT=coordinator in .env really would make edge-r1 introduce
itself as coordinator. `up` warns when .env sets one of these (see
`dotenv_identity_warnings`)."""

LOG_TAIL_LINES = 20
"""Log lines shown in an up failure message, so the message names the
cause instead of pointing at a file."""


class PrecheckError(Exception):
    """The cluster is not reachable; the message names the likely fixes."""


class PlatformError(Exception):
    """This platform has no /proc, so the process checks cannot work."""


def _has_proc() -> bool:
    return Path("/proc/self/cmdline").exists()


def require_proc() -> None:
    """Refuse before touching anything where the process checks cannot
    work (macOS has no /proc: every pid would look dead there, so `up`
    would 'clean' the pid files of live services and `down` would find
    nothing to stop)."""
    if not _has_proc():
        raise PlatformError("process checks read /proc, so rosctl up/down need Linux or WSL2")


@dataclass(frozen=True)
class ServiceSpec:
    """One row of the architecture Section 4 table.

    `args` is the argv after the interpreter: a real service is
    ("-m", "<package>"), a test's fake target is ("-c", "<code>").
    Identity lives in `env`, never in arguments. `port` comes from
    `common.cluster.service_port`; the relays have none. `ready` is the
    path whose 200 answer means the service is up (the gateway and the
    edge APIs serve /health, the coordinator /admin/cluster); None for
    services without an HTTP port, which are judged by surviving
    `RELAY_GRACE_S`.
    """

    name: str
    args: tuple[str, ...]
    env: Mapping[str, str]
    port: int | None = None
    ready: str | None = None


@dataclass
class Step:
    """What `up` or `down` did to one service. The command layer prints
    these; the tests assert them."""

    name: str
    outcome: str
    detail: str


def spec_for_name(name: str, cluster: Cluster | None = None) -> ServiceSpec:
    """The service a NAME implies, from the fixed patterns: gateway,
    coordinator, hq-worker, relay-r<n>, edge-r<n>. The region id of a
    relay or edge is part of its name, so a spec can be built from a pid
    file alone (`down` passes no cluster); the port is filled in only
    when a cluster is given and the name has one. A name that matches no
    pattern raises KeyError.

    This is the one table: `service_specs` builds every cluster's list
    from these same patterns, so host mode and the later container mode
    cannot drift apart."""
    if name == "gateway":
        return ServiceSpec(
            name,
            ("-m", "restaurantos.gateway"),
            {"ROS_MODE": "gateway", "ROS_CLIENT": "gateway"},
            port=SERVICE_PORTS["gateway"],
            ready="/health",
        )
    if name == "coordinator":
        return ServiceSpec(
            name,
            ("-m", "restaurantos.coordinator"),
            {"ROS_CLIENT": "coordinator"},
            port=SERVICE_PORTS["coordinator"],
            ready="/admin/cluster",
        )
    if name == "hq-worker":
        return ServiceSpec(
            name,
            ("-m", "restaurantos.relay"),
            {"ROS_RELAY_ROLE": "hq", "ROS_CLIENT": "hq"},
        )
    if name.startswith("relay-"):
        rid = name.removeprefix("relay-")
        if rid:
            return ServiceSpec(
                name,
                ("-m", "restaurantos.relay"),
                {
                    "ROS_RELAY_ROLE": "region",
                    "ROS_CLIENT": name,
                    "ROS_REGION_ID": rid,
                },
            )
    if name.startswith("edge-"):
        rid = name.removeprefix("edge-")
        if rid:
            port = None
            if cluster is not None and rid in cluster.region_ids:
                port = service_port(cluster, name)
            return ServiceSpec(
                name,
                ("-m", "restaurantos.gateway"),
                {"ROS_MODE": "edge", "ROS_REGION_ID": rid},
                port=port,
                ready="/health",
            )
    raise KeyError(f"no service named {name!r}")


def service_specs(cluster: Cluster) -> list[ServiceSpec]:
    """Every host-mode service for this cluster's shape: the gateway,
    the coordinator and the HQ worker, then one region relay and one
    edge API per region (architecture Section 4), all built from
    `spec_for_name`."""
    names = ["gateway", "coordinator", "hq-worker"]
    for rid in cluster.region_ids:
        names += [f"relay-{rid}", f"edge-{rid}"]
    return [spec_for_name(name, cluster) for name in names]


def select_specs(specs: Sequence[ServiceSpec], names: Sequence[str] | None) -> list[ServiceSpec]:
    """The specs to act on: all of them, or the named ones (from the
    table - `up` starts only services this cluster actually has). An
    unknown name is an error that lists the valid ones."""
    if not names:
        return list(specs)
    by_name = {spec.name: spec for spec in specs}
    unknown = [name for name in names if name not in by_name]
    if unknown:
        valid = ", ".join(sorted(by_name))
        raise ValueError(f"no service {', '.join(unknown)} - valid names: {valid}")
    return [by_name[name] for name in names]


_STOP_RANK = {"coordinator": 0, "gateway": 1, "hq-worker": 2}


def _stop_key(name: str) -> tuple[int, str]:
    """Down order: the coordinator first (gone before the services it
    probes), then the gateway and the HQ worker, then the region side -
    relays, and the edges last (they keep working through a gateway
    cut, so they are stopped last)."""
    if name in _STOP_RANK:
        return (_STOP_RANK[name], "")
    if name.startswith("relay-"):
        return (3, name.removeprefix("relay-"))
    if name.startswith("edge-"):
        return (4, name.removeprefix("edge-"))
    return (5, name)


# --- pid and log files -------------------------------------------------


def pid_path(runs_dir: Path, name: str) -> Path:
    return runs_dir / "services" / f"{name}.pid"


def log_path(runs_dir: Path, name: str) -> Path:
    return runs_dir / "services" / f"{name}.log"


def read_pid(runs_dir: Path, name: str) -> int | None:
    """The pid recorded for `name`, or None when there is no readable
    pid file."""
    try:
        text = pid_path(runs_dir, name).read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def running_names(runs_dir: Path) -> list[str]:
    """Every name with a pid file, in down order - `down`'s work list
    when no names are given: what `up` started is what `down` stops,
    whatever cluster file either command was given."""
    services_dir = runs_dir / "services"
    try:
        names = [path.stem for path in services_dir.glob("*.pid")]
    except OSError:
        return []
    return sorted(names, key=_stop_key)


def log_tail(runs_dir: Path, name: str, max_lines: int = LOG_TAIL_LINES) -> str:
    """The last lines of a service's log, for failure messages."""
    try:
        lines = log_path(runs_dir, name).read_text(encoding="utf-8").splitlines()
    except OSError:
        return "(no log)"
    return "\n".join(lines[-max_lines:])


# --- process identity ---------------------------------------------------


def _proc_fields(pid: int) -> tuple[list[str], dict[str, str]] | None:
    """argv and environment of `pid`, or None when /proc cannot be read
    (no such process, another user's, or a platform without /proc)."""
    try:
        argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        environ = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
    except OSError:
        return None
    args = [part.decode(errors="replace") for part in argv if part]
    env: dict[str, str] = {}
    for entry in environ:
        if b"=" in entry:
            key, _, value = entry.partition(b"=")
            env[key.decode(errors="replace")] = value.decode(errors="replace")
    return args, env


def is_service(pid: int, spec: ServiceSpec) -> bool:
    """True if `pid` is the process `spec` describes: the interpreter
    arguments match AND every identity variable has the table's value.
    A reused pid running some other python fails both checks; a
    sibling service (same argv, different identity) fails the second."""
    fields = _proc_fields(pid)
    if fields is None:
        return False
    args, env = fields
    if len(args) < len(spec.args) or args[-len(spec.args) :] != list(spec.args):
        return False
    return all(env.get(key) == value for key, value in spec.env.items())


def _alive(pid: int) -> bool:
    """True if `pid` names a running process. Two processes that answer
    kill(pid, 0) must not count as alive: a zombie (exited, not yet
    reaped) and a dead-on-arrival pid - both have state Z in
    /proc/<pid>/stat. The state is what decides, NOT an empty cmdline:
    right after Popen returns, a child can still be inside execve, and
    its cmdline is empty for a moment then - reading it as dead made
    `up` report a starting service as exited-immediately and orphan
    it (its pid file was cleaned while it lived)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        state = stat.rpartition(")")[2].strip().split(" ")[0]
        return state not in ("Z", "X", "x")
    except OSError, IndexError:
        return False


# --- reachability -------------------------------------------------------


def port_in_use(port: int, host: str = HOST_ADDRESS) -> bool:
    """True if something is already listening on host:port. A refused
    connect means the port is free; a live listener - ours running
    without a pid file, or a foreign process - means `up` must not
    spawn a child that would just fail to bind."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


def dialed_ports(cluster: Cluster) -> list[int]:
    """Every port the spawned services will dial, deduplicated: their
    Toxiproxy proxies (the gateway, coordinator, HQ worker and region
    relays reach the databases through them) and the nodes' published
    ports (the edge APIs and region relays go direct to their own
    region's nodes). The Toxiproxy API on 8474 and `baseline` are not
    on any service's path, so neither is checked - the precheck dials
    what the children dial, nothing else."""
    ports = [proxy.port for proxy in proxies(cluster)]
    ports += [host_port(cluster, node) for node in cluster.nodes]
    return sorted(set(ports))


def port_labels(cluster: Cluster) -> dict[int, str]:
    """What each dialed port IS, so an unreachable port can be named:
    a proxy by its link (gateway__r2-primary), a node by its id
    (r2-primary). A bare port number says nothing about the cause."""
    labels = {proxy.port: f"{proxy.client}__{proxy.target}" for proxy in proxies(cluster)}
    for node in cluster.nodes:
        labels[host_port(cluster, node)] = node
    return labels


def unreachable_ports(
    ports: Sequence[int], host: str = HOST_ADDRESS, timeout_s: float = 1.0
) -> list[int]:
    """The ports in `ports` that nothing answers on. One TCP connect
    each, no semantic checks: whether the cluster is *loaded* is the
    children's business, and they say so in their logs."""
    out: list[int] = []
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout_s)
            if sock.connect_ex((host, port)) != 0:
                out.append(port)
    return out


def precheck_message(cluster: Cluster, unreachable: Sequence[int]) -> str:
    """The error for a cluster that does not answer: each unreachable
    port named by what it is, then the three usual causes. Bootstrap is
    the LAST resort in the list - it rebuilds from empty volumes, and
    the common causes (stopped containers, a fault test that left a
    proxy disabled) do not need it. Following a bare 'run bootstrap'
    hint there would throw away a run's orders."""
    labels = port_labels(cluster)
    shown = ", ".join(f"{labels.get(port, '?')} ({port})" for port in unreachable[:5])
    more = f" (+{len(unreachable) - 5} more)" if len(unreachable) > 5 else ""
    return (
        f"nothing answers on {HOST_ADDRESS}: {shown}{more} - the cluster is not "
        f"reachable. If the containers were stopped, bring them back with "
        f"docker compose -p restaurantos -f infra/generated/compose.yaml up -d "
        f"(stopping does not delete the data). If a fault test left a proxy "
        f"disabled, the named proxies above are the suspects: POST /reset on "
        f"the Toxiproxy API, or the sync in rosctl bootstrap, re-enables them. "
        f"If there is no cluster yet, run 'uv run rosctl bootstrap'. Pass "
        f"--no-precheck to start the services anyway."
    )


# --- spawning and stopping ----------------------------------------------


def child_env(spec: ServiceSpec, cluster_file: Path | None) -> dict[str, str]:
    """The environment a spawned service gets: the spawner's, with the
    identity keys stripped and re-applied from the table, plus
    ROS_NETWORK=host (the spawner, not the child, decides where it
    runs) and the cluster file `up` itself used, so the children agree
    with the shape they were started for."""
    env = dict(os.environ)
    for key in IDENTITY_KEYS:
        env.pop(key, None)
    env["ROS_NETWORK"] = "host"
    if cluster_file is not None:
        env["ROS_CLUSTER_FILE"] = str(Path(cluster_file).resolve())
    env.update(spec.env)
    return env


def dotenv_identity_warnings(env_path: Path = Path(".env")) -> list[str]:
    """The .env lines that also say who a process is (IDENTITY_KEYS or
    ROS_NETWORK). Every child reads .env, so a key the table does not
    set falls back to it - see IDENTITY_KEYS. The caller prints one
    warning per line returned."""
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError:
        return []
    out: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.partition("=")[0].strip().removeprefix("export ").strip()
        if key in IDENTITY_KEYS or key == "ROS_NETWORK":
            out.append(line)
    return out


def start_service(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    cluster_file: Path | None = None,
) -> Step:
    """Start one service as a detached child and record its pid. An
    already-running service is reported, not duplicated (idempotent
    `up`); a port someone else holds is reported, not fought over."""
    pid = read_pid(runs_dir, spec.name)
    if pid is not None:
        if _alive(pid) and is_service(pid, spec):
            return Step(spec.name, "already-running", f"pid {pid}, not started again")
        pid_path(runs_dir, spec.name).unlink(missing_ok=True)
    if spec.port is not None and port_in_use(spec.port):
        return Step(
            spec.name,
            "port-in-use",
            f"port {spec.port} already answers - already running without a "
            f"pid file, or another process holds it",
        )
    log_path(runs_dir, spec.name).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path(runs_dir, spec.name), "ab") as log:
        # one header line per start, so an appended log stays readable
        started = datetime.now(UTC).isoformat(timespec="seconds")
        identity = " ".join(f"{key}={value}" for key, value in sorted(spec.env.items()))
        command = " ".join([sys.executable, *spec.args])
        log.write(f"# {started} start {spec.name}: {command} ({identity})\n".encode())
        log.flush()
        process = subprocess.Popen(
            [sys.executable, *spec.args],
            env=child_env(spec, cluster_file),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path(runs_dir, spec.name).write_text(f"{process.pid}\n", encoding="utf-8")
    return Step(spec.name, "started", f"pid {process.pid}")


def _start_failure(spec: ServiceSpec, runs_dir: Path, outcome: str, reason: str = "") -> Step:
    """The failure step for a service that died or never became ready:
    the pid file is cleaned, the message carries the log tail (the
    cause is usually the last exception in it) and says the other
    services keep running."""
    pid_path(runs_dir, spec.name).unlink(missing_ok=True)
    detail = f"{reason} - see {log_path(runs_dir, spec.name)}".lstrip(" -")
    return Step(
        spec.name,
        outcome,
        f"{detail}; last log lines:\n{log_tail(runs_dir, spec.name)}\n"
        f"(the other services keep running - a second 'rosctl up' skips "
        f"what is already up)",
    )


def _ready_step(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    pid: int,
    ready_timeout_s: float,
    grace_s: float,
) -> Step:
    """Wait for a just-started service to be ready: an HTTP service
    answers its readiness route with 200 (a service that binds but
    fails inside startup, like a gateway whose directory read fails,
    never answers it); a relay or the HQ worker just has to survive
    its grace period, because their configuration errors exit within
    a second."""
    if spec.port is None:
        deadline = time.monotonic() + grace_s
        while time.monotonic() < deadline and _alive(pid):
            time.sleep(0.1)
        if not _alive(pid):
            return _start_failure(spec, runs_dir, "exited-immediately")
        return Step(
            spec.name,
            "started",
            f"pid {pid} alive after {grace_s}s (no HTTP port), log {log_path(runs_dir, spec.name)}",
        )
    url = f"http://{HOST_ADDRESS}:{spec.port}{spec.ready}"
    deadline = time.monotonic() + ready_timeout_s
    while time.monotonic() < deadline:
        if not _alive(pid):
            return _start_failure(spec, runs_dir, "exited-immediately")
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return Step(
                    spec.name,
                    "started",
                    f"pid {pid} ready on port {spec.port}, log {log_path(runs_dir, spec.name)}",
                )
        except httpx.HTTPError:
            pass
        time.sleep(0.25)
    return _start_failure(
        spec,
        runs_dir,
        "not-ready",
        f"still not answering {spec.ready} on port {spec.port} within {ready_timeout_s}s",
    )


def stop_service(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    stop_timeout_s: float = STOP_TIMEOUT_S,
    kill_timeout_s: float = KILL_TIMEOUT_S,
) -> Step:
    """Stop one service: SIGINT first (uvicorn and the relay's
    asyncio.run both shut down cleanly on it), then SIGKILL after the
    timeout. Only a process proven to be `spec` is signalled (see
    `is_service`); a missing, dead or foreign pid is reported and its
    file cleaned up. The control plane on 8002 has no pid file here,
    so `down` can never touch it."""
    pid = read_pid(runs_dir, spec.name)
    if pid is None:
        return Step(spec.name, "not-running", "no pid file")
    if not _alive(pid):
        pid_path(runs_dir, spec.name).unlink(missing_ok=True)
        return Step(spec.name, "not-running", f"pid {pid} was not running, pid file removed")
    if not is_service(pid, spec):
        pid_path(runs_dir, spec.name).unlink(missing_ok=True)
        return Step(
            spec.name,
            "stale-pid",
            f"pid {pid} is not {spec.name} (exited and the pid was reused, or "
            f"the file is wrong) - pid file removed, process left alone",
        )
    os.kill(pid, signal.SIGINT)
    deadline = time.monotonic() + stop_timeout_s
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    outcome, detail = "stopped", f"pid {pid}"
    if _alive(pid):
        os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + kill_timeout_s
        while _alive(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        if _alive(pid):
            return Step(
                spec.name,
                "unkillable",
                f"pid {pid} did not exit after SIGKILL, pid file left in place",
            )
        outcome = "killed"
        detail = f"pid {pid} did not answer SIGINT within {stop_timeout_s}s, killed"
    pid_path(runs_dir, spec.name).unlink(missing_ok=True)
    return Step(spec.name, outcome, detail)


def up_services(
    cluster: Cluster,
    *,
    runs_dir: Path,
    names: Sequence[str] | None = None,
    cluster_file: Path | None = None,
    precheck: bool = True,
    ready_timeout_s: float = READY_TIMEOUT_S,
    grace_s: float = RELAY_GRACE_S,
    specs: Sequence[ServiceSpec] | None = None,
) -> list[Step]:
    """Start every service (or the named ones) and wait for each to be
    ready. Precheck first, all of it before anything spawns: no
    half-started set to reason about afterwards. `specs` overrides the
    table - how the lifecycle tests drive a fake target instead of the
    real services."""
    require_proc()
    if precheck:
        unreachable = unreachable_ports(dialed_ports(cluster))
        if unreachable:
            raise PrecheckError(precheck_message(cluster, unreachable))
    table = list(specs) if specs is not None else service_specs(cluster)
    selected = select_specs(table, names)
    steps: list[Step] = []
    for spec in selected:
        step = start_service(spec, runs_dir=runs_dir, cluster_file=cluster_file)
        if step.outcome != "started":
            steps.append(step)
            continue
        pid = read_pid(runs_dir, spec.name)
        assert pid is not None  # just written by start_service
        steps.append(
            _ready_step(
                spec,
                runs_dir=runs_dir,
                pid=pid,
                ready_timeout_s=ready_timeout_s,
                grace_s=grace_s,
            )
        )
    return steps


def down_services(
    cluster: Cluster,
    *,
    runs_dir: Path,
    names: Sequence[str] | None = None,
    stop_timeout_s: float = STOP_TIMEOUT_S,
    specs: Sequence[ServiceSpec] | None = None,
) -> list[Step]:
    """Stop the services started by `up`. With no names, the work list
    is the pid files themselves, in down order (see `_stop_key`), each
    one's spec rebuilt from its name - so a plain `rosctl down` stops
    everything `up` started even when it was given a different
    cluster file. With names, they are looked up in the table (or, for
    a relay/edge of another cluster shape, built from the name). A pid
    file whose name matches no service pattern is reported and left
    alone."""
    require_proc()
    steps: list[Step] = []
    if names is None:
        for name in running_names(runs_dir):
            try:
                spec = spec_for_name(name)
            except KeyError:
                steps.append(
                    Step(name, "unknown", "pid file matches no service pattern - left alone")
                )
                continue
            steps.append(stop_service(spec, runs_dir=runs_dir, stop_timeout_s=stop_timeout_s))
        return steps
    table = {spec.name: spec for spec in (specs or service_specs(cluster))}
    for name in sorted(names, key=_stop_key):
        spec = table.get(name)
        if spec is None:
            try:
                spec = spec_for_name(name, cluster)
            except KeyError:
                valid = ", ".join(sorted(table))
                raise ValueError(f"no service {name} - valid names: {valid}") from None
        steps.append(stop_service(spec, runs_dir=runs_dir, stop_timeout_s=stop_timeout_s))
    return steps


def up_exit_code(steps: Sequence[Step]) -> int:
    """`up` fails when a service could not be started or never became
    ready; an already-running service is the idempotent case, not a
    failure."""
    bad = {"port-in-use", "exited-immediately", "not-ready"}
    return 1 if any(step.outcome in bad for step in steps) else 0


def down_exit_code(steps: Sequence[Step]) -> int:
    """`down` fails only when a process survived SIGKILL."""
    return 1 if any(step.outcome == "unkillable" for step in steps) else 0

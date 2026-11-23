"""The service table `rosctl up` starts from, and the host-mode start
and stop around it.

This module is the ONE place that knows what the services are (WP6a
brief: keep the list in one place so host mode and the later container
mode both read it). `service_specs` follows the "How each service
starts" table in architecture Section 4 exactly - same commands, same
identity environment, same ports from `common.cluster.service_port` -
and derives the per-region children (relay, edge API) from the loaded
cluster, so a 1-region and a 3-region cluster produce different child
sets from one table.

Supervision model (settled with Mark, 2026-11-23): detached children,
no supervisor parent. `up` spawns each service in its own session
(`start_new_session`), writes `runs/services/<name>.pid`, and returns;
the services outlive the terminal, and `rosctl up <name>` starting one
service for a debugger stays possible. `down` reads the pid files and
signals a process only after proving it is the service the pid file
names: same interpreter arguments AND the identity environment from the
table (two relay children share their argv, so the environment is what
tells them apart). A missing, dead or foreign pid is reported and the
file cleaned up - never killed, which is the guard against a reused
pid. The control plane on 8002 (WP6b) is never touched: `down` signals
only processes whose pid file names them.

Before spawning, `up` checks the cluster is reachable: one TCP connect
to each port the children themselves will dial - their Toxiproxy
proxies (the request path goes through them) and the nodes' published
ports (the edge APIs and region relays go direct to their own region).
Five children dying on connection errors is the worst first-run
experience; `--no-precheck` is the documented escape for deliberately
starting a service with no cluster.

Standard library only: no psutil, no new dependency.
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
from pathlib import Path

from restaurantos.common.cluster import (
    HOST_ADDRESS,
    Cluster,
    host_port,
    proxies,
    service_port,
)

RUNS_DIR = Path("runs")
"""Where playback writes run logs; the services' pid and log files go
to a `services` folder beside them."""

TERM_TIMEOUT_S = 10.0
"""How long `down` waits after SIGTERM before escalating to SIGKILL."""

KILL_TIMEOUT_S = 5.0
"""How long `down` waits after SIGKILL for the process to disappear."""

SETTLE_S = 0.5
"""How long `up` waits after spawning to notice a service that died
immediately (a misconfigured child exits within a second; saying so
beats a log file full of stack traces)."""

IDENTITY_KEYS = ("ROS_CLIENT", "ROS_MODE", "ROS_REGION_ID", "ROS_RELAY_ROLE")
"""The settings that say who a process is. `up` strips them from the
inherited environment before applying a service's own, so a leftover
shell variable can never leak into the wrong service."""


@dataclass(frozen=True)
class ServiceSpec:
    """One row of the architecture Section 4 table.

    `args` is the argv after the interpreter: a real service is
    ("-m", "<package>"), a test's fake target is ("-c", "<code>").
    Identity lives in `env`, never in arguments. `port` comes from
    `common.cluster.service_port`; the relays have none.
    """

    name: str
    args: tuple[str, ...]
    env: Mapping[str, str]
    port: int | None = None


@dataclass
class Step:
    """What `up` or `down` did to one service. The command layer prints
    these; the tests assert them."""

    name: str
    outcome: str
    detail: str


def service_specs(cluster: Cluster) -> list[ServiceSpec]:
    """Every host-mode service for this cluster's shape: the gateway,
    the coordinator and the HQ worker, then one region relay and one
    edge API per region (architecture Section 4)."""
    specs = [
        ServiceSpec(
            name="gateway",
            args=("-m", "restaurantos.gateway"),
            env={"ROS_MODE": "gateway", "ROS_CLIENT": "gateway"},
            port=service_port(cluster, "gateway"),
        ),
        ServiceSpec(
            name="coordinator",
            args=("-m", "restaurantos.coordinator"),
            env={"ROS_CLIENT": "coordinator"},
            port=service_port(cluster, "coordinator"),
        ),
        ServiceSpec(
            name="hq-worker",
            args=("-m", "restaurantos.relay"),
            env={"ROS_RELAY_ROLE": "hq", "ROS_CLIENT": "hq"},
        ),
    ]
    for rid in cluster.region_ids:
        specs += [
            ServiceSpec(
                name=f"relay-{rid}",
                args=("-m", "restaurantos.relay"),
                env={
                    "ROS_RELAY_ROLE": "region",
                    "ROS_CLIENT": f"relay-{rid}",
                    "ROS_REGION_ID": rid,
                },
            ),
            ServiceSpec(
                name=f"edge-{rid}",
                args=("-m", "restaurantos.gateway"),
                env={"ROS_MODE": "edge", "ROS_REGION_ID": rid},
                port=service_port(cluster, f"edge-{rid}"),
            ),
        ]
    return specs


def select_specs(specs: Sequence[ServiceSpec], names: Sequence[str] | None) -> list[ServiceSpec]:
    """The specs to act on: all of them, or the named ones. An unknown
    name is an error that lists the valid ones."""
    if not names:
        return list(specs)
    by_name = {spec.name: spec for spec in specs}
    unknown = [name for name in names if name not in by_name]
    if unknown:
        valid = ", ".join(sorted(by_name))
        raise ValueError(f"no service {', '.join(unknown)} - valid names: {valid}")
    return [by_name[name] for name in names]


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


# --- process identity ---------------------------------------------------


def _proc_fields(pid: int) -> tuple[list[str], dict[str, str]] | None:
    """argv and environment of `pid`, or None when /proc cannot be read
    (no such process, or another user's). Linux only, like the rest of
    the dev tooling."""
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
    """True if `pid` names a running process. A zombie (exited, not yet
    reaped) still answers kill(pid, 0) but has an empty cmdline, so it
    counts as dead."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        return bool(Path(f"/proc/{pid}/cmdline").read_bytes())
    except OSError:
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


class PrecheckError(Exception):
    """The cluster is not reachable; `rosctl bootstrap` is the fix."""


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
        process = subprocess.Popen(
            [sys.executable, *spec.args],
            env=child_env(spec, cluster_file),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path(runs_dir, spec.name).write_text(f"{process.pid}\n", encoding="utf-8")
    return Step(spec.name, "started", f"pid {process.pid}, log {log_path(runs_dir, spec.name)}")


def stop_service(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    term_timeout_s: float = TERM_TIMEOUT_S,
    kill_timeout_s: float = KILL_TIMEOUT_S,
) -> Step:
    """Stop one service: SIGTERM, wait, escalate to SIGKILL. Only a
    process proven to be `spec` is signalled (see `is_service`); a
    missing, dead or foreign pid is reported and its file cleaned up.
    The control plane on 8002 has no pid file here, so `down` can
    never touch it."""
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
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + term_timeout_s
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
        detail = f"pid {pid} did not answer SIGTERM within {term_timeout_s}s, killed"
    pid_path(runs_dir, spec.name).unlink(missing_ok=True)
    return Step(spec.name, outcome, detail)


def up_services(
    cluster: Cluster,
    *,
    runs_dir: Path,
    names: Sequence[str] | None = None,
    cluster_file: Path | None = None,
    precheck: bool = True,
    settle_s: float = SETTLE_S,
    specs: Sequence[ServiceSpec] | None = None,
) -> list[Step]:
    """Start every service (or the named ones). Precheck first, all of
    it before anything spawns: no half-started set to reason about
    afterwards. `specs` overrides the table - how the lifecycle tests
    drive a fake target instead of the real services."""
    if precheck:
        unreachable = unreachable_ports(dialed_ports(cluster))
        if unreachable:
            shown = ", ".join(str(port) for port in unreachable[:5])
            more = f" (+{len(unreachable) - 5} more)" if len(unreachable) > 5 else ""
            raise PrecheckError(
                f"nothing answers on {HOST_ADDRESS}:{shown}{more} - the cluster "
                f"is not reachable, run 'rosctl bootstrap' first (or pass "
                f"--no-precheck to start anyway)"
            )
    table = list(specs) if specs is not None else service_specs(cluster)
    selected = select_specs(table, names)
    steps = [start_service(spec, runs_dir=runs_dir, cluster_file=cluster_file) for spec in selected]
    if settle_s > 0:
        time.sleep(settle_s)
        for i, step in enumerate(steps):
            if step.outcome != "started":
                continue
            pid = read_pid(runs_dir, step.name)
            if pid is None or not _alive(pid):
                pid_path(runs_dir, step.name).unlink(missing_ok=True)
                steps[i] = Step(
                    step.name,
                    "exited-immediately",
                    f"the process died at once - see {log_path(runs_dir, step.name)}",
                )
    return steps


def down_services(
    cluster: Cluster,
    *,
    runs_dir: Path,
    names: Sequence[str] | None = None,
    term_timeout_s: float = TERM_TIMEOUT_S,
    specs: Sequence[ServiceSpec] | None = None,
) -> list[Step]:
    """Stop every service (or the named ones). `specs` overrides the
    table, as in `up_services`."""
    table = list(specs) if specs is not None else service_specs(cluster)
    selected = select_specs(table, names)
    return [
        stop_service(spec, runs_dir=runs_dir, term_timeout_s=term_timeout_s) for spec in selected
    ]


def up_exit_code(steps: Sequence[Step]) -> int:
    """`up` fails when a service could not be started; an
    already-running service is the idempotent case, not a failure."""
    return 1 if any(step.outcome in ("port-in-use", "exited-immediately") for step in steps) else 0


def down_exit_code(steps: Sequence[Step]) -> int:
    """`down` fails only when a process survived SIGKILL."""
    return 1 if any(step.outcome == "unkillable" for step in steps) else 0

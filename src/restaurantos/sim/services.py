"""The service table `rosctl up` starts from, and the host-mode start
and stop around it.

This module is the ONE place that knows what the services are (WP6a
brief: keep the list in one place so host mode and the later container
mode both read it). `spec_for_name` is the table: the names follow
fixed patterns (`gateway`, `coordinator`, `hq-worker`, `relay-r<n>`,
`edge-r<n>` with canonical region ids) with the commands, identity
environment and ports of architecture Section 4 - ports come from
`common.cluster`, never typed - and `service_specs` builds a whole
cluster's list from those same patterns. Container mode, when WP0's
image exists, reads the same patterns into the Compose generator.

Supervision model (settled with Mark, 2026-11-23): detached children,
no supervisor parent. `up` spawns each service in its own session
(`start_new_session`), writes `runs/services/<name>.pid` as
"<pid> <start_ticks>" (the kernel start time from /proc: the one fact
about a pid that never changes across execve, so a pid file names one
process, not one number the kernel might hand out again), and waits
for each one to be READY before reporting success - the HTTP services
answer their readiness route (/health, /admin/cluster), the relays and
the HQ worker survive a grace period. A service that never becomes
ready is STOPPED, not orphaned: a still-running child behind a removed
pid file would hold its port and `down` could not find it. Two `up`s
racing on one name are serialized by a lock file, so the second sees
`already-running`. `down` works from the pid files, not from a cluster
file: what `up` started is what `down` stops, even when the cluster
file has changed in between - which is why the specs are rebuilt from
the names.

A process is signalled only through a pidfd opened for the exact pid
that passed the identity proof (start ticks, then interpreter
arguments, then identity environment - two relay children share their
argv, so the environment is what tells them apart), never by bare pid
number: a reused pid cannot be signalled by mistake. A missing, dead
or foreign pid is reported and its file cleaned up - never killed. A
malformed pid file is reported and left in place for a person to look
at. Stop is SIGINT first (uvicorn and the relay's asyncio.run both
shut down cleanly on it), escalating to SIGKILL after a timeout; the
wait for exit polls the pidfd, not the pid. The control plane on 8002
(WP6b) is never touched: `down` signals only processes whose pid file
names them, and stops in down order - the coordinator before the
services it probes, the region side last.

Before spawning, `up` checks the cluster is reachable: one TCP connect
to each port the SELECTED services themselves dial (see `dials`) -
their Toxiproxy proxies (the request path goes through them) and the
nodes' published ports (the edge APIs and region relays go direct to
their own region). Replication proxies are dialed by postgres, not by
a service, so they are not checked. Five children dying on connection
errors is the worst first-run experience; `--no-precheck` is the
documented escape for deliberately starting a service with no cluster.

Linux or WSL2 only: the process checks read /proc, and `up`/`down`
refuse at once anywhere else. Standard library plus httpx (already a
dependency); no psutil.
"""

from __future__ import annotations

import contextlib
import os
import re
import select
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import httpx

from restaurantos.common.cluster import (
    HOST_ADDRESS,
    HQ_PRIMARY,
    SERVICE_PORTS,
    Cluster,
    host_port,
    primary_of,
    proxies,
    service_port,
    standby_of,
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
route. A service that binds later than this (or never) is stopped and
reported not-ready instead of started."""

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
`dotenv_identity_warnings`). ROS_NETWORK is not one of them: rosctl
always sets it itself, and "Running things" tells everyone to put it
in .env, so warning on it would flag every developer on every run."""

LOG_TAIL_LINES = 20
"""Log lines shown in an up failure message, so the message names the
cause instead of pointing at a file."""

_REGION_ID = re.compile(r"r[1-9][0-9]*")
"""A canonical region id: r1, r2, ... r10 - never r0, r01 or r1x."""


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
    these; the tests assert them. `pid` and `start_ticks` are set
    wherever a process was spawned: together they name the launch, so
    a later cleanup can act only on THAT launch even after the pid
    file has been cleaned up or replaced."""

    name: str
    outcome: str
    detail: str
    pid: int | None = None
    start_ticks: int | None = None


def _region_of(name: str) -> str | None:
    """The canonical region id inside a relay-/edge- name, or None when
    there is none (`relay-`, `relay-r0`, `edge-r01`, `relay-not-a-region`)."""
    for prefix in ("relay-", "edge-"):
        if name.startswith(prefix):
            rid = name.removeprefix(prefix)
            return rid if _REGION_ID.fullmatch(rid) else None
    return None


def spec_for_name(name: str, cluster: Cluster | None = None) -> ServiceSpec:
    """The service a NAME implies, from the fixed patterns: gateway,
    coordinator, hq-worker, relay-r<n>, edge-r<n> with canonical region
    ids. The region id of a relay or edge is part of its name, so a
    spec can be built from a pid file alone (`down` passes no cluster);
    the port is filled in only when a cluster is given and the name has
    one. A name that matches no pattern raises KeyError.

    This is the one table: `service_specs` builds every cluster's list
    from these same patterns, so host mode and the later container mode
    cannot drift apart."""
    rid = _region_of(name)
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
    if name.startswith("relay-") and rid is not None:
        return ServiceSpec(
            name,
            ("-m", "restaurantos.relay"),
            {
                "ROS_RELAY_ROLE": "region",
                "ROS_CLIENT": name,
                "ROS_REGION_ID": rid,
            },
        )
    if name.startswith("edge-") and rid is not None:
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


def read_pid_record(runs_dir: Path, name: str) -> tuple[int, int] | str | None:
    """What the pid file for `name` says: `(pid, start_ticks)` when it
    is well-formed, a REASON string when it is malformed or cannot be
    read, or None when there is no pid file. Missing is NOT the same
    as unreadable: a permission problem or a directory at the path is
    a reason string (down reports it; up replaces it, because up owns
    its records). The bytes are decoded here because a binary pid file
    raises UnicodeDecodeError (a ValueError, not an OSError) on
    read_text - which once ended a whole `down` sweep with a traceback
    instead of reporting one bad file. The values are checked too: a
    pid outside pid_t's range (Linux pid_t is a signed 32-bit int) or
    negative start ticks are impossible - pid 0 in particular makes
    os.kill(0, 0) check the caller's own process group, so it would
    look alive."""
    try:
        raw = pid_path(runs_dir, name).read_bytes()
    except FileNotFoundError:
        return None
    except OSError as error:
        return f"unreadable ({error})"
    try:
        text = raw.decode("utf-8").strip()
    except UnicodeDecodeError as error:
        return f"not valid utf-8 ({error})"
    parts = text.split()
    if len(parts) == 2:
        try:
            pid, ticks = int(parts[0]), int(parts[1])
        except ValueError:
            pass
        else:
            if 1 <= pid <= 2**31 - 1 and ticks >= 0:
                return pid, ticks
            return f"impossible values (pid {parts[0]}, start_ticks {parts[1]})"
    return f"expected 'pid start_ticks', found {text!r}"


def _write_pid_file(path: Path, pid: int, start_ticks: int) -> None:
    """Atomically, so a concurrent reader never sees a half-written
    file: a temp file beside it, then rename into place. A failed
    rename removes the temp file, so nothing partial is left behind."""
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(f"{pid} {start_ticks}\n", encoding="utf-8")
    try:
        os.replace(tmp, path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def running_names(runs_dir: Path) -> list[str]:
    """Every name with a pid file, in down order - `down`'s work list
    when no names are given: what `up` started is what `down` stops,
    whatever cluster file either command was given. A MISSING folder
    is simply empty; a folder that cannot be listed raises, because
    "nothing running" would be a lie while services run (Path.glob
    swallows the error, so this reads the directory itself)."""
    services_dir = runs_dir / "services"
    try:
        with os.scandir(services_dir) as entries:
            names = [
                entry.name.removesuffix(".pid") for entry in entries if entry.name.endswith(".pid")
            ]
    except FileNotFoundError:
        return []
    return sorted(names, key=_stop_key)


def log_tail(runs_dir: Path, name: str, max_lines: int = LOG_TAIL_LINES) -> str:
    """The last lines of a service's log, for failure messages. The
    bytes are decoded with replacement characters, because a failure
    message must never end the up sweep with a UnicodeDecodeError of
    its own."""
    try:
        text = log_path(runs_dir, name).read_bytes().decode("utf-8", errors="replace")
    except OSError:
        return "(no log)"
    return "\n".join(text.splitlines()[-max_lines:])


# --- process identity ---------------------------------------------------


def _start_ticks(pid: int) -> int:
    """Field 22 of /proc/<pid>/stat: the kernel's start time for the
    process, in clock ticks. Read right after Popen returns: it is set
    when the process is created and never changes across execve, so it
    is the one fact that distinguishes this process from a future
    reuser of the same pid number."""
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    return int(stat.rpartition(")")[2].split()[19])


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


def identity(
    pid: int, start_ticks: int, spec: ServiceSpec
) -> Literal["match", "mismatch", "unknown"]:
    """Whether `pid` is the exact process the pid file recorded for
    `spec` - three answers, because "cannot tell" must not be read as
    "not ours". Three proofs, in order of what they catch: the start
    ticks (a pid handed out again to a different process fails here,
    even one with the same argv and environment from a second checkout
    of the repo), the interpreter arguments, and every identity
    variable (two relay children share argv, so the environment is
    what tells them apart). A tick mismatch is a `mismatch`; an
    unreadable /proc is an `unknown`. When the ticks match but argv or
    environment is empty or unreadable, the process can simply be
    mid-execve or just starting: retry every 50 ms for up to 1 s, then
    answer `unknown` - never `mismatch` on missing evidence."""
    try:
        if _start_ticks(pid) != start_ticks:
            return "mismatch"
    except OSError, IndexError, ValueError:
        return "unknown"
    deadline = time.monotonic() + 1.0
    while True:
        fields = _proc_fields(pid)
        if fields is not None:
            args, env = fields
            if args and env:
                if len(args) < len(spec.args) or args[-len(spec.args) :] != list(spec.args):
                    return "mismatch"
                if not all(env.get(key) == value for key, value in spec.env.items()):
                    return "mismatch"
                return "match"
        # Empty or unreadable argv/environ with matching start ticks is
        # mid-exec - or a process DYING: the kernel releases the cmdline
        # and environ BEFORE the state becomes Z, so a just-killed child
        # can show empty evidence with state R for a moment. The state
        # decides between them: Z (or the entry gone) means the launch
        # this record named has exited, which is a mismatch, not an
        # unknown.
        try:
            state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            state = state.rpartition(")")[2].strip().split(" ")[0]
        except FileNotFoundError:
            return "mismatch"  # gone since the ticks matched
        except OSError, IndexError:
            state = "?"  # unreadable mid-loop: keep retrying
        if state in ("Z", "X", "x"):
            return "mismatch"  # it exited while we were looking
        if time.monotonic() >= deadline:
            return "unknown"
        time.sleep(0.05)


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


def _liveness(pid: int) -> Literal["alive", "dead", "unknown"]:
    """Whether `pid` names a running process - three answers, because
    "cannot tell" must not be read as "not running". `dead`: the pid
    does not answer kill(pid, 0), or its state is a zombie (exited,
    not reaped) or dying. `alive`: the state was read and is anything
    else, or the process belongs to another user (PermissionError
    means it exists). `unknown`: any other problem reading /proc - a
    hidepid mount, say, or a stat entry that went missing while the
    pid still answers (a missing entry ALONE is not proof of exit:
    the kernel is asked again, and only a pid that no longer answers
    is dead). `start` and `stop` both branch on this FIRST, before
    the identity proofs: a `dead` record is simply replaced by the
    next `up` (the most common use after a crash), and an `unknown`
    one is reported, never acted on."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "dead"
    except PermissionError:
        return "alive"  # another user's process: it exists
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        # a missing stat is not proof of exit: the entry may be
        # unreadable rather than gone. Ask the kernel once more - only
        # a pid that no longer answers kill(0) between the two checks
        # has exited; anything else (still answers, or another user's)
        # is unknown, never dead
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return "dead"
        return "unknown"
    except OSError:
        return "unknown"
    try:
        state = stat.rpartition(")")[2].strip().split(" ")[0]
    except IndexError:
        return "unknown"
    if state in ("Z", "X", "x"):
        return "dead"
    return "alive"


def _probe_ready(spec: ServiceSpec, timeout_s: float) -> bool:
    """One readiness probe, no cleanup and no liveness checks: wait up
    to timeout_s for the readiness route to answer 200, with the
    request timeout always the time REMAINING so the wait ends at its
    deadline, never at the server's leisure. `_ready_step` slices it
    into short attempts (it must watch the pid between them); a second
    `up` gives it the whole budget to decide whether a running HTTP
    service is really ready. Never reads the body: the status line is
    the readiness answer, and a 200 after a slow answer is still
    ready."""
    url = f"http://{HOST_ADDRESS}:{spec.port}{spec.ready}"
    with httpx.Client(trust_env=False) as client:
        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            timeout = httpx.Timeout(min(1.0, remaining))
            try:
                with client.stream("GET", url, timeout=timeout) as response:
                    if response.status_code == 200:
                        return True
            except httpx.HTTPError:
                pass
            time.sleep(0.25)


# --- reachability -------------------------------------------------------


def port_in_use(port: int, host: str = HOST_ADDRESS) -> bool:
    """True if something is already listening on host:port. A refused
    connect means the port is free; a live listener - ours running
    without a pid file, or a foreign process - means `up` must not
    spawn a child that would just fail to bind."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


def dials(spec: ServiceSpec, cluster: Cluster) -> list[int]:
    """Every port this ONE service connects to, derived from the
    services' own connection calls (not guessed from the proxy list):

    - gateway: its Toxiproxy proxies - client `gateway` (gateway/app.py
      reaches HQ and the regions through them, direct=False);
    - coordinator: its proxies - client `coordinator`;
    - hq-worker: its proxies - client `hq` (relay/menu.py, holding.py
      reach the region nodes through them) - plus HQ's published port
      (relay/holding.py dials hq-primary with direct=True);
    - relay-r<n>: its proxies - client `relay-r<n>` (relay/worker.py
      reaches HQ through them) - plus r<n>'s published node ports
      (relay/worker.py dials its own region with direct=True, primary
      or standby after a promotion);
    - edge-r<n>: r<n>'s published node ports only (gateway/app.py in
      edge mode dials its region directly and keeps no HQ directory).

    The database-to-database replication proxies (a standby to its
    primary) are dialed by postgres, not by a service, so they are not
    here; neither is the Toxiproxy API (8474) or `baseline`."""

    def proxy_ports(client: str) -> list[int]:
        return [proxy.port for proxy in proxies(cluster) if proxy.client == client]

    def region_node_ports(rid: str) -> list[int]:
        wanted = {primary_of(rid)}
        if cluster.standbys:
            wanted.add(standby_of(rid))
        return [host_port(cluster, node) for node in cluster.nodes if node in wanted]

    rid = _region_of(spec.name)
    if spec.name == "gateway":
        return proxy_ports("gateway")
    if spec.name == "coordinator":
        return proxy_ports("coordinator")
    if spec.name == "hq-worker":
        return proxy_ports("hq") + [host_port(cluster, HQ_PRIMARY)]
    if spec.name.startswith("relay-") and rid is not None:
        return proxy_ports(f"relay-{rid}") + region_node_ports(rid)
    if spec.name.startswith("edge-") and rid is not None:
        return region_node_ports(rid)
    return []


def dialed_ports(cluster: Cluster) -> list[int]:
    """Every port the WHOLE service table dials, deduplicated: the
    union of `dials` over `service_specs(cluster)`. `up` itself checks
    only what the SELECTED services dial; this is the all-services
    view the tests pin."""
    ports: set[int] = set()
    for spec in service_specs(cluster):
        ports.update(dials(spec, cluster))
    return sorted(ports)


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
    port named by what it is, then the usual causes. The disabled-proxy
    case names the exact reset command - pointing at `rosctl bootstrap`
    there would send someone to the command that wipes the volumes.
    Bootstrap is named only for the no-cluster case, where rebuilding
    from empty volumes is genuinely the fix."""
    labels = port_labels(cluster)
    shown = ", ".join(f"{labels.get(port, '?')} ({port})" for port in unreachable[:5])
    more = f" (+{len(unreachable) - 5} more)" if len(unreachable) > 5 else ""
    return (
        f"nothing answers on {HOST_ADDRESS}: {shown}{more} - the cluster is not "
        f"reachable. If the containers were stopped, bring them back with "
        f"docker compose -p restaurantos -f infra/generated/compose.yaml up -d "
        f"(stopping does not delete the data). If a fault test left a proxy "
        f"disabled, the named proxies above are the suspects: "
        f"curl -X POST http://127.0.0.1:8474/reset re-enables every proxy and "
        f"removes every toxic. If there is no cluster yet, run "
        f"'uv run rosctl bootstrap' (it rebuilds from empty volumes). Pass "
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
    """The .env lines that also say who a process is (IDENTITY_KEYS).
    Every child reads .env, so a key the table does not set falls back
    to it - see IDENTITY_KEYS. ROS_NETWORK is NOT reported: rosctl
    always sets it itself, so it can never leak into a service. The
    caller prints one warning per line returned."""
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
        if key in IDENTITY_KEYS:
            out.append(line)
    return out


@contextlib.contextmanager
def _service_lock(runs_dir: Path, name: str) -> Iterator[None]:
    """The per-service lock, held by every code path that reads a pid
    record and then changes it: `start_service`, the whole of
    `stop_service` (the SIGINT and SIGKILL waits included - at most
    STOP_TIMEOUT_S + KILL_TIMEOUT_S, 15 s, which merely delays a
    concurrent `up` for that name; it then finds no record and starts
    a fresh child), and the cleanup in `_start_failure`. With no other
    writer during a stop, every unlink in `stop_service` can only
    remove the record it read. flock locks belong to the open file, so
    a second `_service_lock` for the SAME name inside the same process
    would wait for itself forever: never call one locked function from
    inside another - today `_ready_step` calls `stop_service` only
    after `start_service` has returned, which keeps the two apart.
    fcntl is imported here, not at module top: native Windows has no
    fcntl, and the module must import there so rosctl can print its
    Linux/WSL2 message instead of an ImportError traceback."""
    import fcntl

    lock_path = runs_dir / "services" / f"{name}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _drop_record(runs_dir: Path, name: str) -> Step | None:
    """Remove a pid record `up` no longer obeys - one guard for every
    branch that drops one (malformed, dead, mismatch): a record that
    cannot be removed must stop the start (`launch-failed`, nothing
    spawns) rather than leave a new child running beside a record
    that names a dead launch. Returns None on success."""
    try:
        pid_path(runs_dir, name).unlink(missing_ok=True)
    except OSError as error:
        return Step(
            name,
            "launch-failed",
            f"could not remove the record {pid_path(runs_dir, name)}: {error}",
        )
    return None


def start_service(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    cluster_file: Path | None = None,
    ready_timeout_s: float = READY_TIMEOUT_S,
) -> Step:
    """Start one service as a detached child and record its pid. The
    start of one name is serialized by the per-service lock, so two
    `up`s at once cannot both spawn the same service - the second
    waits and sees `already-running`, or `still-starting` when the
    first launch is too young for /proc to prove anything about. An
    already-running service is reported, not duplicated (idempotent
    `up`); a port someone else holds is reported, not fought over. Up
    owns its pid files: a malformed or unreadable record is replaced
    (when even that fails, `launch-failed` and nothing is spawned - a
    service must never run without its record). An I/O failure (log
    folder, log file, the run itself, the pid file) is a
    `launch-failed` step that names the path - never an exception that
    ends the whole sweep; a child whose pid file could not be written
    is killed and reaped, because it must not outlive the record of
    it."""
    services = runs_dir / "services"
    try:
        services.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        return Step(spec.name, "launch-failed", f"could not prepare {services}: {error}")
    stack = contextlib.ExitStack()
    try:
        stack.enter_context(_service_lock(runs_dir, spec.name))
    except OSError as error:
        with contextlib.suppress(Exception):
            stack.close()
        return Step(
            spec.name,
            "launch-failed",
            f"could not open the lock {runs_dir / 'services' / f'{spec.name}.lock'}: {error}",
        )
    with stack:
        record = read_pid_record(runs_dir, spec.name)
        if isinstance(record, str):
            # up owns its own pid file: a malformed or unreadable one
            # is replaced, not obeyed (down leaves it for a person)
            failed = _drop_record(runs_dir, spec.name)
            if failed is not None:
                return failed
        elif record is not None:
            pid, ticks = record
            how = _liveness(pid)
            if how == "dead":
                # the recorded launch has exited: up restarts it - the
                # most common way up is used, after a crash
                failed = _drop_record(runs_dir, spec.name)
                if failed is not None:
                    return failed
            elif how == "unknown":
                return Step(
                    spec.name,
                    "still-starting",
                    "another launch is still coming up - not started again",
                )
            else:
                who = identity(pid, ticks, spec)
                if who == "match":
                    # running - but an HTTP service must really answer
                    # its readiness route before up calls it started;
                    # the caller's ready timeout decides how long the
                    # re-probe may take, not the module constant
                    if spec.port is not None and not _probe_ready(spec, ready_timeout_s):
                        return Step(
                            spec.name,
                            "running-not-ready",
                            f"pid {pid} is running but not answering {spec.ready} on "
                            f"port {spec.port} within {ready_timeout_s}s - left running, "
                            f"its record kept; run rosctl down {spec.name} then "
                            f"rosctl up {spec.name} to restart it",
                            pid=pid,
                            start_ticks=ticks,
                        )
                    return Step(
                        spec.name,
                        "already-running",
                        f"pid {pid}, not started again",
                        pid=pid,
                        start_ticks=ticks,
                    )
                if who == "unknown":
                    return Step(
                        spec.name,
                        "still-starting",
                        "another launch is still coming up - not started again",
                    )
                failed = _drop_record(runs_dir, spec.name)
                if failed is not None:
                    return failed
        if spec.port is not None and port_in_use(spec.port):
            return Step(
                spec.name,
                "port-in-use",
                f"port {spec.port} already answers - already running without a "
                f"pid file, or another process holds it",
            )
        try:
            with open(log_path(runs_dir, spec.name), "ab") as log:
                # one header line per start, so an appended log stays readable
                started = datetime.now(UTC).isoformat(timespec="seconds")
                identity_line = " ".join(
                    f"{key}={value}" for key, value in sorted(spec.env.items())
                )
                command = " ".join([sys.executable, *spec.args])
                log.write(f"# {started} start {spec.name}: {command} ({identity_line})\n".encode())
                log.flush()
                try:
                    process = subprocess.Popen(
                        [sys.executable, *spec.args],
                        env=child_env(spec, cluster_file),
                        stdin=subprocess.DEVNULL,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                except OSError as error:
                    return Step(spec.name, "launch-failed", f"could not run {command}: {error}")
        except OSError as error:
            return Step(
                spec.name,
                "launch-failed",
                f"could not write the service log {log_path(runs_dir, spec.name)}: {error}",
            )
        try:
            # the start time is read before the exec completes and never
            # changes across it
            ticks = _start_ticks(process.pid)
            _write_pid_file(pid_path(runs_dir, spec.name), process.pid, ticks)
        except (OSError, IndexError, ValueError) as error:
            process.kill()
            process.wait()
            pid_path(runs_dir, spec.name).unlink(missing_ok=True)
            return Step(
                spec.name,
                "launch-failed",
                f"could not write {pid_path(runs_dir, spec.name)}: {error}",
                pid=process.pid,
            )
        return Step(
            spec.name,
            "started",
            f"pid {process.pid}",
            pid=process.pid,
            start_ticks=ticks,
        )


def _start_failure(
    spec: ServiceSpec,
    runs_dir: Path,
    outcome: str,
    reason: str = "",
    *,
    pid: int | None = None,
    start_ticks: int | None = None,
) -> Step:
    """The failure step for a service that died before becoming ready.
    The pid file is cleaned only when it still names THIS launch
    (pid and start ticks): a record that was replaced meanwhile belongs
    to another launch and is left for it. The message carries the log
    tail (the cause is usually the last exception in it) and says the
    other services keep running."""
    with _service_lock(runs_dir, spec.name):
        record = read_pid_record(runs_dir, spec.name)
        if pid is not None and start_ticks is not None:
            if record == (pid, start_ticks):
                pid_path(runs_dir, spec.name).unlink(missing_ok=True)
        elif record is not None:
            pid_path(runs_dir, spec.name).unlink(missing_ok=True)
    detail = f"{reason} - see {log_path(runs_dir, spec.name)}".lstrip(" -")
    return Step(
        spec.name,
        outcome,
        f"{detail}; last log lines:\n{log_tail(runs_dir, spec.name)}\n"
        f"(the other services keep running - a second 'rosctl up' skips "
        f"what is already up)",
        pid=pid,
        start_ticks=start_ticks,
    )


def _ready_step(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    pid: int,
    start_ticks: int,
    ready_timeout_s: float,
    grace_s: float,
) -> Step:
    """Wait for a just-started service to be ready: an HTTP service
    answers its readiness route with 200 (a service that binds but
    fails inside startup, like a gateway whose directory read fails,
    never answers it); a relay or the HQ worker just has to survive its
    grace period, because their configuration errors exit within a
    second. A service that never becomes ready is STOPPED, not
    orphaned: it holds its port, and `down` could not find it behind a
    removed pid file. The cleanup acts only on the launch it started
    (`expect`): a pid file that was replaced meanwhile names another
    launch and is left alone. The probe stays inside the deadline (its
    timeout is the time remaining, never a fixed 1 s) and never reads
    the body - the status line is the readiness answer, and a 200
    after a slow answer is still ready, not a reason to stop a healthy
    service."""
    if spec.port is None:
        deadline = time.monotonic() + grace_s
        while time.monotonic() < deadline and _alive(pid):
            time.sleep(0.1)
        if not _alive(pid):
            return _start_failure(
                spec, runs_dir, "exited-immediately", pid=pid, start_ticks=start_ticks
            )
        return Step(
            spec.name,
            "started",
            f"pid {pid} alive after {grace_s}s (no HTTP port), log {log_path(runs_dir, spec.name)}",
            pid=pid,
            start_ticks=start_ticks,
        )
    deadline = time.monotonic() + ready_timeout_s
    while True:
        if not _alive(pid):
            return _start_failure(
                spec, runs_dir, "exited-immediately", pid=pid, start_ticks=start_ticks
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        # one sliced probe between liveness checks (_probe_ready does
        # no cleanup and takes its own remaining-time budget)
        if _probe_ready(spec, min(1.0, remaining)):
            return Step(
                spec.name,
                "started",
                f"pid {pid} ready on port {spec.port}, log {log_path(runs_dir, spec.name)}",
                pid=pid,
                start_ticks=start_ticks,
            )
        time.sleep(0.25)
    stop = stop_service(spec, runs_dir=runs_dir, expect=(pid, start_ticks))
    base = f"still not answering {spec.ready} on port {spec.port} within {ready_timeout_s}s"
    if stop.outcome in ("stopped", "killed"):
        tail = f"{base} - the service was stopped ({stop.outcome}), pid file removed"
    elif stop.outcome in ("not-running", "replaced"):
        tail = f"{base} - nothing to clean ({stop.outcome}: {stop.detail})"
    else:
        tail = f"{base} - could not stop it ({stop.outcome}: {stop.detail}); pid file kept"
    return Step(
        spec.name,
        "not-ready",
        f"{tail}; last log lines:\n{log_tail(runs_dir, spec.name)}\n"
        f"(the other services keep running - a second 'rosctl up' skips "
        f"what is already up)",
        pid=pid,
        start_ticks=start_ticks,
    )


def _wait_pidfd(fd: int, timeout_s: float) -> bool:
    """True if the process behind `fd` exited within the timeout: a
    pidfd becomes readable when its process exits, so the wait is one
    poll, not a loop of pid-number lookups that a recycled pid could
    confuse."""
    poller = select.poll()
    poller.register(fd, select.POLLIN)
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if poller.poll(remaining * 1000):
            return True


def stop_service(
    spec: ServiceSpec,
    *,
    runs_dir: Path,
    stop_timeout_s: float = STOP_TIMEOUT_S,
    kill_timeout_s: float = KILL_TIMEOUT_S,
    expect: tuple[int, int] | None = None,
) -> Step:
    """Stop one service: SIGINT first (uvicorn and the relay's
    asyncio.run both shut down cleanly on it), then SIGKILL after the
    timeout, all through a pidfd opened for the exact pid that passed
    the identity proof - never by bare pid number, so a pid reused
    between the check and the signal cannot be hit. The per-service
    lock is held for the whole stop (see `_service_lock`), so nothing
    else can write a new record mid-stop. `expect`, when given, is the
    launch the CALLER started: a record that is missing or names
    another launch is `replaced` - signalled and unlinked nothing. A
    missing, dead or foreign pid is reported and its file cleaned up; a
    malformed or unreadable pid file is reported and left in place for
    a person to look at; an identity that cannot be decided (mid-exec,
    unreadable /proc) is `identity-unknown`, kept and signalled
    nothing. A process that vanishes mid-stop was the goal all along:
    stopped. A platform without pidfds (kernels older than 5.3; WSL2 is
    5.15 or newer) reports `cannot-signal` and never falls back to
    os.kill - signalling a number that was only just proven is the one
    thing this design exists to prevent. The control plane on 8002 has
    no pid file here, so `down` can never touch it."""
    stack = contextlib.ExitStack()
    try:
        stack.enter_context(_service_lock(runs_dir, spec.name))
    except OSError as error:
        with contextlib.suppress(Exception):
            stack.close()
        return Step(
            spec.name,
            "lock-failed",
            f"could not open the lock {runs_dir / 'services' / f'{spec.name}.lock'}: "
            f"{error} - record kept, nothing signalled",
        )
    with stack:
        record = read_pid_record(runs_dir, spec.name)
        if expect is not None and record != expect:
            return Step(
                spec.name,
                "replaced",
                "the pid file now names another launch - left alone",
            )
        if isinstance(record, str):
            return Step(
                spec.name,
                "malformed-pid-file",
                f"{record} - pid file left in place so it can be inspected",
            )
        if record is None:
            return Step(spec.name, "not-running", "no pid file")
        pid, ticks = record
        how = _liveness(pid)
        if how == "dead":
            pid_path(runs_dir, spec.name).unlink(missing_ok=True)
            return Step(spec.name, "not-running", f"pid {pid} was not running, pid file removed")
        if how == "unknown":
            return Step(
                spec.name,
                "identity-unknown",
                f"pid {pid}: could not tell whether it is running (/proc could not "
                f"be read) - record kept, nothing signalled",
            )
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            # vanished between the alive check and the open: gone is the goal
            pid_path(runs_dir, spec.name).unlink(missing_ok=True)
            return Step(spec.name, "stopped", f"pid {pid} exited while down was stopping it")
        except OSError as error:
            return Step(
                spec.name,
                "cannot-signal",
                f"pid {pid}: could not open a pidfd ({error}) - pid file left in place",
            )
        try:
            who = identity(pid, ticks, spec)
            if who == "unknown":
                return Step(
                    spec.name,
                    "identity-unknown",
                    f"pid {pid}: could not prove it is {spec.name} (still starting, "
                    f"or /proc could not be read) - record kept, nothing signalled",
                )
            if who == "mismatch":
                pid_path(runs_dir, spec.name).unlink(missing_ok=True)
                return Step(
                    spec.name,
                    "stale-pid",
                    f"pid {pid} is not {spec.name} (exited and the pid was reused, or "
                    f"the file is wrong) - pid file removed, process left alone",
                )
            try:
                signal.pidfd_send_signal(fd, signal.SIGINT)
            except ProcessLookupError:
                pid_path(runs_dir, spec.name).unlink(missing_ok=True)
                return Step(spec.name, "stopped", f"pid {pid} exited while down was stopping it")
            if _wait_pidfd(fd, stop_timeout_s):
                pid_path(runs_dir, spec.name).unlink(missing_ok=True)
                return Step(spec.name, "stopped", f"pid {pid}")
            try:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
            except ProcessLookupError:
                pid_path(runs_dir, spec.name).unlink(missing_ok=True)
                return Step(spec.name, "stopped", f"pid {pid} exited while down was stopping it")
            if _wait_pidfd(fd, kill_timeout_s):
                pid_path(runs_dir, spec.name).unlink(missing_ok=True)
                return Step(
                    spec.name,
                    "killed",
                    f"pid {pid} did not answer SIGINT within {stop_timeout_s}s, killed",
                )
            return Step(
                spec.name,
                "unkillable",
                f"pid {pid} did not exit after SIGKILL, pid file left in place",
            )
        finally:
            os.close(fd)


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
    ready. The selection happens first, then the precheck dials only
    what the SELECTED services dial, all of it before anything spawns:
    no half-started set to reason about afterwards. `specs` overrides
    the table - how the lifecycle tests drive a fake target instead of
    the real services."""
    require_proc()
    table = list(specs) if specs is not None else service_specs(cluster)
    selected = select_specs(table, names)
    if precheck:
        wanted = sorted({port for spec in selected for port in dials(spec, cluster)})
        unreachable = unreachable_ports(wanted)
        if unreachable:
            raise PrecheckError(precheck_message(cluster, unreachable))
    steps: list[Step] = []
    for spec in selected:
        step = start_service(
            spec,
            runs_dir=runs_dir,
            cluster_file=cluster_file,
            ready_timeout_s=ready_timeout_s,
        )
        steps.append(step)
        if step.outcome != "started":
            continue
        assert step.pid is not None and step.start_ticks is not None  # set on the spawn
        steps[-1] = _ready_step(
            spec,
            runs_dir=runs_dir,
            pid=step.pid,
            start_ticks=step.start_ticks,
            ready_timeout_s=ready_timeout_s,
            grace_s=grace_s,
        )
    return steps


def down_services(
    cluster: Cluster | None = None,
    *,
    runs_dir: Path,
    names: Sequence[str] | None = None,
    stop_timeout_s: float = STOP_TIMEOUT_S,
    specs: Sequence[ServiceSpec] | None = None,
) -> list[Step]:
    """Stop the services started by `up`. With no names, the work list
    is the pid files themselves, in down order (see `_stop_key`), each
    one's spec rebuilt from its name - so a plain `rosctl down` stops
    everything `up` started even when it was given a different cluster
    file, and needs no cluster file at all. With names, EVERY name is
    resolved to a spec first (nothing is stopped if any name is
    invalid), then they are stopped in down order. A pid file whose
    name matches no service pattern is reported and left alone."""
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
    rows: Sequence[ServiceSpec]
    if specs is not None:
        rows = specs
    elif cluster is not None:
        rows = service_specs(cluster)
    else:
        rows = []
    table = {spec.name: spec for spec in rows}
    resolved: list[ServiceSpec] = []
    for name in names:
        spec = table.get(name)
        if spec is None:
            if cluster is None:
                valid = ", ".join(sorted(table))
                raise ValueError(
                    f"no service {name} - valid names: {valid or '(no table to name them)'}"
                ) from None
            try:
                spec = spec_for_name(name, cluster)
            except KeyError:
                valid = ", ".join(sorted(table))
                raise ValueError(f"no service {name} - valid names: {valid}") from None
        resolved.append(spec)
    for spec in sorted(resolved, key=lambda s: _stop_key(s.name)):
        steps.append(stop_service(spec, runs_dir=runs_dir, stop_timeout_s=stop_timeout_s))
    return steps


def up_exit_code(steps: Sequence[Step]) -> int:
    """`up` fails when a service could not be started, could not be
    launched, never became ready, or is running but not answering its
    readiness route; an already-running AND ready service is the
    idempotent case, not a failure."""
    bad = {"port-in-use", "exited-immediately", "not-ready", "launch-failed", "running-not-ready"}
    return 1 if any(step.outcome in bad for step in steps) else 0


def down_exit_code(steps: Sequence[Step]) -> int:
    """`down` fails when it left something it could not deal with: a
    process that survived SIGKILL, one it could not signal at all (no
    pidfd: `cannot-signal`), one it could not identify
    (`identity-unknown`: something that may be running was not
    stopped), a malformed or unreadable record it had to leave in
    place, or a per-service lock it could not take. The sweep still
    continues past each of these."""
    bad = ("unkillable", "cannot-signal", "identity-unknown", "malformed-pid-file", "lock-failed")
    return 1 if any(step.outcome in bad for step in steps) else 0

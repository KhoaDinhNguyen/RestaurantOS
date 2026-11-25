"""`rosctl links`, `fault`, `node` and `job`: clients of the control process.

Architecture Section 6.3: commands that create or read shared state
(links, faults, nodes) call the control process on port 8002 instead of
running a second implementation, so the fault list stays in one place
and a fault created by one command is healed by the next one through the
same process. Start it with `rosctl control`.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import httpx

from restaurantos.sim.config import get_sim_settings

# a node kill or start waits for docker compose; give it room
_NODE_TIMEOUT_S = 60.0


def _base_url(args: argparse.Namespace) -> str:
    sim = get_sim_settings()
    return f"http://{sim.control_host}:{getattr(args, 'port', None) or sim.control_port}"


def _label(args: argparse.Namespace) -> str:
    sub = getattr(args, "fault_command", None) or getattr(args, "node_command", None)
    return f"rosctl {args.command}" + (f" {sub}" if sub else "")


def _request(
    args: argparse.Namespace,
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout_s: float | None = None,
    transport: httpx.BaseTransport | None = None,
) -> tuple[int, Any] | None:
    """One call to the control process: (status, body), or None after
    printing why there was no answer."""
    url = _base_url(args)
    timeout = timeout_s or get_sim_settings().http_timeout_s
    try:
        with httpx.Client(base_url=url, timeout=timeout, transport=transport) as client:
            response = client.request(method, path, json=json_body)
    except httpx.ConnectError:
        print(
            f"{_label(args)}: no control process at {url} - start it with `rosctl control`",
            file=sys.stderr,
        )
        return None
    except httpx.HTTPError as error:
        print(f"{_label(args)}: {error}", file=sys.stderr)
        return None
    try:
        body = response.json()
    except ValueError:
        body = response.text
    return response.status_code, body


def _fail(args: argparse.Namespace, body: Any) -> int:
    message = (
        (body.get("message") or body.get("detail") or body) if isinstance(body, dict) else body
    )
    print(f"{_label(args)}: {message}", file=sys.stderr)
    return 1


def _print(body: Any) -> None:
    print(json.dumps(body, indent=2, sort_keys=True))


def _note_unexplained(links: list[dict[str, Any]]) -> None:
    stray = [
        entry["group"]
        for entry in links
        if any((entry.get("unexplained") or {}).get(key) for key in ("disabled", "toxics"))
    ]
    if stray:
        print(
            f"note: {', '.join(stray)} hold disabled proxies or toxics no known fault "
            "explains (left by an earlier control process, or by hand); "
            "`rosctl fault heal-all` clears them",
            file=sys.stderr,
        )


def _warn_cut_state(body: Any) -> None:
    """Say when a cut found proxies already disabled (they stay that way
    after the heal) or a heal left such proxies disabled."""
    if not isinstance(body, dict):
        return
    already = body.get("already_disabled") or []
    if already:
        print(
            "warning: already disabled before this cut, "
            f"will stay disabled after heal: {', '.join(already)}",
            file=sys.stderr,
        )
    left = body.get("left_disabled") or []
    if left:
        print(
            f"warning: left disabled (already disabled before their cut): {', '.join(left)}",
            file=sys.stderr,
        )


def run(args: argparse.Namespace, *, transport: httpx.BaseTransport | None = None) -> int:
    def call(method: str, path: str, **kwargs: Any) -> tuple[int, Any] | None:
        return _request(args, method, path, transport=transport, **kwargs)

    if args.command == "links":
        answer = call("GET", "/sim/links")
        if answer is None:
            return 1
        status, body = answer
        if status != 200:
            return _fail(args, body)
        _print(body)
        _note_unexplained(body)
        return 0

    if args.command == "fault":
        command = args.fault_command
        if command == "add":
            params = {
                key: value
                for key, value in (
                    ("latency_ms", args.latency_ms),
                    ("jitter_ms", args.jitter_ms),
                    ("timeout_ms", args.timeout_ms),
                )
                if value is not None
            }
            answer = call(
                "POST",
                "/sim/faults",
                json_body={"target": args.target, "fault": args.kind, "params": params},
            )
            expected = 201
        elif command == "heal":
            answer = call("DELETE", f"/sim/faults/{args.fault_id}")
            expected = 200
        elif command == "heal-all":
            answer = call("POST", "/sim/faults/heal-all")
            expected = 200
        elif command == "list":
            answer = call("GET", "/sim/links")
            if answer is None:
                return 1
            status, body = answer
            if status != 200:
                return _fail(args, body)
            _print([fault for entry in body for fault in entry["faults"]])
            _note_unexplained(body)
            return 0
        else:  # pragma: no cover - argparse refuses anything else
            raise AssertionError(f"unhandled fault command {command!r}")
        if answer is None:
            return 1
        status, body = answer
        if status != expected:
            return _fail(args, body)
        _warn_cut_state(body)
        _print(body)
        return 0

    if args.command == "node":
        answer = call(
            "POST",
            f"/sim/nodes/{args.node_id}/{args.node_command}",
            timeout_s=_NODE_TIMEOUT_S,
        )
        if answer is None:
            return 1
        status, body = answer
        if status != 200:
            return _fail(args, body)
        _print(body)
        return 0

    if args.command == "job":
        answer = call("GET", f"/sim/jobs/{args.job_id}")
        if answer is None:
            return 1
        status, body = answer
        if status != 200:
            return _fail(args, body)
        _print(body)
        return 0 if body.get("state") != "failed" else 1

    raise AssertionError(f"unhandled control command {args.command!r}")  # pragma: no cover

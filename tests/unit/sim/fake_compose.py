"""A fake `docker compose` for the node and snapshot tests: answers `ps`,
`kill`, `start` and `stop` per service and records every call
(imported by name: the sim conftest puts this folder on sys.path)."""

from __future__ import annotations

import json
import subprocess


class FakeCompose:
    """Answers `ps`, `kill` and `start` like docker compose, per service."""

    def __init__(self, running: set[str]) -> None:
        self.running = set(running)
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, *args: str) -> subprocess.CompletedProcess:
        self.calls.append(args)
        command, node = args[0], args[-1]
        if command == "ps":
            state = "running" if node in self.running else "exited"
            line = json.dumps({"Service": node, "State": state})
            return subprocess.CompletedProcess(args, 0, stdout=line + "\n", stderr="")
        if command == "kill":
            self.running.discard(node)
        elif command == "start":
            self.running.add(node)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

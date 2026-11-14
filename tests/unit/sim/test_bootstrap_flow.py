"""The rebuild order: prompt, teardown of the old shape, generate, up.

Teardown runs against the OLD generated Compose file — the one the
previous bootstrap wrote — because switching from more regions to fewer
must remove the services and volumes the new file will not name. Docker
and the generator are faked; nothing here touches a container.
"""

import subprocess

import restaurantos.sim.bootstrap as bootstrap
from restaurantos.common.cluster import Cluster


def _fake_docker(events: list[str], *, returncode: int = 0):
    """Record every docker invocation; never run anything."""

    def run(argv, **kwargs):  # noqa: ANN001, ANN003 - subprocess signature
        assert argv[0] == "docker"
        if "down" in argv:
            events.append(f"down:{argv[argv.index('-f') + 1]}")
        elif "up" in argv:
            events.append("up")
        return subprocess.CompletedProcess(
            argv, returncode, stdout="", stderr="docker is grumpy" if returncode else ""
        )

    return run


def test_refusal_tears_down_nothing(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])
    monkeypatch.setattr(bootstrap, "_confirm_volume_deletion", lambda volumes: False)
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    monkeypatch.setattr(bootstrap, "generate", lambda *args: events.append("generate"))

    assert bootstrap._rebuild(Cluster(regions=1), yes=False) is False
    assert events == []


def test_a_failed_teardown_stops_bootstrap_before_generating(monkeypatch, tmp_path):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", tmp_path / "compose.yaml")
    (tmp_path / "compose.yaml").write_text("OLD", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events, returncode=1))
    monkeypatch.setattr(bootstrap, "generate", lambda *args: events.append("generate"))

    try:
        bootstrap._rebuild(Cluster(regions=1), yes=True)
    except RuntimeError as error:
        assert "docker is grumpy" in str(error)
    else:
        raise AssertionError("a failed teardown must stop bootstrap")
    # teardown of the old file failed: nothing after it ran
    assert events == [f"down:{tmp_path / 'compose.yaml'}"]


def test_teardown_uses_the_old_file_then_generate_then_up(monkeypatch, tmp_path):
    old_file = tmp_path / "compose.yaml"
    old_file.write_text("OLD", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "COMPOSE_FILE", old_file)
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: ["restaurantos_x-data"])

    def fake_run(argv, **kwargs):  # noqa: ANN001, ANN003
        assert argv[0] == "docker"
        if "down" in argv:
            # teardown still sees the old generated file, not the new one
            assert old_file.read_text(encoding="utf-8") == "OLD"
            assert "--remove-orphans" in argv
            assert "-v" in argv
            events.append(f"down:{argv[argv.index('-f') + 1]}")
        elif "up" in argv:
            # and the cluster is started from the file the generator wrote
            assert old_file.read_text(encoding="utf-8") == "NEW"
            events.append("up")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def fake_generate(cluster, directory):
        events.append("generate")
        (directory / "compose.yaml").write_text("NEW", encoding="utf-8")

    monkeypatch.setattr(bootstrap.subprocess, "run", fake_run)
    monkeypatch.setattr(bootstrap, "generate", fake_generate)

    assert bootstrap._rebuild(Cluster(regions=1), yes=True) is True
    # the old generated file first, then the committed baseline, then the
    # generator, then the start — never generate before teardown
    assert events[0] == f"down:{old_file}"
    assert events[1] == f"down:{bootstrap.COMMITTED_COMPOSE}"
    assert events[2] == "generate"
    assert events[3] == "up"


def test_no_volumes_no_prompt(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(bootstrap, "_existing_volumes", lambda: [])
    called: list[bool] = []
    monkeypatch.setattr(
        bootstrap,
        "_confirm_volume_deletion",
        lambda volumes: called.append(True) or False,
    )
    monkeypatch.setattr(bootstrap.subprocess, "run", _fake_docker(events))
    monkeypatch.setattr(bootstrap, "generate", lambda *args: events.append("generate"))

    assert bootstrap._rebuild(Cluster(regions=1), yes=False) is True
    assert called == []  # nothing to ask about
    assert events[-1] == "up"

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from restaurantos.common.cluster import Cluster
from restaurantos.common.config import get_settings
from restaurantos.relay import __main__ as entry
from restaurantos.relay.__main__ import main
from restaurantos.relay.config import RelaySettings


@pytest.fixture(autouse=True)
def isolated_settings(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name in ("ROLE", "BATCH_SIZE", "POLL_INTERVAL_S", "LOCAL_CHECK_S"):
        monkeypatch.delenv(f"ROS_RELAY_{name}", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_uses_documented_defaults(monkeypatch):
    monkeypatch.setenv("ROS_RELAY_ROLE", "region")

    assert RelaySettings().model_dump() == {
        "role": "region",
        "batch_size": 500,
        "poll_interval_s": 0.5,
        "local_check_s": 1.0,
    }


def test_reads_shared_dotenv_and_prefers_environment(monkeypatch, tmp_path):
    (tmp_path / ".env").write_text(
        "ROS_CLIENT=hq\nROS_RELAY_ROLE=hq\nROS_RELAY_BATCH_SIZE=20\n"
        "ROS_RELAY_POLL_INTERVAL_S=0.25\nROS_RELAY_LOCAL_CHECK_S=2\n"
    )
    monkeypatch.setenv("ROS_RELAY_BATCH_SIZE", "10")

    assert RelaySettings().model_dump() == {
        "role": "hq",
        "batch_size": 10,
        "poll_interval_s": 0.25,
        "local_check_s": 2.0,
    }


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ROLE", "gateway"),
        ("BATCH_SIZE", "0"),
        ("POLL_INTERVAL_S", "0"),
        ("LOCAL_CHECK_S", "inf"),
    ],
)
def test_rejects_invalid_relay_settings(monkeypatch, name, value):
    monkeypatch.setenv("ROS_RELAY_ROLE", "region")
    monkeypatch.setenv(f"ROS_RELAY_{name}", value)

    with pytest.raises(ValidationError):
        RelaySettings()


def test_hq_startup_runs_worker_with_wp1(monkeypatch):
    monkeypatch.setenv("ROS_RELAY_ROLE", "hq")
    monkeypatch.setenv("ROS_CLIENT", "hq")
    monkeypatch.setattr(entry, "load_cluster", lambda _: Cluster())
    place_order = AsyncMock()
    worker = AsyncMock()
    monkeypatch.setattr(
        entry,
        "import_module",
        lambda _: SimpleNamespace(place_order=place_order, apply_menu_version=AsyncMock()),
    )
    monkeypatch.setattr(entry, "run_hq_worker", worker)

    assert main() == 0
    worker.assert_awaited_once()
    assert worker.await_args.args[3] is place_order
    assert callable(worker.await_args.args[4])


def test_hq_startup_explains_missing_wp1(monkeypatch, capsys):
    monkeypatch.setenv("ROS_RELAY_ROLE", "hq")
    monkeypatch.setenv("ROS_CLIENT", "hq")
    monkeypatch.setattr(entry, "load_cluster", lambda _: Cluster())
    monkeypatch.setattr(entry, "import_module", lambda _: SimpleNamespace())

    assert main() == 1
    assert "needs WP1: restaurantos.region.place_order" in capsys.readouterr().err


def test_hq_startup_rejects_wrong_client(monkeypatch, capsys):
    monkeypatch.setenv("ROS_RELAY_ROLE", "hq")
    monkeypatch.setenv("ROS_CLIENT", "gateway")

    assert main() == 1
    assert "ROS_CLIENT=hq" in capsys.readouterr().err


def test_hq_startup_explains_missing_menu_function(monkeypatch, capsys):
    monkeypatch.setenv("ROS_RELAY_ROLE", "hq")
    monkeypatch.setenv("ROS_CLIENT", "hq")
    monkeypatch.setattr(entry, "load_cluster", lambda _: Cluster())
    monkeypatch.setattr(entry, "import_module", lambda _: SimpleNamespace(place_order=AsyncMock()))

    assert main() == 1
    assert "needs WP1: restaurantos.region.apply_menu_version" in capsys.readouterr().err

import pytest
from pydantic import ValidationError

from restaurantos.common.config import get_settings
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


def test_hq_startup_validates_identity(monkeypatch, capsys):
    monkeypatch.setenv("ROS_RELAY_ROLE", "hq")
    monkeypatch.setenv("ROS_CLIENT", "hq")

    assert main() == 0
    assert capsys.readouterr().out == "HQ worker configuration is valid.\n"


def test_hq_startup_rejects_wrong_client(monkeypatch, capsys):
    monkeypatch.setenv("ROS_RELAY_ROLE", "hq")
    monkeypatch.setenv("ROS_CLIENT", "gateway")

    assert main() == 1
    assert "ROS_CLIENT=hq" in capsys.readouterr().err

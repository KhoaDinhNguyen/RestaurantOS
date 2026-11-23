from unittest.mock import Mock

import pytest

from restaurantos.common.cluster import Cluster, service_port
from restaurantos.common.config import Settings
from restaurantos.coordinator import __main__ as entry


def test_normal_entry_point_uses_documented_service_port(monkeypatch):
    cluster = Cluster()
    monkeypatch.setattr(
        entry, "get_settings", lambda: Settings(_env_file=None, client="coordinator")
    )
    monkeypatch.setattr(entry, "load_cluster", lambda path: cluster)
    serve = Mock()
    monkeypatch.setattr(entry.uvicorn, "run", serve)
    entry.main()
    serve.assert_called_once_with(
        "restaurantos.coordinator.app:app",
        host="0.0.0.0",
        port=service_port(cluster, "coordinator"),
    )


def test_wrong_process_identity_is_rejected(monkeypatch):
    monkeypatch.setattr(entry, "get_settings", lambda: Settings(_env_file=None, client="gateway"))
    serve = Mock()
    monkeypatch.setattr(entry.uvicorn, "run", serve)
    with pytest.raises(ValueError, match="ROS_CLIENT=coordinator"):
        entry.main()
    serve.assert_not_called()

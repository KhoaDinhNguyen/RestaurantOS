from restaurantos.gateway.config import GatewaySettings


def test_gateway_settings_defaults() -> None:
    settings = GatewaySettings()

    assert settings.directory_refresh_s == 5.0
    assert settings.local_check_s == 1.0


def test_gateway_settings_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("ROS_GATEWAY_DIRECTORY_REFRESH_S", "10")
    monkeypatch.setenv("ROS_GATEWAY_LOCAL_CHECK_S", "2")

    settings = GatewaySettings()

    assert settings.directory_refresh_s == 10.0
    assert settings.local_check_s == 2.0

"""Gateway-specific runtime settings."""

from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    """Settings owned by the gateway component."""

    model_config = SettingsConfigDict(
        env_prefix="ROS_GATEWAY_",
        env_file=".env",
        extra="ignore",
    )

    directory_refresh_s: float = 5.0
    local_check_s: float = 1.0


@cache
def get_gateway_settings() -> GatewaySettings:
    return GatewaySettings()

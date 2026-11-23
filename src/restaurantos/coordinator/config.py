"""Coordinator-owned settings."""

from pydantic import AnyHttpUrl, Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class CoordinatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ROS_COORDINATOR_", env_file=".env", extra="ignore"
    )

    heartbeat_interval_s: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    heartbeat_misses: PositiveInt = 3
    heartbeat_timeout_s: float = Field(default=0.8, gt=0, allow_inf_nan=False)
    gateway_timeout_s: float = Field(default=4.0, gt=0, allow_inf_nan=False)
    gateway_url: AnyHttpUrl | None = None

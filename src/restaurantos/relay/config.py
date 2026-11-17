"""Settings shared by both relay roles."""

from typing import Literal

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class RelaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ROS_RELAY_", env_file=".env", extra="ignore")

    role: Literal["region", "hq"]
    batch_size: PositiveInt = 500
    poll_interval_s: float = Field(default=0.5, gt=0, allow_inf_nan=False)
    local_check_s: float = Field(default=1.0, gt=0, allow_inf_nan=False)

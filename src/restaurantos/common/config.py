"""Runtime settings, read from environment variables prefixed ROS_ (or a .env file)."""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ROS_", env_file=".env", extra="ignore")

    # Where things are
    data_dir: Path = Path("data")
    cluster_file: Path = Path("infra/cluster.toml")

    # Who this process is. `client` is the name used in Toxiproxy proxy names:
    # gateway, coordinator, hq, relay-r1, ...
    client: str = "gateway"
    mode: Literal["gateway", "edge"] = "gateway"
    region_id: str | None = None

    # Database access (development defaults; nothing here is a real secret)
    db_name: str = "restaurantos"
    db_user: str = "ros"
    db_password: str = "ros"
    connect_timeout_s: float = 1.0
    statement_timeout_ms: int = 2000
    pool_min_size: int = 1
    pool_max_size: int = 10

    # Toxiproxy control API
    toxiproxy_url: str = "http://toxiproxy:8474"


@cache
def get_settings() -> Settings:
    return Settings()

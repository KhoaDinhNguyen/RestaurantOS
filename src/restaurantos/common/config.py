"""Runtime settings, read from environment variables prefixed ROS_ (or a .env file).

Only settings that several components share live here. A component's own
settings go in <component>/config.py with the prefix ROS_<COMPONENT>_.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

TOXIPROXY_URLS = {"compose": "http://toxiproxy:8474", "host": "http://127.0.0.1:8474"}


class Settings(BaseSettings):
    # extra="ignore" is deliberate. Every process reads the same .env, and that
    # file also holds names that are not fields here: each component's own
    # settings (ROS_RELAY_..., ROS_GATEWAY_...) and three read elsewhere
    # (ROS_DB_DIR, ROS_TEST_DSN, ROS_POSTGRES_IMAGE). "forbid" would reject all
    # of those. It would also not catch a misspelt variable set in the real
    # environment, which is how Compose passes them: pydantic-settings only
    # checks .env entries. To see what a process read, log its settings at start.
    model_config = SettingsConfigDict(env_prefix="ROS_", env_file=".env", extra="ignore")

    # Where things are
    data_dir: Path = Path("data")
    cluster_file: Path = Path("infra/cluster.toml")

    # Where this process runs. "compose": inside the Compose network (the
    # default, and what a service container gets). "host": on the Docker host,
    # started with `uv run`; databases and proxies are then reached on
    # 127.0.0.1 through published ports. rosctl always runs on the host.
    network: Literal["compose", "host"] = "compose"

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
    # The server ends a session that sits inside an open transaction for this
    # long. It frees the locks of a caller that gave up or was cut off.
    idle_in_transaction_timeout_ms: int = 5000
    # How long a request path waits for a database call before it treats the
    # region as unreachable. Enforced by the client (common.db.run_on), so it
    # also holds when the link is silent. Keep it above statement_timeout_ms.
    request_deadline_s: float = 3.0
    pool_min_size: int = 1
    pool_max_size: int = 10

    # Toxiproxy control API. Left unset, it follows `network`.
    toxiproxy_url: str = TOXIPROXY_URLS["compose"]

    @model_validator(mode="after")
    def _toxiproxy_url_follows_network(self) -> Settings:
        if "toxiproxy_url" not in self.model_fields_set:
            self.toxiproxy_url = TOXIPROXY_URLS[self.network]
        return self


@cache
def get_settings() -> Settings:
    return Settings()

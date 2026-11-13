"""WP6a's own settings, with the ROS_SIM_ prefix (AGENTS.md rule 8).

The stock parameters are documented in docs/replay-stock-parameters.md:
why a warm-up window, and what W, C and F mean. R (restock interval) is
used by playback and arrives with it.
"""

from __future__ import annotations

from functools import cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class SimSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ROS_SIM_", env_file=".env", extra="ignore")

    # Stock: measure usage on the first W days only; start every counter at
    # max(F, ceil(average daily usage x C)), which is also the par level.
    stock_warm_up_days: int = 14
    stock_cover_days: int = 7
    stock_floor: int = 5


@cache
def get_sim_settings() -> SimSettings:
    return SimSettings()

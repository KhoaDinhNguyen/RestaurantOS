"""WP6a's own settings, with the ROS_SIM_ prefix (AGENTS.md rule 8).

The stock parameters are documented in docs/replay-stock-parameters.md:
why a warm-up window, and what W, C and F mean. R (restock interval) is
used by playback and arrives with it.
"""

from __future__ import annotations

from functools import cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ROS_SIM_", env_file=".env", extra="ignore")

    # Stock: measure usage on the first W days only; start every counter at
    # max(F, ceil(average daily usage x C)), which is also the par level.
    # The constraints are part of the contract: W=0 would mean "no warm-up
    # window", and bootstrap builds the settings before it destroys
    # anything, so a bad value fails before the volumes are deleted.
    stock_warm_up_days: int = Field(default=14, gt=0)
    stock_cover_days: int = Field(default=7, ge=0)
    stock_floor: int = Field(default=5, ge=0)


@cache
def get_sim_settings() -> SimSettings:
    return SimSettings()

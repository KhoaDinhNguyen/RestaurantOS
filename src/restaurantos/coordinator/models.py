"""The admin read shapes from architecture Section 6.2."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class NodeStatus(BaseModel):
    node_id: str
    region_id: str | None
    role: Literal["primary", "standby"]
    status: Literal["up", "suspect", "down"] = "suspect"
    last_heartbeat_at: datetime | None = None


class RegionStatus(BaseModel):
    region_id: str
    epoch: int
    menu_version: int
    primary_node: str
    standby_node: str | None
    replication: Literal["sync", "async"] | None = None
    orders_per_s: float | None = None
    standby_lag_s: float | None = None
    hq_lag_s: float | None = None
    outbox_backlog: int | None = None
    holding_queue: int | None = None
    figures_as_of: datetime | None = None


class ClusterStatus(BaseModel):
    as_of: datetime
    nodes: list[NodeStatus]
    regions: list[RegionStatus]


class ClusterEvent(BaseModel):
    event_id: int
    occurred_at: datetime
    kind: str
    region_id: str | None
    node_id: str | None
    detail: dict[str, Any] = Field(default_factory=dict)


class ExceptionEntry(BaseModel):
    seq: int
    region_id: str
    order_day: int
    order_id: UUID
    kind: str
    current_status: str
    current_changed_at: datetime
    attempted_status: str
    attempted_at: datetime
    hq_applied_at: datetime

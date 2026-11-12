"""Shared data shapes. These are contracts: change them only with team agreement.

Three groups:
- what the HTTP API accepts and returns (OrderRequest, OrderResponse, ...);
- what the region write functions return (OrderResult, StatusResult);
- what travels from a region to HQ (OutboxEvent and its payloads).
"""

from __future__ import annotations

from datetime import datetime, time
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Geohash5 = Annotated[str, Field(pattern=r"^[0-9a-z]{5}$")]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- order status ----------------------------------------------------------


class OrderStatus(StrEnum):
    RECEIVED = "received"
    ACCEPTED = "accepted"
    PREPARING = "preparing"
    READY = "ready"
    PICKED_UP = "picked_up"
    CANCELED = "canceled"


ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.RECEIVED: frozenset({OrderStatus.ACCEPTED, OrderStatus.CANCELED}),
    OrderStatus.ACCEPTED: frozenset({OrderStatus.PREPARING, OrderStatus.CANCELED}),
    OrderStatus.PREPARING: frozenset({OrderStatus.READY, OrderStatus.CANCELED}),
    OrderStatus.READY: frozenset({OrderStatus.PICKED_UP, OrderStatus.CANCELED}),
    OrderStatus.PICKED_UP: frozenset(),
    OrderStatus.CANCELED: frozenset(),
}
"""The status state machine. First committed change wins; anything else is a conflict."""


def can_transition(current: OrderStatus, to: OrderStatus) -> bool:
    return to in ALLOWED_TRANSITIONS[current]


# --- placing an order ------------------------------------------------------


class OrderItemIn(Model):
    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)


class OrderRequest(Model):
    """Body of POST /orders. order_day and order_time are simulated time."""

    channel: str = Field(min_length=1)
    external_order_id: str = Field(min_length=1)
    vendor_id: str = Field(min_length=1)
    customer_id: str | None = None
    customer_geohash: Geohash5 | None = None
    order_day: int = Field(ge=0)
    order_time: time
    items: list[OrderItemIn] = Field(min_length=1)


class OrderOutcome(StrEnum):
    ACCEPTED = "accepted"
    REFUSED = "refused"


class RefusalReason(StrEnum):
    OUT_OF_STOCK = "out_of_stock"
    UNAVAILABLE = "unavailable"
    UNKNOWN_PRODUCT = "unknown_product"


class OrderResult(Model):
    """What region.place_order returns."""

    outcome: OrderOutcome
    duplicate: bool = False
    order_id: UUID | None = None
    order_day: int | None = None
    status: OrderStatus | None = None
    menu_version: int | None = None
    committed_at: datetime | None = None
    refusal_reason: RefusalReason | None = None


class OrderResponse(OrderResult):
    """What POST /orders returns: the region's result plus gateway facts.

    queued=True means the region was unreachable and the order is parked in
    HQ's holding queue; outcome is then not known yet.
    """

    outcome: OrderOutcome | None = None
    queued: bool = False
    region_id: str
    received_at: datetime
    acked_at: datetime


# --- status changes --------------------------------------------------------


class StatusChangeRequest(Model):
    """Body of POST /vendors/{vendor_id}/orders/{order_id}/status."""

    event_id: UUID
    to_status: OrderStatus


class StatusResult(Model):
    """What region.change_status returns."""

    order_id: UUID
    status: OrderStatus
    applied: bool
    """True if this call changed the status."""
    duplicate: bool = False
    """True if this event_id was already processed."""
    conflict: bool = False
    """True if the change was rejected and written to exception_log."""


# --- region -> HQ events ---------------------------------------------------


class EventType(StrEnum):
    ORDER_CREATED = "order.created"
    ORDER_STATUS_CHANGED = "order.status_changed"
    EXCEPTION_LOGGED = "exception.logged"


class OrderItemRow(Model):
    line_no: int
    product_id: str
    quantity: int
    price_units: int


class OrderCreatedPayload(Model):
    order_id: UUID
    order_day: int
    vendor_id: str
    channel: str
    external_order_id: str
    customer_id: str | None = None
    customer_geohash: str | None = None
    order_time: time
    status: OrderStatus
    menu_version: int
    received_at: datetime | None = None
    committed_at: datetime
    items: list[OrderItemRow]


class StatusChangedPayload(Model):
    order_id: UUID
    order_day: int
    from_status: OrderStatus
    to_status: OrderStatus
    changed_at: datetime


class ExceptionLoggedPayload(Model):
    order_id: UUID
    order_day: int
    kind: str = "status_conflict"
    current_status: OrderStatus
    current_changed_at: datetime
    attempted_status: OrderStatus
    attempted_at: datetime
    detail: dict[str, Any] = Field(default_factory=dict)


PAYLOAD_MODELS: dict[EventType, type[Model]] = {
    EventType.ORDER_CREATED: OrderCreatedPayload,
    EventType.ORDER_STATUS_CHANGED: StatusChangedPayload,
    EventType.EXCEPTION_LOGGED: ExceptionLoggedPayload,
}


class OutboxEvent(Model):
    """One outbox row as the relay sends it to HQ."""

    event_id: UUID
    region_id: str
    seq: int
    event_type: EventType
    order_day: int | None = None
    created_at: datetime
    payload: dict[str, Any]

    def parsed_payload(self) -> Model:
        return PAYLOAD_MODELS[self.event_type].model_validate(self.payload)


# --- query responses -------------------------------------------------------


class QueryMeta(Model):
    """Attached to every /query response."""

    mode: Literal["live", "hq", "baseline"]
    nodes_contacted: list[str]
    missing_regions: list[str] = Field(default_factory=list)
    rows_transferred: int = 0
    data_as_of: datetime | None = None
    elapsed_ms: float

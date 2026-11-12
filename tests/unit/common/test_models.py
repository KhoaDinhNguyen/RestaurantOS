from datetime import time
from uuid import uuid4

import pytest
from pydantic import ValidationError

from restaurantos.common.models import (
    ALLOWED_TRANSITIONS,
    EventType,
    OrderOutcome,
    OrderRequest,
    OrderResult,
    OrderStatus,
    OutboxEvent,
    StatusChangedPayload,
    can_transition,
)

REQUEST = {
    "channel": "dhrd",
    "external_order_id": "1234567",
    "vendor_id": "212753d2",
    "customer_id": "1ba124d4e5",
    "customer_geohash": "w21z7",
    "order_day": 85,
    "order_time": "12:03:29",
    "items": [{"product_id": "783e85338f1c", "quantity": 1}],
}


def test_order_request_parses_the_documented_example():
    req = OrderRequest.model_validate(REQUEST)
    assert req.order_time == time(12, 3, 29)
    assert req.items[0].quantity == 1


@pytest.mark.parametrize(
    "change",
    [
        {"items": []},
        {"customer_geohash": "w21z"},
        {"order_day": -1},
        {"items": [{"product_id": "x", "quantity": 0}]},
        {"surprise": 1},
    ],
)
def test_order_request_rejects_bad_input(change):
    with pytest.raises(ValidationError):
        OrderRequest.model_validate({**REQUEST, **change})


def test_every_status_has_a_transition_entry():
    assert set(ALLOWED_TRANSITIONS) == set(OrderStatus)


def test_state_machine_happy_path_and_cancel():
    path = ["received", "accepted", "preparing", "ready", "picked_up"]
    for a, b in zip(path, path[1:], strict=False):
        assert can_transition(OrderStatus(a), OrderStatus(b))
    for s in path[:-1]:
        assert can_transition(OrderStatus(s), OrderStatus.CANCELED)


def test_state_machine_forbids_skips_reversals_and_leaving_final_states():
    assert not can_transition(OrderStatus.RECEIVED, OrderStatus.READY)
    assert not can_transition(OrderStatus.READY, OrderStatus.PREPARING)
    assert not can_transition(OrderStatus.PICKED_UP, OrderStatus.CANCELED)
    assert not can_transition(OrderStatus.CANCELED, OrderStatus.ACCEPTED)


def test_refused_result_has_no_order():
    result = OrderResult(outcome=OrderOutcome.REFUSED, refusal_reason="out_of_stock")
    assert result.order_id is None


def test_outbox_event_payload_round_trip():
    payload = StatusChangedPayload(
        order_id=uuid4(),
        order_day=14,
        from_status="received",
        to_status="accepted",
        changed_at="2026-11-20T18:03:11.482Z",
    )
    event = OutboxEvent(
        event_id=uuid4(),
        region_id="r1",
        seq=7,
        event_type=EventType.ORDER_STATUS_CHANGED,
        order_day=14,
        created_at="2026-11-20T18:03:11.482Z",
        payload=payload.model_dump(mode="json"),
    )
    again = OutboxEvent.model_validate_json(event.model_dump_json())
    assert again.parsed_payload() == payload

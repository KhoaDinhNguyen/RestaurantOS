from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from restaurantos.common.models import (
    OrderOutcome,
    OrderResult,
    OrderStatus,
)


async def place_order_accepted(*args, **kwargs) -> OrderResult:
    """Fake WP1 place_order returning an accepted order."""
    return OrderResult(
        outcome=OrderOutcome.ACCEPTED,
        duplicate=False,
        order_id=uuid4(),
        order_day=14,
        status=OrderStatus.RECEIVED,
        menu_version=1,
        committed_at=datetime.now(UTC),
    )

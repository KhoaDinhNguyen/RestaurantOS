"""Apply an ordered outbox batch in one HQ transaction."""

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from restaurantos.common.config import Settings
from restaurantos.common.db import run_on
from restaurantos.common.locks import ANALYTICS_EXCEPTIONS_LOCK_KEY
from restaurantos.common.models import (
    EventType,
    ExceptionLoggedPayload,
    OrderCreatedPayload,
    OutboxEvent,
    StatusChangedPayload,
)
from restaurantos.common.sqlfiles import load_query


async def apply_batch(
    pool: AsyncConnectionPool, events: list[OutboxEvent], *, settings: Settings
) -> None:
    """Commit each new inbox entry together with its analytics changes."""

    async def apply(conn: AsyncConnection) -> None:
        # Acquire before claiming inbox rows so concurrent retry batches take
        # locks in the same order. PostgreSQL holds this until commit/rollback.
        if any(event.event_type == EventType.EXCEPTION_LOGGED for event in events):
            await conn.execute(
                load_query("relay/lock_exceptions"),
                {"lock_key": ANALYTICS_EXCEPTIONS_LOCK_KEY},
            )
        for event in events:
            key = {"region_id": event.region_id, "event_id": event.event_id}
            claim = await conn.execute(load_query("relay/claim_event"), {**key, "seq": event.seq})
            if claim.rowcount == 0:
                continue

            match event.parsed_payload():
                case OrderCreatedPayload() as order:
                    await conn.execute(
                        load_query("relay/insert_order"),
                        {**key, **order.model_dump(exclude={"items"})},
                    )
                    rows = [
                        {
                            "order_day": order.order_day,
                            "order_id": order.order_id,
                            "vendor_id": order.vendor_id,
                            **item.model_dump(),
                        }
                        for item in order.items
                    ]
                    async with conn.cursor() as cursor:
                        await cursor.executemany(load_query("relay/insert_order_item"), rows)
                case StatusChangedPayload() as status:
                    updated = await conn.execute(
                        load_query("relay/update_order_status"), {**key, **status.model_dump()}
                    )
                    if updated.rowcount != 1:
                        raise ValueError(f"status event {event.event_id} has no matching HQ order")
                case ExceptionLoggedPayload() as exception:
                    await conn.execute(
                        load_query("relay/insert_exception"),
                        {**key, **exception.model_dump(), "detail": Jsonb(exception.detail)},
                    )

    if events:
        await run_on(pool, apply, region_id=events[0].region_id, settings=settings)

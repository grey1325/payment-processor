from datetime import datetime, UTC, timedelta

from sqlalchemy import select, or_, func, cast, Interval
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.outbox_event import OutboxEvent

LEASE_DURATION = timedelta(minutes=2)


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session


    def create(self, event: OutboxEvent) -> None:
        self.session.add(event)

    async def claim_unpublished(self, limit: int = 20) -> list[OutboxEvent]:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .where(
                or_(
                    OutboxEvent.claimed_until.is_(None),
                    OutboxEvent.claimed_until <= func.now(),
                )
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        results = await self.session.execute(stmt)
        events = list(results.scalars().all())
        for event in events:
            event.claimed_until = func.now() + cast(LEASE_DURATION, Interval)
        await self.session.flush()
        return events

    async def release_claim(self, events: list[OutboxEvent]) -> None:
        for event in events:
            event.claimed_until = None

        await self.session.flush()

    async def mark_as_published(self, event: OutboxEvent) -> None:
        event.published_at = datetime.now(UTC)
        event.claimed_until = None
        await self.session.flush()
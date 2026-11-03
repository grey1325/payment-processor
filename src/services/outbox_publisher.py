import asyncio
import logging

from src.core.rabbitmq import payment_queue
from src.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)
PUBLISH_TIMEOUT = 3.0

class OutboxPublisher:
    def __init__(self, broker, session_factory, batch_size: int = 20) -> None:
        self.broker = broker
        self.session_factory = session_factory
        self.batch_size = batch_size

    async def publish_events(self) -> int:
        published_count = 0
        publish_error: Exception | None = None
        async with self.session_factory() as session:
            repo = OutboxRepository(session)
            events = await repo.claim_unpublished(limit=self.batch_size)
            await session.commit()
            published_events = []

            for event in events:
                try:
                    await asyncio.wait_for(
                        self.broker.publish(
                            message=event.payload,
                            queue=payment_queue,
                            message_id=str(event.id),
                            persist=True,
                        ),
                        timeout=PUBLISH_TIMEOUT,
                    )
                except Exception as exc:
                    logger.warning("Failed to publish outbox event %s: %r", event.id, exc)
                    publish_error = exc
                    event.attempts += 1
                    break

                published_events.append(event)
                published_count += 1

            remaining_events = [event for event in events if event not in published_events]
            await repo.release_claim(remaining_events)

            for event in published_events:
                await repo.mark_as_published(event=event)


            await session.commit()

        if publish_error is not None:
            raise publish_error

        return published_count

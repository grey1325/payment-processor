import asyncio
import logging
import signal

from src.core.logger import setup_logging
from src.core.rabbitmq import broker, payment_queue
from src.database.db import async_session_factory, engine
from src.services.outbox_publisher import OutboxPublisher

logger = logging.getLogger(__name__)

MAX_BACKOFF = 30.0

async def main() -> None:
    setup_logging()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    broker_started = False
    try:
        await broker.start()
        broker_started = True
        await broker.declare_queue(payment_queue)
        publisher = OutboxPublisher(broker=broker, session_factory=async_session_factory)
        failures = 0
        while not stop.is_set():
            try:
                published = await publisher.publish_events()
            except Exception:
                failures += 1
                logger.exception("Error while publishing outbox events")
                delay = min(MAX_BACKOFF, 2.0 ** failures)
            else:
                failures = 0
                delay = 0.0 if published >= publisher.batch_size else 1.0
            if delay:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
    finally:
        try:
            if broker_started:
                await broker.stop()
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
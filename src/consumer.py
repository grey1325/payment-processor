import logging
import uuid
import asyncio
import random
import httpx

from faststream import FastStream, AckPolicy
from datetime import datetime, UTC

from faststream.rabbit.annotations import RabbitMessage

from src.core.rabbitmq import broker, payment_queue, dead_letter_queue, dead_letter_exchange
from src.database.db import async_session_factory
from src.repositories.payment import PaymentRepository

app = FastStream(broker)
logger = logging.getLogger(__name__)

WEBHOOK_MAX_ATTEMPTS = 3
WEBHOOK_RETRY_DELAY = 1.0

class PermanentMessageError(Exception):
    pass

class WebhookDeliveryError(Exception):
    pass


async def send_webhook(payment) -> bool:
    async with httpx.AsyncClient(timeout=5.0) as client:
        for attempt in range(1, WEBHOOK_MAX_ATTEMPTS + 1):
            try:
                response = await client.post(
                    payment.webhook_url,
                    json={
                        "payment_id": str(payment.id),
                        "status": payment.status,
                        "amount": str(payment.amount),
                        "currency": payment.currency,
                    },
                )
                response.raise_for_status()

            except httpx.HTTPError:
                if attempt == WEBHOOK_MAX_ATTEMPTS:
                    return False

                await asyncio.sleep(
                    WEBHOOK_RETRY_DELAY * 2 ** (attempt - 1)
                )

            else:
                return True

    return False

@app.on_startup
async def setup_dead_letter_queue():
    await broker.connect()

    exchange = await broker.declare_exchange(dead_letter_exchange)
    queue = await broker.declare_queue(dead_letter_queue)

    await queue.bind(
        exchange=exchange,
        routing_key="payments.dlq",
    )

@broker.subscriber(payment_queue, ack_policy=AckPolicy.MANUAL)
async def handle_payment(message: dict, broker_message: RabbitMessage) -> None:
    try:
        try:
            payment_id = uuid.UUID(message["payment_id"])
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise PermanentMessageError("Invalid payment_id in message") from exc

        async with async_session_factory() as session:
            payment_repo = PaymentRepository(session=session)
            payment = await payment_repo.get_by_id(payment_id)

            if payment is None:
                raise PermanentMessageError (f"Payment {payment_id} not found")

            if payment.status == "pending":
                await asyncio.sleep(random.uniform(2, 5))
                is_success = random.random() < 0.9
                payment.status = "succeeded" if is_success else "failed"
                payment.processed_at = datetime.now(UTC)

                await session.commit()

            if payment.webhook_url is not None and payment.webhook_sent_at is None:
                is_sent = await send_webhook(payment)

                if not is_sent:
                    raise WebhookDeliveryError (f"Webhook delivery failed for payment {payment_id}")

                payment.webhook_sent_at = datetime.now(UTC)
                await session.commit()


    except (PermanentMessageError, WebhookDeliveryError) as exc:
        logger.warning("Message sent to DLQ: %s", exc)
        await broker_message.reject(requeue=False)


    except Exception:
        await broker_message.reject(requeue=True)
        raise

    else:
        await broker_message.ack()
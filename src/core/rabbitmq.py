from faststream.rabbit import RabbitBroker, RabbitQueue, Channel, QueueType, RabbitExchange, ExchangeType
from faststream.rabbit.schemas.queue import QuorumQueueArgs

from src.core.config import settings

broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(
        publisher_confirms=True,
        on_return_raises=True,
    )
)

queue_arguments: QuorumQueueArgs = {
    "x-delivery-limit": 2,
    "x-dead-letter-exchange": "payments.dlx",
    "x-dead-letter-routing-key": "payments.dlq",
}

payment_queue = RabbitQueue(
    name="payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments=queue_arguments,
)


dead_letter_exchange = RabbitExchange(
    name="payments.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)

dead_letter_queue = RabbitQueue(
    name="payments.dlq",
    durable=True,
)
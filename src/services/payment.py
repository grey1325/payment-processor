import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from src.models.outbox_event import OutboxEvent
from src.models.payment import Payment
from src.repositories.outbox import OutboxRepository
from src.repositories.payment import PaymentRepository
from src.schemas.payment import PaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.payment_repo = PaymentRepository(session=session)
        self.outbox_repo = OutboxRepository(session=session)

    async def create_payment(self, payment_data: PaymentCreate, idempotency_key: str) -> Payment:
        try:
            existing_payment = await self.payment_repo.get_by_idempotency_key(idempotency_key)
            if existing_payment is not None:
                return existing_payment

            payment = Payment(
                amount=payment_data.amount,
                currency=payment_data.currency,
                description=payment_data.description,
                idempotency_key=idempotency_key,
                payment_metadata=payment_data.metadata,
                webhook_url=str(payment_data.webhook_url)
                if payment_data.webhook_url is not None
                else None,
            )

            self.payment_repo.create(payment)
            await self.session.flush()

            outbox = OutboxEvent(
                event_type="payment.created",
                payload={
                    "payment_id": str(payment.id),
                    "amount": str(payment.amount),
                    "currency": payment.currency,
                }
            )

            self.outbox_repo.create(outbox)
            await self.session.commit()
            return payment

        except IntegrityError:
            await self.session.rollback()
            existing_payment = await self.payment_repo.get_by_idempotency_key(idempotency_key)
            if existing_payment is not None:
                return existing_payment
            raise

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.payment_repo.get_by_id(payment_id)
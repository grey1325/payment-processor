import uuid

from fastapi import APIRouter, Header, status, HTTPException
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import verify_api_key
from src.database.db import get_db
from src.schemas.payment import PaymentResponse, PaymentCreate
from src.services.payment import PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payment"],
    dependencies=[Depends(verify_api_key)],
)

@router.post("", response_model=PaymentResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
        payment: PaymentCreate,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        session: AsyncSession = Depends(get_db),
):
    service = PaymentService(session=session)
    created_payment = await service.create_payment(
        payment_data=payment,
        idempotency_key=idempotency_key,
    )
    return created_payment

@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    status_code=status.HTTP_200_OK,
    description="Get payment",
)
async def get_payment(payment_id: uuid.UUID, session: AsyncSession = Depends(get_db)) -> PaymentResponse:
    service = PaymentService(session=session)

    payment = await service.get_payment(payment_id=payment_id)

    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")

    return payment
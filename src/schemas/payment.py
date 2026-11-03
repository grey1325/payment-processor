import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class PaymentCreate(BaseModel):
    amount: Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]
    currency: Literal['RUB', 'USD', 'EUR']
    description: str
    metadata: Annotated[dict, Field(default_factory=dict)]
    webhook_url: HttpUrl | None = None


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(serialization_alias="payment_id")
    amount: Decimal
    currency: Literal['RUB', 'USD', 'EUR']
    description: str
    metadata: dict = Field(validation_alias="payment_metadata")
    status: Literal['pending', 'succeeded', 'failed']
    webhook_url: HttpUrl | None
    created_at: datetime
    processed_at: datetime | None

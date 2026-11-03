from fastapi import HTTPException, Header, status

from src.core.config import settings


async def verify_api_key(api_key: str = Header(alias="X-API-Key")) -> None:
    if api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

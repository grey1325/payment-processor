import logging
from time import perf_counter


from fastapi import FastAPI
from starlette.requests import Request

from src.api.router import router
from src.core.config import settings
from src.core.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
)

app.include_router(router)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        duration = perf_counter() - start_time
        logger.exception(
            "Необработанная ошибка HTTP-запроса: method=%s path=%s duration=%.3fs",
            request.method,
            request.url.path,
            duration,
        )
        raise

    duration = perf_counter() - start_time
    logger.info(
        "HTTP-запрос: method=%s path=%s status_code=%s duration=%.3fs",
        request.method,
        request.url.path,
        response.status_code,
        duration,
    )

    return response

@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
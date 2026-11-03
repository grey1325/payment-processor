from pathlib import Path
import logging
import logging.config
from typing import Any

from src.core.config import settings

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"


_PACKAGE_LOG_LEVELS: dict[str, str] = {
    "sqlalchemy.engine": "WARNING",
    "httpx": "WARNING",
    "httpcore": "WARNING",
    "python_multipart": "INFO",
}

def logging_config() -> dict[str, Any]:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"format": settings.log_format}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(LOG_FILE),
                "encoding": "utf-8",
                "maxBytes": 1_000_000,
                "backupCount": 3,
                "formatter": "default",
            }
        },
        "loggers": {name: {"level": level} for name, level in _PACKAGE_LOG_LEVELS.items()},
        "root": {"level": settings.log_level, "handlers": ["console", "file"]},
    }


def setup_logging() -> None:
    logging.config.dictConfig(logging_config())
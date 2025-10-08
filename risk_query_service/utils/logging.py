"""Logging helpers using loguru."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from loguru import logger


def configure_logging() -> None:
    """Configure loguru to emit JSON lines suitable for production."""

    logger.remove()

    def _serialize(record: Dict[str, Any]) -> str:
        payload = {
            "timestamp": record["time"].isoformat(),
            "level": record["level"].name,
            "message": record["message"],
            "module": record["module"],
            "function": record["function"],
            "line": record["line"],
        }
        payload.update(record.get("extra", {}))
        return json.dumps(payload, default=str)

    sink = os.environ.get("LOG_FILE") or "sys.stderr"
    logger.add(sink, level="INFO", serialize=False, backtrace=False, diagnose=False, format=_serialize)


def log_request_summary(endpoint: str, **extra: Any) -> None:
    """Helper to log structured request summary."""

    logger.bind(endpoint=endpoint, **extra).info("request-summary")

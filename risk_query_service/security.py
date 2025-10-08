"""API key security dependencies."""
from __future__ import annotations

from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader

from .config import get_settings

API_KEY_HEADER_NAME = "x-api-key"
API_KEY_HEADER = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


def require_api_key(api_key: str | None = Security(API_KEY_HEADER)) -> str:
    settings = get_settings()
    expected = settings.api_key
    if not expected:
        raise HTTPException(status_code=503, detail="API key is not configured")
    if not api_key or api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

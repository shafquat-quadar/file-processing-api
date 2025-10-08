"""Health check endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..file_index import FILE_PATTERNS, get_latest_file

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
def ready() -> dict[str, str]:
    for report_type in FILE_PATTERNS:
        try:
            get_latest_file(report_type)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"status": "ok"}

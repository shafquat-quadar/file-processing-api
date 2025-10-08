"""Risk action query endpoints."""
from __future__ import annotations

import time
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..datasets import (
    actions_bundle,
    apply_filters,
    paginate_collect,
    select_columns,
    summarize,
)
from ..security import require_api_key
from ..utils.logging import log_request_summary
from ..utils.paginate import (
    Cursor,
    build_initial_cursor,
    encode_cursor,
    maybe_decode_cursor,
    next_cursor,
)

router = APIRouter(prefix="/risk/actions", tags=["Risk Actions"], dependencies=[Depends(require_api_key)])


class QueryResponse(BaseModel):
    data: List[Dict[str, Any]]
    cursor: Optional[str] = None
    has_more: bool
    partial: bool = False
    file_hash: str
    report_type: str


class SummaryItem(BaseModel):
    group: Any
    count: int
    examples: List[Any] = Field(default_factory=list)


class SummaryResponse(BaseModel):
    data: List[SummaryItem]
    report_type: str


MAX_LIMIT = 200
DEFAULT_LIMIT = 50
SUMMARY_GROUPS = {"Role ID", "User Name", "Risk Level", "Action", "System"}


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Invalid date value '{value}'") from exc


def _prepare_columns(columns: Optional[str]) -> Optional[List[str]]:
    if not columns:
        return None
    items = [item.strip() for item in columns.split(",") if item.strip()]
    if not items:
        return None
    return items


@router.get("/query", response_model=QueryResponse)
def query_actions(
    *,
    user: Optional[str] = Query(None, description="Filter by user (substring match)"),
    role: Optional[str] = Query(None, description="Filter by role ID"),
    risk_level: Optional[str] = Query(None, description="Filter by risk level"),
    system: Optional[str] = Query(None, description="Filter by system"),
    action: Optional[str] = Query(None, description="Filter by action"),
    date_from: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)"),
    columns: Optional[str] = Query(None, description="Comma separated list of columns"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    cursor: Optional[str] = Query(None, description="Opaque cursor token"),
    offset: int = Query(0, ge=0, description="Offset (ignored when cursor is provided)"),
) -> QueryResponse:
    bundle = actions_bundle()
    parsed_from = _parse_date(date_from)
    parsed_to = _parse_date(date_to)

    cursor_obj = maybe_decode_cursor(cursor)
    effective_offset = offset
    if cursor_obj:
        if cursor_obj.file_hash != bundle.file_hash:
            effective_offset = 0
            cursor_obj = build_initial_cursor(bundle.file_hash, bundle.report_type)
        else:
            effective_offset = cursor_obj.offset
    else:
        cursor_obj = build_initial_cursor(bundle.file_hash, bundle.report_type, offset)

    lf = bundle.lazyframe
    lf = apply_filters(
        lf,
        user=user,
        role=role,
        risk_level=risk_level,
        system=system,
        action=action,
        date_from=parsed_from,
        date_to=parsed_to,
    )
    lf = select_columns(lf, _prepare_columns(columns))

    start = time.perf_counter()
    rows, has_more = paginate_collect(lf, limit, effective_offset)
    duration = time.perf_counter() - start
    partial = duration > 3.0

    if has_more:
        next_cur = next_cursor(cursor_obj, len(rows))
        cursor_token = encode_cursor(next_cur)
    else:
        cursor_token = None

    log_request_summary(
        "/risk/actions/query",
        filters={
            "user": user,
            "role": role,
            "risk_level": risk_level,
            "system": system,
            "action": action,
            "date_from": date_from,
            "date_to": date_to,
        },
        rows_returned=len(rows),
        has_more=has_more,
        partial=partial,
        latency_ms=int(duration * 1000),
    )

    return QueryResponse(
        data=rows,
        cursor=cursor_token,
        has_more=has_more,
        partial=partial,
        file_hash=bundle.file_hash,
        report_type=bundle.report_type,
    )


@router.get("/summary", response_model=SummaryResponse)
def summary_actions(
    *,
    groupby: str = Query(..., description="Column to group by"),
    top: int = Query(20, ge=1, le=100),
    user: Optional[str] = None,
    role: Optional[str] = None,
    risk_level: Optional[str] = None,
    system: Optional[str] = None,
    action: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> SummaryResponse:
    canonical_group = groupby.strip()
    if canonical_group not in SUMMARY_GROUPS:
        raise HTTPException(status_code=400, detail="Unsupported groupby column")

    bundle = actions_bundle()
    parsed_from = _parse_date(date_from)
    parsed_to = _parse_date(date_to)
    lf = apply_filters(
        bundle.lazyframe,
        user=user,
        role=role,
        risk_level=risk_level,
        system=system,
        action=action,
        date_from=parsed_from,
        date_to=parsed_to,
    )
    records = summarize(lf, canonical_group, top)

    log_request_summary(
        "/risk/actions/summary",
        filters={
            "groupby": groupby,
            "user": user,
            "role": role,
            "risk_level": risk_level,
            "system": system,
            "action": action,
        },
        rows_returned=len(records),
    )

    return SummaryResponse(data=records, report_type=bundle.report_type)

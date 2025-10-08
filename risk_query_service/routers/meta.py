"""Metadata endpoints for schema and facets."""
from __future__ import annotations

from typing import Dict, List

import polars as pl
from fastapi import APIRouter, HTTPException, Query

from ..datasets import actions_bundle, infer_schema, permissions_bundle
from ..utils.schema import CANONICAL_COLUMNS

router = APIRouter(prefix="/meta", tags=["Metadata"])


def _resolve_column(column: str) -> str:
    mapping = {name.lower(): name for name in CANONICAL_COLUMNS}
    key = column.lower()
    if key not in mapping:
        raise HTTPException(status_code=400, detail=f"Unknown column '{column}'")
    return mapping[key]


schema_cache: dict[tuple[str, str], Dict[str, str]] = {}


@router.get("/schema")
def get_schema() -> Dict[str, str]:
    actions = actions_bundle()
    permissions = permissions_bundle()
    key = (actions.file_hash, permissions.file_hash)
    if key not in schema_cache:
        combined = pl.concat([actions.lazyframe, permissions.lazyframe], how="diagonal_relaxed")
        schema_cache[key] = infer_schema(combined)
    return schema_cache[key]


@router.get("/facets")
def get_facets(column: str = Query(..., description="Column name"), n: int = Query(20, ge=1, le=100)) -> List[Dict[str, object]]:
    canonical = _resolve_column(column)
    actions = actions_bundle()
    permissions = permissions_bundle()
    combined = pl.concat([actions.lazyframe, permissions.lazyframe], how="vertical")
    result = (
        combined.group_by(canonical)
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .limit(n)
        .collect()
    )
    facets: List[Dict[str, object]] = []
    for row in result.iter_rows(named=True):
        facets.append({"value": row[canonical], "count": row["count"]})
    return facets

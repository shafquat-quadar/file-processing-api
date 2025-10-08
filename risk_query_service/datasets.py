"""Dataset access utilities built on Polars lazy execution."""
from __future__ import annotations

import hashlib
import itertools
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import polars as pl

from .file_index import FileRecord, ReportType, get_latest_file_with_hash
from .utils.schema import CANONICAL_COLUMNS, CANONICAL_TYPES, DEFAULT_COLUMNS, canonicalize_columns


@dataclass
class DatasetBundle:
    lazyframe: pl.LazyFrame
    file_hash: str
    report_type: str


def scan_report(path: Path) -> pl.LazyFrame:
    """Scan a TSV report lazily."""

    return pl.scan_csv(
        path,
        separator="\t",
        has_header=True,
        ignore_errors=True,
        infer_schema_length=100,
    )


def _normalize_lazyframe(lf: pl.LazyFrame, report_type: ReportType, file_path: Path) -> pl.LazyFrame:
    columns = lf.columns
    missing = [name for name in CANONICAL_COLUMNS if name not in columns]
    for name in missing:
        if name == "IsCritical":
            lf = lf.with_columns(pl.lit(False).alias("IsCritical"))
        else:
            lf = lf.with_columns(pl.lit(None).cast(CANONICAL_TYPES[name]).alias(name))

    if "IsCritical" in columns:
        lf = lf.with_columns(pl.col("IsCritical").cast(pl.Boolean))

    is_critical = "crit" in file_path.stem.lower()
    lf = lf.with_columns(
        pl.lit(is_critical).alias("IsCritical"),
        pl.lit(_report_type_label(report_type, is_critical)).alias("ReportType"),
    )
    return lf.select(CANONICAL_COLUMNS)


def _report_type_label(report_type: ReportType, is_critical: bool) -> str:
    if report_type in {"actions", "crit_actions"}:
        return "Critical Action" if is_critical else "Action"
    if report_type in {"perms", "crit_perms"}:
        return "Critical Permission" if is_critical else "Permission"
    return report_type


def _load_bundle(report_type: ReportType) -> Tuple[pl.LazyFrame, FileRecord]:
    record = get_latest_file_with_hash(report_type)
    lf = scan_report(record.path)
    lf = _normalize_lazyframe(lf, report_type, record.path)
    return lf, record


def _combine_frames(report_types: Iterable[ReportType]) -> DatasetBundle:
    frames: List[pl.LazyFrame] = []
    hashes: List[str] = []
    canonical_report: Optional[str] = None
    for report_type in report_types:
        lf, record = _load_bundle(report_type)
        frames.append(lf)
        hashes.append(record.file_hash)
        canonical_report = canonical_report or report_type
    lazyframe = pl.concat(frames, how="vertical") if len(frames) > 1 else frames[0]
    combined_hash = hashlib.sha1("|".join(hashes).encode("utf-8")).hexdigest()
    report_label = "actions" if canonical_report in {"actions", "crit_actions"} else "permissions"
    return DatasetBundle(lazyframe=lazyframe, file_hash=combined_hash, report_type=report_label)


actions_bundle = lambda: _combine_frames(["actions", "crit_actions"])  # noqa: E731
permissions_bundle = lambda: _combine_frames(["perms", "crit_perms"])  # noqa: E731


FILTERABLE_COLUMNS = {
    "user": ["User ID", "User Name"],
    "role": "Role ID",
    "risk_level": "Risk Level",
    "system": "System",
    "action": "Action",
}


def apply_filters(
    lf: pl.LazyFrame,
    *,
    user: Optional[str] = None,
    role: Optional[str] = None,
    risk_level: Optional[str] = None,
    system: Optional[str] = None,
    action: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pl.LazyFrame:
    exprs: List[pl.Expr] = []

    if user:
        term = user.lower()
        exprs.append(
            pl.any_horizontal(
                [pl.col(col).cast(pl.Utf8).str.to_lowercase().str.contains(term) for col in FILTERABLE_COLUMNS["user"]]
            )
        )
    if role:
        exprs.append(pl.col("Role ID").cast(pl.Utf8).str.to_lowercase() == role.lower())
    if risk_level:
        exprs.append(pl.col("Risk Level").cast(pl.Utf8).str.to_lowercase() == risk_level.lower())
    if system:
        exprs.append(pl.col("System").cast(pl.Utf8).str.to_lowercase() == system.lower())
    if action:
        exprs.append(pl.col("Action").cast(pl.Utf8).str.to_lowercase() == action.lower())

    if date_from or date_to:
        parsed = pl.col("Last Executed On").str.strptime(pl.Date, strict=False, format=None)
        if date_from:
            exprs.append(parsed >= pl.lit(date_from))
        if date_to:
            exprs.append(parsed <= pl.lit(date_to))

    for expr in exprs:
        lf = lf.filter(expr)
    return lf


def paginate_collect(lf: pl.LazyFrame, limit: int, offset: int) -> Tuple[List[Dict[str, object]], bool]:
    window = lf.slice(offset, limit + 1)
    result = window.collect()
    rows = result.to_dicts()
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]
    return rows, has_more


def select_columns(lf: pl.LazyFrame, columns: Optional[List[str]]) -> pl.LazyFrame:
    if columns:
        resolved = canonicalize_columns(columns)
    else:
        resolved = DEFAULT_COLUMNS
    return lf.select(resolved)


def summarize(lf: pl.LazyFrame, groupby: str, top: int) -> List[Dict[str, object]]:
    summary = (
        lf.group_by(groupby)
        .agg(
            [
                pl.len().alias("count"),
                pl.col("User Name").drop_nulls().head(3).alias("examples"),
            ]
        )
        .sort("count", descending=True)
        .limit(top)
        .collect()
    )
    records = []
    for row in summary.iter_rows(named=True):
        records.append({"group": row[groupby], "count": row["count"], "examples": row["examples"]})
    return records


def infer_schema(lf: pl.LazyFrame) -> Dict[str, str]:
    schema = lf.schema
    return {name: dtype.__class__.__name__ for name, dtype in schema.items()}

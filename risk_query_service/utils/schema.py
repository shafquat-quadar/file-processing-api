"""Schema utilities for the risk query service."""
from __future__ import annotations

from typing import Dict, List

import polars as pl

CANONICAL_COLUMNS: List[str] = [
    "User ID",
    "User Name",
    "User Group",
    "Access Risk ID",
    "Risk Description",
    "Role ID",
    "Risk Level",
    "Function",
    "Function Description",
    "System",
    "Action",
    "Action Description",
    "Last Executed On",
    "Business Process",
    "Composite/Business Role Description",
    "ReportType",
    "IsCritical",
]

DEFAULT_COLUMNS: List[str] = [
    "User ID",
    "User Name",
    "Role ID",
    "Risk Level",
    "Action",
    "Action Description",
    "System",
    "Last Executed On",
    "IsCritical",
    "ReportType",
]

CANONICAL_TYPES: Dict[str, pl.DataType] = {
    "User ID": pl.Utf8,
    "User Name": pl.Utf8,
    "User Group": pl.Utf8,
    "Access Risk ID": pl.Utf8,
    "Risk Description": pl.Utf8,
    "Role ID": pl.Utf8,
    "Risk Level": pl.Utf8,
    "Function": pl.Utf8,
    "Function Description": pl.Utf8,
    "System": pl.Utf8,
    "Action": pl.Utf8,
    "Action Description": pl.Utf8,
    "Last Executed On": pl.Utf8,
    "Business Process": pl.Utf8,
    "Composite/Business Role Description": pl.Utf8,
    "ReportType": pl.Utf8,
    "IsCritical": pl.Boolean,
}


def canonicalize_columns(columns: List[str]) -> List[str]:
    """Return canonical columns while preserving the requested order."""

    canon = {name.lower(): name for name in CANONICAL_COLUMNS}
    resolved: List[str] = []
    for column in columns:
        key = column.lower()
        if key not in canon:
            raise KeyError(f"Unknown column: {column}")
        resolved.append(canon[key])
    return resolved

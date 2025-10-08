"""File discovery and indexing for report selection."""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Tuple

from cachetools import TTLCache

from .config import get_settings
from .graph_client import iter_remote_files

ReportType = Literal["actions", "crit_actions", "perms", "crit_perms"]

FILE_PATTERNS: Dict[ReportType, re.Pattern[str]] = {
    "actions": re.compile(r"^RS_Action_Lvl_(\d{8})_(\d{6})\.txt$"),
    "crit_actions": re.compile(r"^RS_CritAction_Lvl_(\d{8})_(\d{6})\.txt$"),
    "perms": re.compile(r"^RS_Perm_Lvl_(\d{8})_(\d{6})\.txt$"),
    "crit_perms": re.compile(r"^RS_CritPerm_Lvl_(\d{8})_(\d{6})\.txt$"),
}


@dataclass(frozen=True)
class FileRecord:
    report_type: ReportType
    path: Path
    timestamp: float
    file_hash: str


_file_cache: TTLCache[ReportType, FileRecord] | None = None
_settings = None


def _ensure_cache() -> None:
    global _file_cache, _settings
    if _file_cache is None:
        _settings = get_settings()
        _file_cache = TTLCache(maxsize=16, ttl=_settings.file_index_ttl_seconds)


def clear_file_cache() -> None:
    if _file_cache is not None:
        _file_cache.clear()


def _parse_timestamp(match: re.Match[str]) -> float:
    date_part, time_part = match.groups()
    return time.mktime(time.strptime(date_part + time_part, "%Y%m%d%H%M%S"))


def _compute_file_hash(path: Path) -> str:
    stat = path.stat()
    return f"{path.name}:{stat.st_size}:{int(stat.st_mtime)}"


def _discover_local_files(pattern: re.Pattern[str]) -> List[Tuple[Path, float]]:
    settings = get_settings()
    folder = settings.onedrive_local_path
    if folder is None:
        raw = os.environ.get("ONEDRIVE_LOCAL_PATH")
        if raw:
            folder = Path(raw).expanduser()
    if not folder:
        return []
    if isinstance(folder, str):
        folder = Path(folder)
    folder = folder.expanduser()
    matches: List[Tuple[Path, float]] = []
    try:
        iterator = folder.iterdir()
    except FileNotFoundError:
        return []
    for file in iterator:
        if not file.is_file():
            continue
        match = pattern.match(file.name)
        if match:
            ts = _parse_timestamp(match)
            matches.append((file, ts))
    return matches


def _discover_remote_files(pattern: re.Pattern[str]) -> List[Tuple[Path, float]]:
    names = []
    settings = get_settings()
    folder = settings.onedrive_local_path
    if folder:
        return []
    client_files: Dict[str, Path] = {}
    for report_type, regex in FILE_PATTERNS.items():
        if regex is pattern:
            names = [name for name in _list_all_remote_names() if regex.match(name)]
            break
    if not names:
        return []
    client_files = iter_remote_files(names)
    results: List[Tuple[Path, float]] = []
    for name, path in client_files.items():
        match = pattern.match(name)
        if not match:
            continue
        ts = _parse_timestamp(match)
        results.append((path, ts))
    return results

_remote_names_cache: Optional[List[str]] = None
_remote_names_timestamp: float = 0.0


def _list_all_remote_names() -> List[str]:
    global _remote_names_cache, _remote_names_timestamp
    settings = get_settings()
    if settings.onedrive_local_path:
        return []
    now = time.time()
    if _remote_names_cache and (now - _remote_names_timestamp) < settings.graph_cache_ttl_seconds:
        return _remote_names_cache
    from .graph_client import GraphClient

    client = GraphClient()
    files = client.list_files()
    _remote_names_cache = [file.name for file in files]
    _remote_names_timestamp = now
    return _remote_names_cache


def _discover_files(report_type: ReportType) -> Optional[FileRecord]:
    _ensure_cache()
    pattern = FILE_PATTERNS[report_type]
    matches = _discover_local_files(pattern)
    if not matches:
        matches = _discover_remote_files(pattern)
    if not matches:
        return None
    matches.sort(key=lambda item: item[1], reverse=True)
    path, ts = matches[0]
    file_hash = _compute_file_hash(path)
    return FileRecord(report_type=report_type, path=path, timestamp=ts, file_hash=file_hash)


def get_latest_file(report_type: ReportType) -> Path:
    """Return the latest file path for the given report type."""

    _ensure_cache()
    assert _file_cache is not None
    record = _file_cache.get(report_type)
    if record is None:
        record = _discover_files(report_type)
        if record is None:
            raise FileNotFoundError(f"No files found for report type {report_type}")
        _file_cache[report_type] = record
    return record.path


def get_latest_file_with_hash(report_type: ReportType) -> FileRecord:
    _ensure_cache()
    assert _file_cache is not None
    record = _file_cache.get(report_type)
    if record is None:
        record = _discover_files(report_type)
        if record is None:
            raise FileNotFoundError(f"No files found for report type {report_type}")
        _file_cache[report_type] = record
    return record

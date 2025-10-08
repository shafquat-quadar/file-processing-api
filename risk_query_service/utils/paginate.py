"""Cursor-based pagination helpers."""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Optional


@dataclass
class Cursor:
    offset: int
    file_hash: str
    report_type: str


class CursorError(ValueError):
    """Raised when the cursor token cannot be decoded."""


def encode_cursor(cursor: Cursor) -> str:
    payload = json.dumps(cursor.__dict__, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


def decode_cursor(token: str) -> Cursor:
    try:
        raw = base64.urlsafe_b64decode(token.encode("utf-8"))
        data = json.loads(raw.decode("utf-8"))
        return Cursor(offset=int(data["offset"]), file_hash=str(data["file_hash"]), report_type=str(data["report_type"]))
    except Exception as exc:  # noqa: BLE001
        raise CursorError("Invalid cursor token") from exc


def next_cursor(current: Cursor, advance: int) -> Cursor:
    return Cursor(offset=current.offset + advance, file_hash=current.file_hash, report_type=current.report_type)


def build_initial_cursor(file_hash: str, report_type: str, offset: int = 0) -> Cursor:
    return Cursor(offset=offset, file_hash=file_hash, report_type=report_type)


def maybe_decode_cursor(token: Optional[str]) -> Optional[Cursor]:
    if not token:
        return None
    return decode_cursor(token)

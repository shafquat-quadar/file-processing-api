from __future__ import annotations

from pathlib import Path

from .conftest import FIXTURE_DIR
from ..config import reset_settings_cache
from ..file_index import clear_file_cache, get_latest_file_with_hash


def test_onedrive_local_path_with_quotes(monkeypatch) -> None:
    quoted = f'"{FIXTURE_DIR}"'
    monkeypatch.setenv("ONEDRIVE_LOCAL_PATH", quoted)
    reset_settings_cache()
    clear_file_cache()

    record = get_latest_file_with_hash("actions")

    assert record.path.parent == Path(FIXTURE_DIR)

    reset_settings_cache()

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from ..config import reset_settings_cache
from ..file_index import clear_file_cache

FIXTURE_DIR = Path(__file__).parent / "fixtures"
os.environ.setdefault("ONEDRIVE_LOCAL_PATH", str(FIXTURE_DIR))
os.environ.setdefault("API_KEY", "test-key")


@pytest.fixture(scope="session")
def client() -> Generator[TestClient, None, None]:
    reset_settings_cache()
    clear_file_cache()
    from ..app import app

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def _reset_caches() -> Generator[None, None, None]:
    clear_file_cache()
    from ..routers import meta

    meta.schema_cache.clear()
    yield
    clear_file_cache()
    meta.schema_cache.clear()

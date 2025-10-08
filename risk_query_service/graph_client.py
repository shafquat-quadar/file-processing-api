"""Minimal Microsoft Graph client for OneDrive file access."""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from cachetools import TTLCache

from .config import get_settings

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


@dataclass
class GraphFile:
    name: str
    download_url: str
    size: int
    last_modified: str


class GraphClient:
    """Client wrapping the minimal Graph API calls required for the service."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._token_cache: TTLCache[str, str] = TTLCache(maxsize=1, ttl=3500)
        self._listing_cache: TTLCache[str, List[GraphFile]] = TTLCache(maxsize=8, ttl=self.settings.graph_cache_ttl_seconds)

    def _get_token(self) -> str:
        cached = self._token_cache.get("token")
        if cached:
            return cached

        tenant = self.settings.ms_tenant_id
        client_id = self.settings.ms_client_id
        client_secret = self.settings.ms_client_secret
        if not all([tenant, client_id, client_secret]):
            raise RuntimeError("Microsoft Graph credentials are not configured")

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": GRAPH_SCOPE,
        }
        url = TOKEN_URL_TEMPLATE.format(tenant_id=tenant)
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        token = response.json()["access_token"]
        self._token_cache["token"] = token
        return token

    def list_files(self) -> List[GraphFile]:
        cache_key = "files"
        cached = self._listing_cache.get(cache_key)
        if cached is not None:
            return cached

        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}"}
        folder_path = self.settings.ms_folder_path
        if not folder_path:
            raise RuntimeError("MS_FOLDER_PATH must be configured for Microsoft Graph mode")

        drive_id = self.settings.ms_drive_id
        if drive_id:
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{folder_path}:/children"
        else:
            url = f"https://graph.microsoft.com/v1.0/me/drive/root:{folder_path}:/children"

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        values = response.json().get("value", [])

        files: List[GraphFile] = []
        for item in values:
            download_url = item.get("@microsoft.graph.downloadUrl")
            name = item.get("name")
            if not download_url or not name:
                continue
            files.append(
                GraphFile(
                    name=name,
                    download_url=download_url,
                    size=int(item.get("size", 0)),
                    last_modified=item.get("lastModifiedDateTime", ""),
                )
            )

        self._listing_cache[cache_key] = files
        return files

    def ensure_cached(self, filename: str) -> Path:
        """Download the given file if not already cached locally."""

        cache_dir = self.settings.cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / filename
        if target.exists() and (time.time() - target.stat().st_mtime) < self.settings.graph_cache_ttl_seconds:
            return target

        files = self.list_files()
        match = next((file for file in files if file.name == filename), None)
        if not match:
            raise FileNotFoundError(f"File {filename} not found in Microsoft Graph folder")

        response = requests.get(match.download_url, timeout=30)
        response.raise_for_status()
        target.write_bytes(response.content)
        return target


def iter_remote_files(names: Iterable[str]) -> Dict[str, Path]:
    client = GraphClient()
    return {name: client.ensure_cached(name) for name in names}

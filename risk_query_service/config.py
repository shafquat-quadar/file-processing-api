"""Application configuration management."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    api_key: str = Field(default="", validation_alias=AliasChoices("API_KEY"))
    onedrive_local_path: Optional[Path] = Field(default=None, validation_alias=AliasChoices("ONEDRIVE_LOCAL_PATH"))

    ms_tenant_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("MS_TENANT_ID"))
    ms_client_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("MS_CLIENT_ID"))
    ms_client_secret: Optional[str] = Field(default=None, validation_alias=AliasChoices("MS_CLIENT_SECRET"))
    ms_drive_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("MS_DRIVE_ID"))
    ms_folder_path: Optional[str] = Field(default=None, validation_alias=AliasChoices("MS_FOLDER_PATH"))

    cache_dir: Path = Field(default=Path("cache"), validation_alias=AliasChoices("CACHE_DIR"))
    file_index_ttl_seconds: int = Field(default=60, validation_alias=AliasChoices("FILE_INDEX_TTL"))
    graph_cache_ttl_seconds: int = Field(default=900, validation_alias=AliasChoices("GRAPH_CACHE_TTL"))

    enable_cors: bool = Field(default=False, validation_alias=AliasChoices("ENABLE_CORS"))

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    @field_validator("cache_dir", mode="before")
    def _expand_cache_dir(cls, value: str | Path) -> Path:
        path = Path(value)
        return path.expanduser().resolve()

    @field_validator("onedrive_local_path", mode="before")
    def _expand_local_path(cls, value: Optional[str | Path]) -> Optional[Path]:
        if value in (None, ""):
            return None
        if isinstance(value, Path):
            path = value
        else:
            raw = str(value).strip().strip('"').strip("'")
            if not raw:
                return None
            path = Path(raw)
        path = path.expanduser()
        return path.absolute()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""

    settings = Settings()
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    return settings


def reset_settings_cache() -> None:
    """Clear the cached settings (primarily for tests)."""

    get_settings.cache_clear()

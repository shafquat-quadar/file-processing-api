"""Main FastAPI application for the risk query service."""
from __future__ import annotations

from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import ORJSONResponse

from .config import get_settings
from .routers import actions, health, meta, permissions
from .security import API_KEY_HEADER, API_KEY_HEADER_NAME
from .utils.logging import configure_logging
from .utils.swagger2 import SwaggerConversionError, convert_openapi3_to_swagger2

load_dotenv()
configure_logging()

settings = get_settings()

app = FastAPI(
    title="Risk Query Service",
    version="1.0.0",
    default_response_class=ORJSONResponse,
    description="FastAPI service exposing risk action and permission datasets.",
)

headers = ["*"]
if API_KEY_HEADER_NAME not in headers:
    headers.append(API_KEY_HEADER_NAME)

if settings.enable_cors:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=headers,
    )

app.include_router(health.router)
app.include_router(meta.router)
app.include_router(actions.router)
app.include_router(permissions.router)


def custom_openapi() -> Dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    components = openapi_schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes["ApiKeyAuth"] = {
        "type": "apiKey",
        "in": "header",
        "name": API_KEY_HEADER_NAME,
    }
    openapi_schema["security"] = [{"ApiKeyAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore[assignment]


@app.on_event("startup")
async def _build_swagger2() -> None:
    try:
        openapi = app.openapi()
        swagger2 = convert_openapi3_to_swagger2(openapi)
        app.state.swagger2 = swagger2
    except SwaggerConversionError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/swagger2.json", include_in_schema=False)
def get_swagger2() -> Dict[str, Any]:
    swagger2 = getattr(app.state, "swagger2", None)
    if swagger2 is None:
        openapi = app.openapi()
        swagger2 = convert_openapi3_to_swagger2(openapi)
        app.state.swagger2 = swagger2
    return swagger2


__all__ = ["app"]

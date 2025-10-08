"""Convert OpenAPI 3.0 schemas to Swagger 2.0 at runtime."""
from __future__ import annotations

from typing import Any, Dict


class SwaggerConversionError(RuntimeError):
    """Raised when conversion cannot be completed."""


def convert_openapi3_to_swagger2(openapi_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Perform a minimal conversion from OpenAPI 3.0 to Swagger 2.0."""

    if "openapi" not in openapi_schema:
        raise SwaggerConversionError("Expected an OpenAPI 3.0 schema")

    swagger: Dict[str, Any] = {
        "swagger": "2.0",
        "info": openapi_schema.get("info", {}),
        "paths": {},
        "definitions": {},
    }

    servers = openapi_schema.get("servers", [])
    if servers:
        url = servers[0].get("url", "")
        if url.startswith("http://"):
            swagger["schemes"] = ["http"]
            swagger["host"] = url.replace("http://", "", 1).rstrip("/")
        elif url.startswith("https://"):
            swagger["schemes"] = ["https"]
            swagger["host"] = url.replace("https://", "", 1).rstrip("/")
        if "/" in swagger.get("host", ""):
            host, base_path = swagger["host"].split("/", 1)
            swagger["host"] = host
            swagger["basePath"] = f"/{base_path}"

    components = openapi_schema.get("components", {})
    schemas = components.get("schemas", {})
    swagger["definitions"] = schemas

    security_definitions = components.get("securitySchemes", {})
    if security_definitions:
        converted = {}
        for name, scheme in security_definitions.items():
            if scheme.get("type") == "apiKey":
                converted[name] = {
                    "type": "apiKey",
                    "name": scheme.get("name"),
                    "in": scheme.get("in", "header"),
                }
        if converted:
            swagger["securityDefinitions"] = converted

    global_security = openapi_schema.get("security")
    if global_security:
        swagger["security"] = global_security

    for path, methods in openapi_schema.get("paths", {}).items():
        swagger_methods: Dict[str, Any] = {}
        for method, operation in methods.items():
            new_operation = dict(operation)
            responses = new_operation.get("responses", {})
            for status, response in list(responses.items()):
                content = response.get("content")
                if content:
                    json_schema = content.get("application/json", {}).get("schema")
                    if json_schema:
                        response = dict(response)
                        response["schema"] = json_schema
                    response.pop("content", None)
                    responses[status] = response
            new_operation["responses"] = responses

            parameters = []
            for param in new_operation.get("parameters", []):
                param = dict(param)
                schema = param.pop("schema", None)
                if schema is not None:
                    param["type"] = schema.get("type")
                    if schema.get("enum"):
                        param["enum"] = schema["enum"]
                    if schema.get("items"):
                        param["items"] = schema["items"]
                    if schema.get("format"):
                        param["format"] = schema["format"]
                parameters.append(param)
            new_operation["parameters"] = parameters

            new_operation.pop("callbacks", None)
            new_operation.pop("servers", None)
            swagger_methods[method] = new_operation
        swagger["paths"][path] = swagger_methods

    return swagger

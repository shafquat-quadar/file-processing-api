from __future__ import annotations

from typing import Dict

from fastapi.testclient import TestClient

API_HEADERS = {"x-api-key": "test-key"}


def test_schema_endpoint(client: TestClient) -> None:
    response = client.get("/meta/schema", headers=API_HEADERS)
    assert response.status_code == 200
    schema: Dict[str, str] = response.json()
    assert "User ID" in schema
    assert "ReportType" in schema


def test_facets_endpoint(client: TestClient) -> None:
    response = client.get("/meta/facets", params={"column": "Risk Level", "n": 5}, headers=API_HEADERS)
    assert response.status_code == 200
    facets = response.json()
    assert any(item["value"] == "High" for item in facets)

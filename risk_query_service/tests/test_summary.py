from __future__ import annotations

from fastapi.testclient import TestClient

API_HEADERS = {"x-api-key": "test-key"}


def test_actions_summary_groupby(client: TestClient) -> None:
    response = client.get(
        "/risk/actions/summary",
        params={"groupby": "Risk Level", "top": 10},
        headers=API_HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()
    levels = {item["group"] for item in payload["data"]}
    assert "High" in levels
    assert payload["report_type"] == "actions"


def test_permissions_summary_groupby(client: TestClient) -> None:
    response = client.get(
        "/risk/permissions/summary",
        params={"groupby": "System"},
        headers=API_HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()
    systems = {item["group"] for item in payload["data"]}
    assert "SYS4" in systems

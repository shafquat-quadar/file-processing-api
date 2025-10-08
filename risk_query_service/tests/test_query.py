from __future__ import annotations

from fastapi.testclient import TestClient

API_HEADERS = {"x-api-key": "test-key"}


def test_actions_query_filter(client: TestClient) -> None:
    response = client.get(
        "/risk/actions/query",
        params={"risk_level": "High", "limit": 5},
        headers=API_HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["has_more"] in {True, False}
    assert payload["report_type"] == "actions"
    for row in payload["data"]:
        assert row["Risk Level"].lower() == "high"


def test_permissions_query_user_search(client: TestClient) -> None:
    response = client.get(
        "/risk/permissions/query",
        params={"user": "Frank", "limit": 5},
        headers=API_HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]
    assert payload["data"][0]["User Name"] == "Frank Hall"

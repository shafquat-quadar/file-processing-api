from __future__ import annotations

from fastapi.testclient import TestClient

API_HEADERS = {"x-api-key": "test-key"}


def test_cursor_pagination(client: TestClient) -> None:
    first = client.get(
        "/risk/actions/query",
        params={"limit": 1, "columns": "User Name,Risk Level"},
        headers=API_HEADERS,
    )
    assert first.status_code == 200
    payload = first.json()
    assert payload["has_more"] is True
    cursor = payload["cursor"]
    assert cursor
    first_name = payload["data"][0]["User Name"]

    second = client.get(
        "/risk/actions/query",
        params={"limit": 1, "cursor": cursor},
        headers=API_HEADERS,
    )
    assert second.status_code == 200
    payload2 = second.json()
    assert payload2["data"]
    assert payload2["data"][0]["User Name"] != first_name or payload2["has_more"] is False

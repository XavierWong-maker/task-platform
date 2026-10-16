from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from task_platform.api.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(f"sqlite:///{tmp_path / 'api_jobs.db'}")
    with TestClient(app) as c:
        yield c


def test_list_jobs_returns_page_envelope(client: TestClient) -> None:
    body = client.get("/api/v1/jobs").json()
    assert body["total"] >= 3
    assert body["page"] == 1 and body["page_size"] == 20
    assert {"demo-success", "demo-failure", "demo-slow"} <= {j["name"] for j in body["items"]}
    first = body["items"][0]
    assert {"id", "name", "job_type", "handler", "retry_policy", "enabled", "created_at"} <= set(
        first
    )


def test_pagination_slices_items(client: TestClient) -> None:
    body = client.get("/api/v1/jobs", params={"page": 1, "page_size": 2}).json()
    assert len(body["items"]) == 2
    assert body["total_pages"] == -(-body["total"] // 2)


def test_filter_by_enabled(client: TestClient) -> None:
    assert client.get("/api/v1/jobs", params={"enabled": False}).json()["total"] == 0
    assert client.get("/api/v1/jobs", params={"enabled": True}).json()["total"] >= 3


@pytest.mark.parametrize("params", [{"page": 0}, {"page_size": 0}, {"page_size": 101}])
def test_invalid_pagination_returns_unified_422(client: TestClient, params: dict[str, int]) -> None:
    resp = client.get("/api/v1/jobs", params=params)
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "validation_error"
    assert error["details"][0]["field"].startswith("query.")


def test_get_job_detail(client: TestClient) -> None:
    job = client.get("/api/v1/jobs").json()["items"][0]
    resp = client.get(f"/api/v1/jobs/{job['id']}")
    assert resp.status_code == 200
    assert resp.json() == job


def test_get_unknown_job_returns_unified_404(client: TestClient) -> None:
    resp = client.get("/api/v1/jobs/job_nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "job_not_found"


def test_unknown_route_uses_same_error_format(client: TestClient) -> None:
    resp = client.get("/no/such/route")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"

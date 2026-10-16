from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from task_platform.api.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(f"sqlite:///{tmp_path / 'api_write.db'}")
    with TestClient(app) as c:
        yield c


def _create(client: TestClient, name: str = "my-job", **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"name": name, "handler": "demo.success", **overrides}
    resp = client.post("/api/v1/jobs", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


# ---------------- 创建 ----------------


def test_create_job_applies_defaults_and_persists(client: TestClient) -> None:
    job = _create(client)
    assert job["job_type"] == "CUSTOM"
    assert job["timeout_seconds"] == 30.0
    assert job["enabled"] is True
    assert job["retry_policy"]["max_attempts"] == 3
    assert client.get(f"/api/v1/jobs/{job['id']}").json() == job


def test_create_with_custom_retry_policy(client: TestClient) -> None:
    job = _create(
        client,
        retry_policy={"max_attempts": 5, "backoff": "EXPONENTIAL", "base_delay_seconds": 0.5},
    )
    assert job["retry_policy"]["max_attempts"] == 5
    assert job["retry_policy"]["backoff"] == "EXPONENTIAL"


def test_create_duplicate_name_returns_409(client: TestClient) -> None:
    _create(client)
    resp = client.post("/api/v1/jobs", json={"name": "my-job", "handler": "demo.success"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "job_name_conflict"


def test_create_with_unregistered_handler_returns_422(client: TestClient) -> None:
    resp = client.post("/api/v1/jobs", json={"name": "x", "handler": "no.such.handler"})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "handler_not_registered"


@pytest.mark.parametrize(
    "body",
    [
        {"name": "  ", "handler": "demo.success"},
        {"name": "x", "handler": "demo.success", "timeout_seconds": 0},
        {"name": "x", "handler": "demo.success", "retry_policy": {"max_attempts": 0}},
        {
            "name": "x",
            "handler": "demo.success",
            "retry_policy": {"base_delay_seconds": 10, "max_delay_seconds": 1},
        },
        {"name": "x", "handler": "demo.success", "unknown_field": 1},
    ],
)
def test_create_invalid_body_returns_unified_422(client: TestClient, body: dict[str, Any]) -> None:
    resp = client.post("/api/v1/jobs", json=body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


# ---------------- 修改 ----------------


def test_patch_updates_only_given_fields(client: TestClient) -> None:
    job = _create(client, timeout_seconds=10)
    resp = client.patch(f"/api/v1/jobs/{job['id']}", json={"timeout_seconds": 99})
    assert resp.status_code == 200
    updated = resp.json()
    assert updated["timeout_seconds"] == 99
    assert updated["name"] == job["name"]
    assert updated["retry_policy"] == job["retry_policy"]
    assert client.get(f"/api/v1/jobs/{job['id']}").json() == updated


def test_patch_renaming_to_existing_name_returns_409(client: TestClient) -> None:
    _create(client, "job-a")
    other = _create(client, "job-b")
    resp = client.patch(f"/api/v1/jobs/{other['id']}", json={"name": "job-a"})
    assert resp.status_code == 409


def test_patch_same_name_is_allowed(client: TestClient) -> None:
    job = _create(client)
    resp = client.patch(f"/api/v1/jobs/{job['id']}", json={"name": job["name"]})
    assert resp.status_code == 200


@pytest.mark.parametrize(
    "body", [{}, {"name": None}, {"handler": "demo.failure"}, {"job_type": "DEMO_SLOW"}]
)
def test_patch_rejects_empty_null_or_immutable_fields(
    client: TestClient, body: dict[str, Any]
) -> None:
    job = _create(client)
    resp = client.patch(f"/api/v1/jobs/{job['id']}", json=body)
    assert resp.status_code == 422


def test_patch_unknown_job_returns_404(client: TestClient) -> None:
    resp = client.patch("/api/v1/jobs/job_nope", json={"enabled": False})
    assert resp.status_code == 404


# ---------------- 删除 ----------------


def test_delete_job_then_404(client: TestClient) -> None:
    job = _create(client)
    assert client.delete(f"/api/v1/jobs/{job['id']}").status_code == 204
    assert client.get(f"/api/v1/jobs/{job['id']}").status_code == 404
    assert client.delete(f"/api/v1/jobs/{job['id']}").status_code == 404


# ---------------- pause / resume ----------------


def test_pause_and_resume_toggle_enabled_and_are_idempotent(client: TestClient) -> None:
    job = _create(client)
    url = f"/api/v1/jobs/{job['id']}"

    assert client.post(f"{url}/pause").json()["enabled"] is False
    assert client.post(f"{url}/pause").json()["enabled"] is False  # 幂等
    assert client.get(url).json()["enabled"] is False
    assert client.get("/api/v1/jobs", params={"enabled": False}).json()["total"] == 1

    assert client.post(f"{url}/resume").json()["enabled"] is True
    assert client.get(url).json()["enabled"] is True


def test_pause_unknown_job_returns_404(client: TestClient) -> None:
    assert client.post("/api/v1/jobs/job_nope/pause").status_code == 404
    assert client.post("/api/v1/jobs/job_nope/resume").status_code == 404

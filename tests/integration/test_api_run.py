from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from task_platform.api.app import create_app
from task_platform.domain.enums import JobType
from task_platform.domain.models import Job
from task_platform.infrastructure.db.repositories import JobLogRepository, JobRepository
from task_platform.infrastructure.db.session import session_scope


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(f"sqlite:///{tmp_path / 'api_run.db'}")
    with TestClient(app) as c:
        yield c


def _job_id(client: TestClient, name: str) -> str:
    items = client.get("/api/v1/jobs", params={"page_size": 100}).json()["items"]
    return next(j["id"] for j in items if j["name"] == name)


def _run(client: TestClient, name: str) -> dict[str, Any]:
    resp = client.post(f"/api/v1/jobs/{_job_id(client, name)}/run")
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


# ---------------- 手动触发 ----------------


def test_run_success_returns_and_persists_execution(client: TestClient) -> None:
    run = _run(client, "demo-success")
    assert run["status"] == "SUCCESS"
    assert run["attempt"] == 1
    assert run["error_message"] is None
    assert run["started_at"] is not None and run["finished_at"] is not None

    page = client.get("/api/v1/executions").json()
    assert page["total"] == 1
    assert page["items"][0] == run


def test_run_failure_records_error_and_retrying_status(client: TestClient) -> None:
    run = _run(client, "demo-failure")
    # 手动触发只执行一次尝试；还有重试额度时状态为 RETRYING（与 CLI 一致）
    assert run["status"] == "RETRYING"
    assert "demo_failure" in run["error_message"]


def test_run_persists_logs(client: TestClient) -> None:
    run = _run(client, "demo-success")
    factory = client.app.state.session_factory  # type: ignore[attr-defined]
    with session_scope(factory) as s:
        logs = JobLogRepository(s).list_by_run(run["run_id"]).items
    assert any("job attempt started" in log.message for log in logs)


def test_paused_job_can_still_be_run_manually(client: TestClient) -> None:
    job_id = _job_id(client, "demo-success")
    client.post(f"/api/v1/jobs/{job_id}/pause")
    assert client.post(f"/api/v1/jobs/{job_id}/run").status_code == 201


def test_run_unknown_job_returns_404(client: TestClient) -> None:
    assert client.post("/api/v1/jobs/job_nope/run").status_code == 404


def test_run_job_with_unregistered_handler_returns_409(client: TestClient) -> None:
    orphan = Job(name="orphan", job_type=JobType.CUSTOM, handler="gone.handler")
    factory = client.app.state.session_factory  # type: ignore[attr-defined]
    with session_scope(factory) as s:
        JobRepository(s).add(orphan)

    resp = client.post(f"/api/v1/jobs/{orphan.id}/run")
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "handler_not_registered"
    assert client.get("/api/v1/executions").json()["total"] == 0  # 不留下残缺记录


# ---------------- 执行记录查询 ----------------


def test_executions_are_most_recent_first(client: TestClient) -> None:
    first = _run(client, "demo-success")
    second = _run(client, "demo-success")
    items = client.get("/api/v1/executions").json()["items"]
    assert [e["run_id"] for e in items] == [second["run_id"], first["run_id"]]


def test_filter_by_job_id_and_status(client: TestClient) -> None:
    _run(client, "demo-success")
    _run(client, "demo-failure")

    by_job = client.get("/api/v1/executions", params={"job_id": _job_id(client, "demo-failure")})
    assert by_job.json()["total"] == 1

    by_status = client.get("/api/v1/executions", params={"status": "SUCCESS"})
    assert by_status.json()["total"] == 1


def test_filter_by_time_range(client: TestClient) -> None:
    _run(client, "demo-success")
    now = datetime.now(UTC)

    inside = client.get(
        "/api/v1/executions", params={"started_after": (now - timedelta(hours=1)).isoformat()}
    )
    assert inside.json()["total"] == 1

    outside = client.get(
        "/api/v1/executions", params={"started_after": (now + timedelta(hours=1)).isoformat()}
    )
    assert outside.json()["total"] == 0


def test_naive_datetime_filter_returns_422(client: TestClient) -> None:
    resp = client.get("/api/v1/executions", params={"started_after": "2026-10-01T00:00:00"})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


def test_invalid_status_and_pagination_return_422(client: TestClient) -> None:
    assert client.get("/api/v1/executions", params={"status": "BOGUS"}).status_code == 422
    assert client.get("/api/v1/executions", params={"page": 0}).status_code == 422


def test_executions_pagination(client: TestClient) -> None:
    for _ in range(3):
        _run(client, "demo-success")
    body = client.get("/api/v1/executions", params={"page": 2, "page_size": 2}).json()
    assert body["total"] == 3 and body["total_pages"] == 2
    assert len(body["items"]) == 1

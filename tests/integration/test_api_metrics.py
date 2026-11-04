from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from task_platform.api.app import create_app
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("TASK_PLATFORM_QUEUE_STREAM", "test:stream")
    app = create_app(f"sqlite:///{tmp_path / 'api_metrics.db'}", queue=MemoryQueue())
    with TestClient(app) as c:
        yield c


def _run(client: TestClient, name: str) -> None:
    items = client.get("/api/v1/jobs", params={"page_size": 100}).json()["items"]
    job_id = next(j["id"] for j in items if j["name"] == name)
    assert client.post(f"/api/v1/jobs/{job_id}/run").status_code == 201


def _job(body: dict[str, Any], name: str) -> dict[str, Any]:
    return next(j for j in body["jobs"] if j["job"] == name)


def test_empty_metrics(client: TestClient) -> None:
    body = client.get("/api/v1/metrics").json()
    assert body["jobs"] == [] and body["window_seconds"] is None
    assert body["queue"] == {"depth": 0, "pending": 0}


def test_metrics_reflect_runs(client: TestClient) -> None:
    _run(client, "demo-success")
    _run(client, "demo-failure")

    body = client.get("/api/v1/metrics").json()

    ok = _job(body, "demo-success")
    assert ok["status_counts"]["SUCCESS"] == 1
    assert ok["success_rate"] == 1.0 and ok["failure_rate"] == 0.0
    assert ok["avg_duration_seconds"] is not None

    bad = _job(body, "demo-failure")
    assert bad["status_counts"]["RETRYING"] == 1
    assert bad["success_rate"] is None  # 还没有任何 SUCCESS / FAILED 的终态
    assert bad["retry_attempts"] == 0  # 手动触发只有第 1 次尝试


def test_window_parameter(client: TestClient) -> None:
    _run(client, "demo-success")
    body = client.get("/api/v1/metrics", params={"window_seconds": 3600}).json()
    assert body["window_seconds"] == 3600
    assert _job(body, "demo-success")["status_counts"]["SUCCESS"] == 1


@pytest.mark.parametrize("value", [0, -1, 400_000_000])
def test_invalid_window_returns_unified_422(client: TestClient, value: float) -> None:
    resp = client.get("/api/v1/metrics", params={"window_seconds": value})
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


def test_queue_depth_is_reported(client: TestClient) -> None:
    queue = client.app.state.queue  # type: ignore[attr-defined]
    queue.publish(TaskMessage("job_1", "run_1"))
    queue.publish(TaskMessage("job_1", "run_2"))
    body = client.get("/api/v1/metrics").json()
    assert body["queue"] == {"depth": 2, "pending": 0}


def test_prometheus_endpoint(client: TestClient) -> None:
    _run(client, "demo-success")

    resp = client.get("/metrics")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert 'job_execution_total{job="demo-success",status="SUCCESS"} 1' in resp.text
    assert 'queue_depth{queue="test:stream"} 0' in resp.text


def test_without_queue_metrics_still_work(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TASK_PLATFORM_METRICS_QUEUE", raising=False)
    with TestClient(create_app(f"sqlite:///{tmp_path / 'no_queue.db'}")) as c:
        body = c.get("/api/v1/metrics").json()
        assert body["queue"] == {"depth": None, "pending": None}
        assert "queue_depth" not in c.get("/metrics").text

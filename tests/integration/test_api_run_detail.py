from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from task_platform.api.app import create_app
from task_platform.cli.main import main
from task_platform.domain.enums import JobStatus
from task_platform.domain.models import JobExecution, JobLog
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobLogRepository
from task_platform.infrastructure.db.session import session_scope


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(f"sqlite:///{tmp_path / 'api_detail.db'}")
    with TestClient(app) as c:
        yield c


def _job_id(client: TestClient, name: str) -> str:
    items = client.get("/api/v1/jobs", params={"page_size": 100}).json()["items"]
    return next(j["id"] for j in items if j["name"] == name)


def _seed_two_attempts(client: TestClient) -> str:
    """同一个 run_id 下手工造两次尝试：attempt 1 RETRYING，attempt 2 SUCCESS，各带日志"""
    job_id = _job_id(client, "demo-success")
    factory = client.app.state.session_factory  # type: ignore[attr-defined]
    with session_scope(factory) as s:
        repo = ExecutionRepository(s)
        repo.add(
            JobExecution(job_id=job_id, run_id="run_multi", attempt=2, status=JobStatus.SUCCESS)
        )
        repo.add(
            JobExecution(job_id=job_id, run_id="run_multi", attempt=1, status=JobStatus.RETRYING)
        )
        JobLogRepository(s).add_many(
            [
                JobLog("run_multi", 1, "WARNING", "first failed"),
                JobLog("run_multi", 2, "INFO", "second ok"),
            ]
        )
    return "run_multi"


# ---------------- 运行详情 ----------------


def test_run_detail_lists_attempts_in_order_and_uses_last_status(client: TestClient) -> None:
    run_id = _seed_two_attempts(client)
    body = client.get(f"/api/v1/executions/{run_id}").json()

    assert body["run_id"] == run_id
    assert body["status"] == "SUCCESS"
    assert [a["attempt"] for a in body["attempts"]] == [1, 2]
    assert body["job_id"] == _job_id(client, "demo-success")


def test_run_detail_unknown_returns_404(client: TestClient) -> None:
    resp = client.get("/api/v1/executions/run_nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "run_not_found"


# ---------------- 日志 ----------------


def test_logs_returns_all_attempts_in_insert_order(client: TestClient) -> None:
    run_id = _seed_two_attempts(client)
    body = client.get(f"/api/v1/executions/{run_id}/logs").json()
    assert body["total"] == 2
    assert [log["message"] for log in body["items"]] == ["first failed", "second ok"]


def test_logs_filter_by_attempt_and_paginate(client: TestClient) -> None:
    run_id = _seed_two_attempts(client)
    only_second = client.get(f"/api/v1/executions/{run_id}/logs", params={"attempt": 2}).json()
    assert [log["message"] for log in only_second["items"]] == ["second ok"]

    page2 = client.get(
        f"/api/v1/executions/{run_id}/logs", params={"page": 2, "page_size": 1}
    ).json()
    assert page2["total"] == 2 and [log["message"] for log in page2["items"]] == ["second ok"]


def test_logs_unknown_run_returns_404_and_bad_params_422(client: TestClient) -> None:
    assert client.get("/api/v1/executions/run_nope/logs").status_code == 404
    run_id = _seed_two_attempts(client)
    assert client.get(f"/api/v1/executions/{run_id}/logs", params={"attempt": 0}).status_code == 422
    assert (
        client.get(f"/api/v1/executions/{run_id}/logs", params={"page_size": 1001}).status_code
        == 422
    )


# ---------------- API 与 CLI 一致性 ----------------

_COMPARED = ("status", "attempt", "error_message", "timeout_seconds", "max_attempts")


def _summarize(client: TestClient) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
    """取唯一一条执行记录的业务字段，以及它的日志 (attempt, level, message)"""
    [execution] = client.get("/api/v1/executions").json()["items"]
    logs = client.get(f"/api/v1/executions/{execution['run_id']}/logs").json()["items"]
    return (
        {k: execution[k] for k in _COMPARED},
        [(log["attempt"], log["level"], log["message"]) for log in logs],
    )


@pytest.mark.parametrize(("job_name", "cli_exit_code"), [("demo-success", 0), ("demo-failure", 1)])
def test_cli_and_api_runs_produce_same_business_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, job_name: str, cli_exit_code: int
) -> None:
    cli_url = f"sqlite:///{tmp_path / 'cli.db'}"
    api_url = f"sqlite:///{tmp_path / 'api.db'}"

    monkeypatch.setenv("TASK_PLATFORM_DB_URL", cli_url)
    assert main(["run", job_name]) == cli_exit_code

    with TestClient(create_app(api_url)) as api_client:
        resp = api_client.post(f"/api/v1/jobs/{_job_id(api_client, job_name)}/run")
        assert resp.status_code == 201
        api_summary = _summarize(api_client)

    # CLI 写入的库，同样可以被 API 读出来
    with TestClient(create_app(cli_url)) as cli_view:
        cli_summary = _summarize(cli_view)

    assert api_summary == cli_summary

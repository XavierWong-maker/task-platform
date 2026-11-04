from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from task_platform.alerts import AlertRules
from task_platform.api.app import create_app
from task_platform.cli.main import main
from task_platform.domain.enums import JobStatus
from task_platform.domain.models import JobExecution, utc_now
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue


def _seed_failures(factory, job_name: str, n: int) -> None:  # type: ignore[no-untyped-def]
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name(job_name)
        assert job is not None
        repo = ExecutionRepository(s)
        for i in range(n):
            now = utc_now()
            repo.add(
                JobExecution(
                    job_id=job.id,
                    run_id=f"fail_{i}",
                    attempt=1,
                    status=JobStatus.FAILED,
                    started_at=now,
                    finished_at=now,
                )
            )


# ---------------- CLI ----------------


@pytest.fixture
def db_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    url = f"sqlite:///{tmp_path / 'alerts_cli.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    assert main(["list"]) == 0  # 建库并同步 demo 任务
    return url


def _seed_cli(url: str, n: int) -> None:
    engine = create_db_engine(url)
    try:
        _seed_failures(make_session_factory(engine), "demo-failure", n)
    finally:
        engine.dispose()


def test_cli_no_alerts_returns_zero(db_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    capsys.readouterr()
    assert main(["alerts"]) == 0
    assert "无告警" in capsys.readouterr().out


def test_cli_firing_alert_returns_one(db_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    _seed_cli(db_url, 3)
    capsys.readouterr()

    assert main(["alerts"]) == 1

    out = capsys.readouterr().out
    assert "CRITICAL" in out and "consecutive_failures" in out and "demo-failure" in out


def test_cli_threshold_from_env(
    db_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_cli(db_url, 3)
    monkeypatch.setenv("TASK_PLATFORM_ALERT_CONSECUTIVE_FAILURES", "4")
    assert main(["alerts"]) == 0


def test_cli_invalid_threshold_or_watch_returns_one(
    db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert main(["alerts", "--watch", "0"]) == 1
    monkeypatch.setenv("TASK_PLATFORM_ALERT_QUEUE_DEPTH", "0")
    assert main(["alerts"]) == 1


def test_cli_with_queue_reports_backlog(
    db_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    queue = MemoryQueue()
    for i in range(2):
        queue.publish(TaskMessage("job_1", f"run_{i}"))
    monkeypatch.setattr("task_platform.cli.alert_commands._open_queue", lambda: queue)
    monkeypatch.setenv("TASK_PLATFORM_ALERT_QUEUE_DEPTH", "2")
    capsys.readouterr()

    assert main(["alerts", "--with-queue"]) == 1
    assert "queue_backlog" in capsys.readouterr().out


def test_cli_with_queue_fails_when_queue_unavailable(
    db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("task_platform.cli.alert_commands._open_queue", lambda: None)
    assert main(["alerts", "--with-queue"]) == 1


# ---------------- API ----------------


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(
        f"sqlite:///{tmp_path / 'alerts_api.db'}",
        queue=MemoryQueue(),
        alert_rules=AlertRules(consecutive_failures=3, queue_depth=2),
    )
    with TestClient(app) as c:
        yield c


def test_api_no_alerts(client: TestClient) -> None:
    body = client.get("/api/v1/alerts").json()
    assert body["alerts"] == []
    assert "generated_at" in body


def test_api_lists_firing_alerts(client: TestClient) -> None:
    _seed_failures(client.app.state.session_factory, "demo-success", 3)  # type: ignore[attr-defined]
    queue = client.app.state.queue  # type: ignore[attr-defined]
    queue.publish(TaskMessage("job_1", "run_1"))
    queue.publish(TaskMessage("job_1", "run_2"))

    alerts = client.get("/api/v1/alerts").json()["alerts"]

    by_rule = {a["rule"]: a for a in alerts}
    assert set(by_rule) == {"consecutive_failures", "queue_backlog"}
    assert by_rule["consecutive_failures"]["subject"] == "demo-success"
    assert by_rule["consecutive_failures"]["severity"] == "CRITICAL"
    assert (by_rule["queue_backlog"]["value"], by_rule["queue_backlog"]["threshold"]) == (2, 2)


def test_api_invalid_alert_env_fails_at_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TASK_PLATFORM_ALERT_QUEUE_DEPTH", "0")
    with pytest.raises(ValueError):
        create_app(f"sqlite:///{tmp_path / 'bad.db'}")

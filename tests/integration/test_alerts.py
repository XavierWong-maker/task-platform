from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.alerts import AlertRules, Severity, evaluate_alerts
from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution
from task_platform.executor import TIMEOUT_ERROR_PREFIX
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
RULES = AlertRules(consecutive_failures=3, timeout_count=3, queue_depth=3)
F, S, R = JobStatus.FAILED, JobStatus.SUCCESS, JobStatus.RETRYING


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_db_engine("sqlite://")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _add_job(factory: sessionmaker[Session], name: str = "job-a", *, enabled: bool = True) -> Job:
    job = Job(name=name, job_type=JobType.CUSTOM, handler="demo.ok", enabled=enabled)
    with session_scope(factory) as s:
        JobRepository(s).add(job)
    return job


def _add_runs(
    factory: sessionmaker[Session],
    job: Job,
    statuses: list[JobStatus],
    *,
    minutes_ago: int = 10,
    message: str | None = None,
) -> None:
    """按给定顺序（最旧在前）插入执行记录，id 递增"""
    with session_scope(factory) as s:
        repo = ExecutionRepository(s)
        for i, status in enumerate(statuses):
            started = NOW - timedelta(minutes=minutes_ago)
            repo.add(
                JobExecution(
                    job_id=job.id,
                    run_id=f"{job.name}-{minutes_ago}-{i}",
                    attempt=1,
                    status=status,
                    started_at=started,
                    finished_at=started + timedelta(seconds=1),
                    error_message=message,
                )
            )


def _evaluate(factory: sessionmaker[Session], queue: MemoryQueue | None = None) -> list[tuple]:  # type: ignore[type-arg]
    with session_scope(factory) as s:
        alerts = evaluate_alerts(s, queue, rules=RULES, queue_name="q", now=NOW)
    return [(a.rule, a.subject) for a in alerts]


def test_healthy_system_has_no_alerts(factory: sessionmaker[Session]) -> None:
    _add_runs(factory, _add_job(factory), [S, S, S])
    assert _evaluate(factory) == []


# ---------------- 连续失败 ----------------


def test_three_consecutive_failures_fire_critical_alert(factory: sessionmaker[Session]) -> None:
    _add_runs(factory, _add_job(factory), [S, F, F, F])
    with session_scope(factory) as s:
        [alert] = evaluate_alerts(s, rules=RULES, now=NOW)
    assert (alert.rule, alert.subject, alert.severity) == (
        "consecutive_failures",
        "job-a",
        Severity.CRITICAL,
    )
    assert (alert.value, alert.threshold) == (3, 3)


def test_streak_counts_from_the_newest_run(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    _add_runs(factory, job, [F, F, F, S, F, F])  # 最新的连续失败只有 2 次
    assert _evaluate(factory) == []


def test_too_few_failures_do_not_fire(factory: sessionmaker[Session]) -> None:
    _add_runs(factory, _add_job(factory), [F, F])
    assert _evaluate(factory) == []


def test_retrying_rows_are_not_final_results(factory: sessionmaker[Session]) -> None:
    _add_runs(factory, _add_job(factory), [F, R, F, R, F])  # 去掉 RETRYING 后是 3 次连续 FAILED
    assert _evaluate(factory) == [("consecutive_failures", "job-a")]


def test_disabled_job_is_ignored(factory: sessionmaker[Session]) -> None:
    _add_runs(factory, _add_job(factory, enabled=False), [F, F, F, F])
    assert _evaluate(factory) == []


# ---------------- 超时 ----------------


def test_timeouts_inside_window_fire(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    _add_runs(factory, job, [F, R, R], message=f"{TIMEOUT_ERROR_PREFIX} 5s 仍未完成")
    # 另有一次成功，避免同时触发连续失败规则
    _add_runs(factory, job, [S], minutes_ago=1)
    assert _evaluate(factory) == [("timeouts", "job-a")]


def test_old_timeouts_outside_window_are_ignored(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    _add_runs(factory, job, [R, R], message=f"{TIMEOUT_ERROR_PREFIX} 5s")
    _add_runs(factory, job, [R], minutes_ago=24 * 60, message=f"{TIMEOUT_ERROR_PREFIX} 5s")
    assert _evaluate(factory) == []  # 窗口内只有 2 次


def test_non_timeout_errors_are_not_counted(factory: sessionmaker[Session]) -> None:
    _add_runs(factory, _add_job(factory), [R, R, R], message="boom")
    assert _evaluate(factory) == []


# ---------------- 队列堆积 ----------------


def _queue_with(n: int) -> MemoryQueue:
    queue = MemoryQueue()
    for i in range(n):
        queue.publish(TaskMessage("job_1", f"run_{i}"))
    return queue


def test_queue_backlog_fires_at_threshold(factory: sessionmaker[Session]) -> None:
    assert _evaluate(factory, _queue_with(2)) == []
    assert _evaluate(factory, _queue_with(3)) == [("queue_backlog", "q")]


def test_consumed_messages_do_not_count_as_backlog(factory: sessionmaker[Session]) -> None:
    queue = _queue_with(4)
    queue.consume("w1")  # 1 条进入 pending，depth 变为 3
    assert _evaluate(factory, queue) == [("queue_backlog", "q")]
    queue.consume("w1")  # depth 变为 2
    assert _evaluate(factory, queue) == []


def test_unreadable_queue_produces_no_alert(factory: sessionmaker[Session]) -> None:
    class DownQueue(MemoryQueue):
        def depth(self) -> int:
            raise ConnectionError("redis down")

    assert _evaluate(factory, DownQueue()) == []

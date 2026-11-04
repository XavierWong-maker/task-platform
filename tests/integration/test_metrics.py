from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue
from task_platform.metrics import JobMetrics, collect_metrics

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_db_engine("sqlite://")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _ex(
    job: Job,
    run_id: str,
    status: JobStatus,
    *,
    attempt: int = 1,
    ago: timedelta = timedelta(hours=1),
    seconds: float | None = 1.0,
) -> JobExecution:
    started = NOW - ago
    finished = started + timedelta(seconds=seconds) if seconds is not None else None
    return JobExecution(
        job_id=job.id,
        run_id=run_id,
        attempt=attempt,
        status=status,
        started_at=started,
        finished_at=finished,
    )


@pytest.fixture
def seeded(factory: sessionmaker[Session]) -> sessionmaker[Session]:
    job_a = Job(name="job-a", job_type=JobType.CUSTOM, handler="demo.ok")
    job_b = Job(name="job-b", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        jobs = JobRepository(s)
        jobs.add(job_a)
        jobs.add(job_b)
        repo = ExecutionRepository(s)
        repo.add(_ex(job_a, "a1", JobStatus.RETRYING, seconds=1))
        repo.add(_ex(job_a, "a1", JobStatus.SUCCESS, attempt=2, seconds=2))
        repo.add(_ex(job_a, "a2", JobStatus.SUCCESS, seconds=4))
        repo.add(_ex(job_a, "a3", JobStatus.FAILED, seconds=6))
        repo.add(_ex(job_a, "a_old", JobStatus.SUCCESS, ago=timedelta(days=3), seconds=100))
        repo.add(_ex(job_b, "b1", JobStatus.CANCELED, seconds=None))
    return factory


def test_empty_database_gives_empty_snapshot(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        snap = collect_metrics(s, now=NOW)
    assert snap.jobs == []
    assert (snap.queue_depth, snap.queue_pending) == (None, None)


def test_all_time_counts_rates_and_durations(seeded: sessionmaker[Session]) -> None:
    with session_scope(seeded) as s:
        snap = collect_metrics(s, now=NOW)

    assert [j.job for j in snap.jobs] == ["job-a", "job-b"]
    a = snap.get("job-a")
    assert a is not None
    assert (a.status_counts["SUCCESS"], a.status_counts["FAILED"]) == (3, 1)
    assert a.status_counts["RETRYING"] == 1 and a.status_counts["QUEUED"] == 0
    assert a.retry_attempts == 1
    assert (a.duration_count, a.duration_sum, a.duration_max) == (4, 112.0, 100.0)


def test_window_excludes_old_executions(seeded: sessionmaker[Session]) -> None:
    with session_scope(seeded) as s:
        snap = collect_metrics(s, window=timedelta(days=1), now=NOW)

    a = snap.get("job-a")
    assert a is not None
    assert (a.success_count, a.failed_count) == (2, 1)
    assert a.success_rate == pytest.approx(2 / 3)
    assert a.failure_rate == pytest.approx(1 / 3)
    assert a.avg_duration_seconds == pytest.approx(4.0)  # (2 + 4 + 6) / 3
    assert a.duration_max == 6.0
    assert a.retry_attempts == 1


def test_job_without_finished_runs_has_no_rates(seeded: sessionmaker[Session]) -> None:
    with session_scope(seeded) as s:
        snap = collect_metrics(s, now=NOW)

    b = snap.get("job-b")
    assert b is not None
    assert b.status_counts["CANCELED"] == 1
    assert b.success_rate is None and b.failure_rate is None
    assert b.avg_duration_seconds is None
    assert snap.get("nope") is None


def test_non_positive_window_is_rejected(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s, pytest.raises(ValueError):
        collect_metrics(s, window=timedelta(0))


def test_queue_stats_are_reported(factory: sessionmaker[Session]) -> None:
    queue = MemoryQueue()
    queue.publish(TaskMessage("job_1", "run_1"))
    queue.publish(TaskMessage("job_1", "run_2"))
    queue.consume("w1")

    with session_scope(factory) as s:
        snap = collect_metrics(s, queue, now=NOW)

    assert (snap.queue_depth, snap.queue_pending) == (1, 1)


def test_broken_queue_does_not_fail_the_snapshot(factory: sessionmaker[Session]) -> None:
    class DownQueue(MemoryQueue):
        def depth(self) -> int:
            raise ConnectionError("redis down")

    with session_scope(factory) as s:
        snap = collect_metrics(s, DownQueue(), now=NOW)

    assert (snap.queue_depth, snap.queue_pending) == (None, None)


def test_job_metrics_properties_with_no_finished_runs() -> None:
    m = JobMetrics("x", {s.value: 0 for s in JobStatus}, 0, 0, 0.0, 0.0)
    assert m.success_rate is None and m.avg_duration_seconds is None

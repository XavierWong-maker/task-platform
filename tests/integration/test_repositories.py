from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, time, timedelta
from typing import Any

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job, JobExecution, RetryPolicy
from task_platform.domain.schedule import Schedule
from task_platform.domain.state_machine import transition
from task_platform.infrastructure.db.repositories import (
    EntityNotFoundError,
    ExecutionRepository,
    JobRepository,
    ScheduleRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_db_engine("sqlite://")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _job(name: str = "job-1", **overrides: Any) -> Job:
    return Job(name=name, job_type=JobType.CUSTOM, handler="demo.ok", **overrides)


def _execution(
    job_id: str, run_id: str = "run_1", attempt: int = 1, **overrides: Any
) -> JobExecution:
    return JobExecution(job_id=job_id, run_id=run_id, attempt=attempt, **overrides)


# ---------------- JobRepository ----------------


def test_job_roundtrip(factory: sessionmaker[Session]) -> None:
    job = _job(
        timeout_seconds=12.5,
        retry_policy=RetryPolicy(
            max_attempts=5,
            backoff=BackoffStrategy.EXPONENTIAL,
            base_delay_seconds=0.5,
            max_delay_seconds=30.0,
        ),
    )
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) == job


def test_get_missing_job_returns_none(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        assert JobRepository(s).get("nope") is None


def test_get_job_by_name(factory: sessionmaker[Session]) -> None:
    job = _job("by-name")
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    with session_scope(factory) as s:
        assert JobRepository(s).get_by_name("by-name") == job
        assert JobRepository(s).get_by_name("other") is None


def test_add_duplicate_job_name_raises_at_call_site(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        JobRepository(s).add(_job("same"))

    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            JobRepository(s).add(_job("same"))


def test_update_job(factory: sessionmaker[Session]) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    job.enabled = False
    job.timeout_seconds = 99.0
    job.retry_policy = RetryPolicy(max_attempts=1)
    with session_scope(factory) as s:
        JobRepository(s).update(job)

    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) == job


def test_update_missing_job_raises(factory: sessionmaker[Session]) -> None:
    with pytest.raises(EntityNotFoundError):
        with session_scope(factory) as s:
            JobRepository(s).update(_job())


def test_delete_job(factory: sessionmaker[Session]) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    with session_scope(factory) as s:
        assert JobRepository(s).delete(job.id) is True
    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) is None
        assert JobRepository(s).delete(job.id) is False


def test_list_all_jobs_ordered_by_created_at(factory: sessionmaker[Session]) -> None:
    base = datetime(2026, 9, 20, tzinfo=UTC)
    older = _job("older", created_at=base)
    newer = _job("newer", created_at=base + timedelta(hours=1))
    with session_scope(factory) as s:
        repo = JobRepository(s)
        repo.add(newer)
        repo.add(older)

    with session_scope(factory) as s:
        assert [j.name for j in JobRepository(s).list_all()] == ["older", "newer"]


def test_delete_job_cascades_to_schedules_and_executions(
    factory: sessionmaker[Session],
) -> None:
    job = _job()
    schedule = Schedule(job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=60)
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ScheduleRepository(s).add(schedule)
        ExecutionRepository(s).add(_execution(job.id, status=JobStatus.SUCCESS))

    with session_scope(factory) as s:
        JobRepository(s).delete(job.id)

    with session_scope(factory) as s:
        assert ScheduleRepository(s).list_by_job(job.id) == []
        assert ExecutionRepository(s).list_by_run("run_1") == []


# ---------------- ScheduleRepository ----------------


def test_schedule_roundtrip_for_both_types(factory: sessionmaker[Session]) -> None:
    job = _job()
    interval = Schedule(job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=30.0)
    daily = Schedule(
        job_id=job.id,
        schedule_type=ScheduleType.DAILY,
        time_of_day=time(1, 0),
        enabled=False,
    )
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = ScheduleRepository(s)
        repo.add(interval)
        repo.add(daily)

    with session_scope(factory) as s:
        loaded = {sch.id: sch for sch in ScheduleRepository(s).list_by_job(job.id)}
    assert loaded == {interval.id: interval, daily.id: daily}


def test_schedule_for_unknown_job_is_rejected(factory: sessionmaker[Session]) -> None:
    schedule = Schedule(
        job_id="no_such_job", schedule_type=ScheduleType.INTERVAL, interval_seconds=5
    )
    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            ScheduleRepository(s).add(schedule)


# ---------------- ExecutionRepository ----------------


def test_execution_roundtrip(factory: sessionmaker[Session]) -> None:
    job = _job()
    started = datetime(2026, 9, 20, 8, 0, tzinfo=UTC)
    execution = _execution(
        job.id,
        status=JobStatus.FAILED,
        started_at=started,
        finished_at=started + timedelta(seconds=2),
        error_message="boom",
        timeout_seconds=5.0,
        max_attempts=3,
    )
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ExecutionRepository(s).add(execution)

    with session_scope(factory) as s:
        loaded = ExecutionRepository(s).get_attempt("run_1", 1)
    assert loaded == execution
    assert loaded is not None
    assert loaded.duration_seconds == 2.0


def test_list_by_run_orders_by_attempt(factory: sessionmaker[Session]) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        repo.add(_execution(job.id, attempt=2, status=JobStatus.SUCCESS))
        repo.add(_execution(job.id, attempt=1, status=JobStatus.RETRYING))

    with session_scope(factory) as s:
        attempts = [e.attempt for e in ExecutionRepository(s).list_by_run("run_1")]
    assert attempts == [1, 2]


def test_update_execution_persists_state_transition(factory: sessionmaker[Session]) -> None:
    job = _job()
    execution = _execution(job.id, status=JobStatus.RUNNING, started_at=datetime.now(UTC))
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ExecutionRepository(s).add(execution)

    transition(execution, JobStatus.SUCCESS)
    execution.finished_at = datetime.now(UTC)
    with session_scope(factory) as s:
        ExecutionRepository(s).update(execution)

    with session_scope(factory) as s:
        assert ExecutionRepository(s).get_attempt("run_1", 1) == execution


def test_update_missing_execution_raises(factory: sessionmaker[Session]) -> None:
    with pytest.raises(EntityNotFoundError):
        with session_scope(factory) as s:
            ExecutionRepository(s).update(_execution("job_x"))


def test_add_duplicate_run_attempt_is_rejected(factory: sessionmaker[Session]) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ExecutionRepository(s).add(_execution(job.id))

    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            ExecutionRepository(s).add(_execution(job.id))

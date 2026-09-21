from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, func, inspect, select
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType
from task_platform.infrastructure.db.models import ExecutionRecord, JobRecord
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


def _job(job_id: str = "job_1", name: str = "job-1", **overrides: object) -> JobRecord:
    fields: dict[str, object] = {
        "id": job_id,
        "name": name,
        "job_type": JobType.CUSTOM,
        "handler": "demo.ok",
        "timeout_seconds": 30.0,
        "retry_max_attempts": 3,
        "retry_backoff": BackoffStrategy.FIXED,
        "retry_base_delay_seconds": 1.0,
        "retry_max_delay_seconds": 60.0,
        "enabled": True,
        "created_at": datetime.now(UTC),
    }
    fields.update(overrides)
    return JobRecord(**fields)


def _execution(run_id: str = "run_1", attempt: int = 1, job_id: str = "job_1") -> ExecutionRecord:
    return ExecutionRecord(run_id=run_id, job_id=job_id, attempt=attempt, status=JobStatus.RUNNING)


def test_tables_are_created(engine: Engine) -> None:
    assert set(inspect(engine).get_table_names()) == {"jobs", "schedules", "job_executions"}


def test_datetime_roundtrips_as_aware_utc(factory: sessionmaker[Session]) -> None:
    created = datetime(2026, 9, 20, 8, 30, tzinfo=UTC)
    with session_scope(factory) as s:
        s.add(_job(created_at=created))

    with session_scope(factory) as s:
        loaded = s.get(JobRecord, "job_1")
        assert loaded is not None
        assert loaded.created_at == created
        assert loaded.created_at.tzinfo is not None
        assert loaded.created_at.utcoffset() == timedelta(0)


def test_naive_datetime_is_rejected(factory: sessionmaker[Session]) -> None:
    with pytest.raises(StatementError):
        with session_scope(factory) as s:
            s.add(_job(created_at=datetime(2026, 9, 20, 8, 30)))


def test_duplicate_job_name_is_rejected(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        s.add(_job("job_1", "same-name"))
    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            s.add(_job("job_2", "same-name"))


def test_duplicate_run_attempt_is_rejected(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        s.add(_job())
        s.flush()
        s.add(_execution("run_1", 1))
    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            s.add(_execution("run_1", 1))


def test_same_run_different_attempt_is_allowed(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        s.add(_job())
        s.flush()
        s.add(_execution("run_1", 1))
        s.add(_execution("run_1", 2))

    with session_scope(factory) as s:
        count = s.scalar(select(func.count()).select_from(ExecutionRecord))
        assert count == 2


def test_foreign_key_is_enforced(factory: sessionmaker[Session]) -> None:
    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            s.add(_execution(job_id="no_such_job"))


def test_session_scope_rolls_back_on_error(factory: sessionmaker[Session]) -> None:
    with pytest.raises(RuntimeError):
        with session_scope(factory) as s:
            s.add(_job())
            s.flush()
            raise RuntimeError("boom")

    with session_scope(factory) as s:
        assert s.scalar(select(func.count()).select_from(JobRecord)) == 0

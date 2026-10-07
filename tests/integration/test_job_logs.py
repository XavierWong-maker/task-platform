from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution, JobLog
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    JobLogRepository,
    JobRepository,
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


def _seed(factory: sessionmaker[Session]) -> Job:
    job = Job(name="log-job", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        repo.add(JobExecution(job_id=job.id, run_id="run_1", attempt=1, status=JobStatus.RETRYING))
        repo.add(JobExecution(job_id=job.id, run_id="run_1", attempt=2, status=JobStatus.SUCCESS))
    return job


def _log(attempt: int, message: str) -> JobLog:
    return JobLog(run_id="run_1", attempt=attempt, level="INFO", message=message)


def test_logs_roundtrip_in_insert_order(factory: sessionmaker[Session]) -> None:
    _seed(factory)
    logs = [_log(1, "first"), _log(1, "second")]
    with session_scope(factory) as s:
        JobLogRepository(s).add_many(logs)

    with session_scope(factory) as s:
        page = JobLogRepository(s).list_by_run("run_1")

    assert page.total == 2
    assert page.items == logs


def test_list_by_run_filters_by_attempt(factory: sessionmaker[Session]) -> None:
    _seed(factory)
    with session_scope(factory) as s:
        JobLogRepository(s).add_many([_log(1, "a1"), _log(2, "a2"), _log(2, "a2b")])

    with session_scope(factory) as s:
        page = JobLogRepository(s).list_by_run("run_1", attempt=2)

    assert [log.message for log in page.items] == ["a2", "a2b"]


def test_list_by_run_paginates(factory: sessionmaker[Session]) -> None:
    _seed(factory)
    with session_scope(factory) as s:
        JobLogRepository(s).add_many([_log(1, f"m{i}") for i in range(5)])

    with session_scope(factory) as s:
        page = JobLogRepository(s).list_by_run("run_1", page=2, page_size=2)

    assert page.total == 5
    assert [log.message for log in page.items] == ["m2", "m3"]


def test_log_for_unknown_execution_is_rejected(factory: sessionmaker[Session]) -> None:
    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            JobLogRepository(s).add_many([JobLog("ghost", 1, "INFO", "x")])


def test_deleting_job_cascades_to_logs(factory: sessionmaker[Session]) -> None:
    job = _seed(factory)
    with session_scope(factory) as s:
        JobLogRepository(s).add_many([_log(1, "gone soon")])

    with session_scope(factory) as s:
        JobRepository(s).delete(job.id)

    with session_scope(factory) as s:
        assert JobLogRepository(s).list_by_run("run_1").total == 0

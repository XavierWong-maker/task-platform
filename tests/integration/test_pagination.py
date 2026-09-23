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


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_db_engine("sqlite://")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _job(name: str, **overrides: object) -> Job:
    return Job(name=name, job_type=JobType.CUSTOM, handler="demo.ok", **overrides)


# ---------------- 参数校验 ----------------


def test_page_below_one_raises(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        with pytest.raises(ValueError):
            JobRepository(s).list_paginated(page=0)


def test_page_size_below_one_raises(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        with pytest.raises(ValueError):
            JobRepository(s).list_paginated(page_size=0)


def test_total_pages_computed_by_ceiling_division() -> None:
    from task_platform.infrastructure.db.repositories import Page

    assert Page(items=[], total=5, page=1, page_size=2).total_pages == 3
    assert Page(items=[], total=4, page=1, page_size=2).total_pages == 2
    assert Page(items=[], total=0, page=1, page_size=2).total_pages == 0


# ---------------- JobRepository 分页 ----------------


def test_job_pagination_basic(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        repo = JobRepository(s)
        for i in range(5):
            repo.add(_job(f"job-{i}", created_at=datetime(2026, 1, 1 + i, tzinfo=UTC)))

    with session_scope(factory) as s:
        page1 = JobRepository(s).list_paginated(page=1, page_size=2)
        page2 = JobRepository(s).list_paginated(page=2, page_size=2)
        page3 = JobRepository(s).list_paginated(page=3, page_size=2)

    assert [j.name for j in page1.items] == ["job-0", "job-1"]
    assert [j.name for j in page2.items] == ["job-2", "job-3"]
    assert [j.name for j in page3.items] == ["job-4"]
    assert page1.total == 5
    assert page1.total_pages == 3


def test_job_pagination_filters_by_enabled(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        repo = JobRepository(s)
        repo.add(_job("on-1", enabled=True))
        repo.add(_job("on-2", enabled=True))
        repo.add(_job("off-1", enabled=False))

    with session_scope(factory) as s:
        enabled_page = JobRepository(s).list_paginated(enabled=True, page_size=10)
        disabled_page = JobRepository(s).list_paginated(enabled=False, page_size=10)

    assert {j.name for j in enabled_page.items} == {"on-1", "on-2"}
    assert enabled_page.total == 2
    assert {j.name for j in disabled_page.items} == {"off-1"}
    assert disabled_page.total == 1


def test_job_pagination_out_of_range_page_returns_empty(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        JobRepository(s).add(_job("only-one"))

    with session_scope(factory) as s:
        page = JobRepository(s).list_paginated(page=5, page_size=10)

    assert page.items == []
    assert page.total == 1


# ---------------- ExecutionRepository 分页/过滤 ----------------


def _execution(job_id: str, run_id: str, **overrides: object) -> JobExecution:
    return JobExecution(job_id=job_id, run_id=run_id, **overrides)


def test_execution_pagination_filters_by_job_id(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        JobRepository(s).add(_job("job-a"))
        JobRepository(s).add(_job("job-b"))

    job_a = job_b = None
    with session_scope(factory) as s:
        job_a = JobRepository(s).get_by_name("job-a")
        job_b = JobRepository(s).get_by_name("job-b")
        assert job_a is not None and job_b is not None
        repo = ExecutionRepository(s)
        repo.add(_execution(job_a.id, "run_a1", status=JobStatus.SUCCESS))
        repo.add(_execution(job_b.id, "run_b1", status=JobStatus.SUCCESS))

    with session_scope(factory) as s:
        page = ExecutionRepository(s).list_paginated(job_id=job_a.id, page_size=10)

    assert page.total == 1
    assert page.items[0].run_id == "run_a1"


def test_execution_pagination_filters_by_status(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        job = _job("job-x")
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        repo.add(_execution(job.id, "run_1", status=JobStatus.SUCCESS))
        repo.add(_execution(job.id, "run_2", status=JobStatus.FAILED))
        repo.add(_execution(job.id, "run_3", status=JobStatus.FAILED))

    with session_scope(factory) as s:
        failed_page = ExecutionRepository(s).list_paginated(status=JobStatus.FAILED, page_size=10)

    assert failed_page.total == 2
    assert {e.run_id for e in failed_page.items} == {"run_2", "run_3"}


def test_execution_pagination_filters_by_time_range(factory: sessionmaker[Session]) -> None:
    base = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    with session_scope(factory) as s:
        job = _job("job-y")
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        repo.add(_execution(job.id, "early", status=JobStatus.SUCCESS, started_at=base))
        repo.add(
            _execution(
                job.id, "middle", status=JobStatus.SUCCESS, started_at=base + timedelta(hours=2)
            )
        )
        repo.add(
            _execution(
                job.id, "late", status=JobStatus.SUCCESS, started_at=base + timedelta(hours=5)
            )
        )

    with session_scope(factory) as s:
        page = ExecutionRepository(s).list_paginated(
            started_after=base + timedelta(hours=1),
            started_before=base + timedelta(hours=4),
            page_size=10,
        )

    assert page.total == 1
    assert page.items[0].run_id == "middle"


def test_execution_pagination_orders_most_recent_first(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        job = _job("job-z")
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        repo.add(_execution(job.id, "run_1", status=JobStatus.SUCCESS))
        repo.add(_execution(job.id, "run_2", status=JobStatus.SUCCESS))
        repo.add(_execution(job.id, "run_3", status=JobStatus.SUCCESS))

    with session_scope(factory) as s:
        page = ExecutionRepository(s).list_paginated(job_id=job.id, page_size=10)

    # id 自增，插入顺序 run_1 -> run_2 -> run_3，DESC 排序应为 run_3, run_2, run_1
    assert [e.run_id for e in page.items] == ["run_3", "run_2", "run_1"]


def test_execution_pagination_combines_page_and_filters(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        job = _job("job-w")
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        for i in range(5):
            repo.add(_execution(job.id, f"run_{i}", status=JobStatus.SUCCESS))

    with session_scope(factory) as s:
        page1 = ExecutionRepository(s).list_paginated(job_id=job.id, page=1, page_size=2)
        page2 = ExecutionRepository(s).list_paginated(job_id=job.id, page=2, page_size=2)

    assert page1.total == 5
    assert [e.run_id for e in page1.items] == ["run_4", "run_3"]
    assert [e.run_id for e in page2.items] == ["run_2", "run_1"]

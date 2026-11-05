"""PostgreSQL 集成测试：需要 TASK_PLATFORM_TEST_PG_URL 指向专用测试库，否则整体 skip。

每个用例前会 DROP SCHEMA public CASCADE，所以库名必须包含 "test"（安全护栏）。
表结构通过 Alembic 迁移建立（与生产一致），不用 create_all。
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine, inspect, make_url, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from task_platform.alerts import evaluate_alerts
from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution, JobLog
from task_platform.executor import TIMEOUT_ERROR_PREFIX
from task_platform.infrastructure.db import models  # noqa: F401
from task_platform.infrastructure.db.base import Base
from task_platform.infrastructure.db.migrate import downgrade_db, upgrade_db
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    IdempotencyRepository,
    JobLogRepository,
    JobRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    make_session_factory,
    session_scope,
)
from task_platform.metrics import collect_metrics

PG_URL = os.environ.get("TASK_PLATFORM_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="TASK_PLATFORM_TEST_PG_URL not set")

APP_TABLES = {"jobs", "schedules", "job_executions", "job_logs", "idempotency_keys"}


@pytest.fixture
def pg_url() -> str:
    assert PG_URL is not None
    database = make_url(PG_URL).database or ""
    if "test" not in database:
        pytest.fail(f"refusing to wipe database {database!r}: its name must contain 'test'")
    return PG_URL


@pytest.fixture
def engine(pg_url: str) -> Iterator[Engine]:
    eng = create_db_engine(pg_url)
    with eng.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine, pg_url: str) -> sessionmaker[Session]:
    upgrade_db(pg_url)
    return make_session_factory(engine)


def _job(name: str = "pg-job") -> Job:
    return Job(name=name, job_type=JobType.CUSTOM, handler="demo.ok")


def _tables(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names())


def _race(n: int, fn: Callable[[int], Any]) -> list[Any]:
    """n 个线程在屏障处同时开跑 fn，返回各自结果"""
    barrier = threading.Barrier(n)
    results: list[Any] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        r = fn(i)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == n  # 线程里抛异常会让结果变少
    return results


# ---------------- 迁移 ----------------


def test_migrations_build_schema_matching_orm_models(engine: Engine, pg_url: str) -> None:
    upgrade_db(pg_url)
    assert _tables(engine) == APP_TABLES | {"alembic_version"}

    def include_object(
        _: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
    ) -> bool:
        return not (type_ == "table" and name == "alembic_version")

    with engine.connect() as conn:
        ctx = MigrationContext.configure(
            conn, opts={"compare_type": False, "include_object": include_object}
        )
        assert compare_metadata(ctx, Base.metadata) == []


def test_downgrade_to_base_drops_everything(engine: Engine, pg_url: str) -> None:
    upgrade_db(pg_url)
    downgrade_db(pg_url)
    assert _tables(engine) == {"alembic_version"}


# ---------------- 数据读写与约束 ----------------


def test_datetime_roundtrips_as_aware_utc(factory: sessionmaker[Session]) -> None:
    created = datetime(2026, 9, 20, 8, 30, tzinfo=timezone(timedelta(hours=8)))
    job = _job()
    job.created_at = created
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    with session_scope(factory) as s:
        loaded = JobRepository(s).get(job.id)
    assert loaded is not None
    assert loaded.created_at == created  # 同一时刻
    assert loaded.created_at.utcoffset() == timedelta(0)  # 读回来是 UTC


def test_constraints_are_enforced(factory: sessionmaker[Session]) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ExecutionRepository(s).add(JobExecution(job_id=job.id, run_id="run_1", attempt=1))

    with pytest.raises(IntegrityError), session_scope(factory) as s:  # (run_id, attempt) 唯一
        ExecutionRepository(s).add(JobExecution(job_id=job.id, run_id="run_1", attempt=1))
    with pytest.raises(IntegrityError), session_scope(factory) as s:  # 外键
        ExecutionRepository(s).add(JobExecution(job_id="job_missing", run_id="run_2", attempt=1))


def test_deleting_job_cascades_to_executions_logs_and_keys(
    factory: sessionmaker[Session],
) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ExecutionRepository(s).add(
            JobExecution(job_id=job.id, run_id="run_1", attempt=1, status=JobStatus.SUCCESS)
        )
        JobLogRepository(s).add_many([JobLog("run_1", 1, "INFO", "hello")])
        assert IdempotencyRepository(s).try_claim("k1", job.id, "run_1") is True

    with session_scope(factory) as s:
        assert JobRepository(s).delete(job.id) is True

    with session_scope(factory) as s:
        assert ExecutionRepository(s).list_by_run("run_1") == []
        assert JobLogRepository(s).list_by_run("run_1").total == 0
        assert IdempotencyRepository(s).get_run_id("k1") is None


# ---------------- 真并发 ----------------


def test_concurrent_idempotency_claims_have_exactly_one_winner(
    factory: sessionmaker[Session],
) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    def claim(i: int) -> bool:
        with session_scope(factory) as s:
            return IdempotencyRepository(s).try_claim("race", job.id, f"run_{i}")

    assert _race(8, claim).count(True) == 1


def test_concurrent_retry_claims_have_exactly_one_winner(
    factory: sessionmaker[Session],
) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ExecutionRepository(s).add(
            JobExecution(
                job_id=job.id,
                run_id="run_r",
                attempt=1,
                status=JobStatus.RETRYING,
                next_retry_at=datetime(2026, 11, 4, tzinfo=UTC),
            )
        )

    def claim(_: int) -> bool:
        with session_scope(factory) as s:
            return ExecutionRepository(s).claim_retry("run_r", 1)

    assert _race(6, claim).count(True) == 1


# ---------------- 指标与告警 SQL ----------------


def test_metrics_and_alert_queries_run_on_postgres(factory: sessionmaker[Session]) -> None:
    job = _job()
    started = datetime.now(UTC) - timedelta(minutes=5)
    statuses = [JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.FAILED, JobStatus.FAILED]
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        for i, status in enumerate(statuses):
            repo.add(
                JobExecution(
                    job_id=job.id,
                    run_id=f"run_{i}",
                    attempt=1,
                    status=status,
                    started_at=started,
                    finished_at=started + timedelta(seconds=2),
                    error_message=None
                    if status is JobStatus.SUCCESS
                    else f"{TIMEOUT_ERROR_PREFIX} 1s",
                )
            )

    with session_scope(factory) as s:
        snap = collect_metrics(s, window=timedelta(hours=1))
        alerts = evaluate_alerts(s)

    m = snap.get("pg-job")
    assert m is not None
    assert (m.success_count, m.failed_count, m.duration_count) == (1, 3, 4)
    assert {a.rule for a in alerts} == {"consecutive_failures", "timeouts"}


def test_claim_returns_true_then_false_and_stores_owner(factory: sessionmaker[Session]) -> None:
    job = _job()
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = IdempotencyRepository(s)
        assert repo.try_claim("k", job.id, "run_a") is True
        assert repo.try_claim("k", job.id, "run_b") is False
    with session_scope(factory) as s:
        assert IdempotencyRepository(s).get_run_id("k") == "run_a"

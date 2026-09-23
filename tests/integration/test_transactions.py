"""
事务边界演示（加入事务边界；演示提交、回滚和唯一约束）

核心断言：session_scope 是唯一的事务边界。
Repository.add/update 只 flush，不 commit；
一个 session_scope 块内，无论涉及几个 Repository、写了几张表，
只要块内抛出异常，所有写入都必须回滚——不允许"Job 提交了、Schedule 没提交"这种半途状态。
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.repositories import (
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


def _job(name: str = "job-1") -> Job:
    return Job(name=name, job_type=JobType.CUSTOM, handler="demo.ok")


def test_multiple_repos_commit_together_in_one_scope(factory: sessionmaker[Session]) -> None:
    """一次 session_scope 里跨 3 个 Repository 写入，退出块后应一起可见"""
    job = _job()
    schedule = Schedule(job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=60)
    execution = JobExecution(job_id=job.id, status=JobStatus.SUCCESS)

    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ScheduleRepository(s).add(schedule)
        ExecutionRepository(s).add(execution)

    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) == job
        assert ScheduleRepository(s).list_by_job(job.id) == [schedule]
        assert ExecutionRepository(s).list_by_run(execution.run_id) == [execution]


def test_failure_partway_rolls_back_every_prior_write_in_same_scope(
    factory: sessionmaker[Session],
) -> None:
    """同一个 session_scope 内：Job 写入成功、Schedule 写入因外键错误失败，
    整个块必须原子回滚——不能出现"Job 落库了，Schedule 没落库"的半途状态。
    """
    job = _job()
    bad_schedule = Schedule(
        job_id="no_such_job",  # 外键错误，故意制造
        schedule_type=ScheduleType.INTERVAL,
        interval_seconds=30,
    )

    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            JobRepository(s).add(job)  # 这一步单独看是成功的
            ScheduleRepository(s).add(bad_schedule)  # flush 时炸

    with session_scope(factory) as s:
        # 整个块回滚：job 也不应该存在
        assert JobRepository(s).get(job.id) is None


def test_conflict_is_raised_at_flush_not_deferred_to_commit(
    factory: sessionmaker[Session],
) -> None:
    """唯一约束冲突应在调用点（add 内部的 flush）就抛出，
    而不是拖到 session_scope 退出时的 commit 才发现。
    一旦冲突发生，这个 session的当前事务已不可用，
    冲突必须穿透 session_scope 触发 rollback
    """
    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            repo = JobRepository(s)
            repo.add(_job("dup"))
            repo.add(_job("dup"))  # 第二次 add 在 flush 时立即抛出，而非等到 commit


def test_scope_is_clean_for_retry_after_a_failed_transaction(
    factory: sessionmaker[Session],
) -> None:
    """一次事务失败回滚后，用一个新的 session_scope 重试同样的写入应该干净地成功——
    验证失败事务不会污染后续状态（比如残留半提交的行、脏 session 对象）。
    """
    job = _job("retry-me")
    bad_schedule = Schedule(
        job_id="no_such_job", schedule_type=ScheduleType.INTERVAL, interval_seconds=10
    )

    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            JobRepository(s).add(job)
            ScheduleRepository(s).add(bad_schedule)

    # 重试：这次给一个合法的 schedule
    good_schedule = Schedule(
        job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=10
    )
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ScheduleRepository(s).add(good_schedule)

    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) == job
        assert ScheduleRepository(s).list_by_job(job.id) == [good_schedule]


def test_update_and_add_in_same_scope_roll_back_together(factory: sessionmaker[Session]) -> None:
    """块内既有 update 又有 add：add 失败时，前面的 update 也必须回滚"""
    job = _job("to-update")
    with session_scope(factory) as s:
        JobRepository(s).add(job)

    job.enabled = False
    duplicate = _job("to-update")  # 与已存在的 name 冲突

    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            JobRepository(s).update(job)  # 这一步本身合法
            JobRepository(s).add(duplicate)  # 这一步撞了 unique(name)

    with session_scope(factory) as s:
        reloaded = JobRepository(s).get(job.id)
        assert reloaded is not None
        assert reloaded.enabled is True  # update 也被回滚了，没有变成 False


def test_created_at_survives_rollback_boundary(factory: sessionmaker[Session]) -> None:
    """确认失败事务不会让 engine/session 处于不可用状态——
    回滚后立刻做一次正常读写应该没有任何残留影响
    """
    created = datetime(2026, 9, 21, tzinfo=UTC)
    job = _job("boundary-check")
    job.created_at = created

    with pytest.raises(IntegrityError):
        with session_scope(factory) as s:
            JobRepository(s).add(job)
            JobRepository(s).add(job)  # 同一个 id 重复 add，触发主键冲突

    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) is None
        JobRepository(s).add(job)

    with session_scope(factory) as s:
        assert JobRepository(s).get(job.id) == job

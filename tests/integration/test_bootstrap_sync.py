from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.bootstrap import build_registry, load_active_schedules, sync_jobs_to_db
from task_platform.domain.enums import ScheduleType
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.repositories import JobRepository, ScheduleRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_db_engine("sqlite://")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def test_sync_inserts_jobs_not_yet_in_db(factory: sessionmaker[Session]) -> None:
    registry = build_registry()
    with session_scope(factory) as s:
        synced = sync_jobs_to_db(registry, s)

    assert "demo-success" in synced
    with session_scope(factory) as s:
        assert JobRepository(s).get_by_name("demo-success") == synced["demo-success"]


def test_sync_preserves_id_and_enabled_across_restarts(factory: sessionmaker[Session]) -> None:
    registry = build_registry()
    with session_scope(factory) as s:
        first = sync_jobs_to_db(registry, s)
    job_id = first["demo-success"].id

    # 模拟用户手工停用
    with session_scope(factory) as s:
        repo = JobRepository(s)
        target = repo.get_by_name("demo-success")
        assert target is not None
        target.enabled = False
        repo.update(target)

    # 模拟"重启"：新的 registry 实例重新 discover，再次 sync
    registry_after_restart = build_registry()
    with session_scope(factory) as s:
        second = sync_jobs_to_db(registry_after_restart, s)

    assert second["demo-success"].id == job_id
    assert second["demo-success"].enabled is False


def test_sync_updates_handler_metadata_from_code(factory: sessionmaker[Session]) -> None:
    registry = TaskRegistry()

    @job("meta.sync.test", timeout_seconds=10.0, max_attempts=2, registry=registry)
    def sample() -> str:
        return "ok"

    with session_scope(factory) as s:
        sync_jobs_to_db(registry, s)

    # 模拟数据库里是旧部署留下的过期定义
    with session_scope(factory) as s:
        repo = JobRepository(s)
        stale = repo.get_by_name("meta-sync-test")
        assert stale is not None
        stale.timeout_seconds = 5.0
        repo.update(stale)

    with session_scope(factory) as s:
        synced = sync_jobs_to_db(registry, s)

    assert synced["meta-sync-test"].timeout_seconds == 10.0  # 代码定义覆盖旧值


def test_load_active_schedules_filters_disabled_schedule(factory: sessionmaker[Session]) -> None:
    registry = build_registry()
    with session_scope(factory) as s:
        jobs = sync_jobs_to_db(registry, s)
        success_job = jobs["demo-success"]
        failure_job = jobs["demo-failure"]
        repo = ScheduleRepository(s)
        repo.add(
            Schedule(
                job_id=success_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=30
            )
        )
        repo.add(
            Schedule(
                job_id=failure_job.id,
                schedule_type=ScheduleType.INTERVAL,
                interval_seconds=30,
                enabled=False,
            )
        )

    with session_scope(factory) as s:
        pairs = load_active_schedules(s)

    assert {j.name for j, _ in pairs} == {"demo-success"}


def test_load_active_schedules_excludes_disabled_job(factory: sessionmaker[Session]) -> None:
    registry = build_registry()
    with session_scope(factory) as s:
        jobs = sync_jobs_to_db(registry, s)
        success_job = jobs["demo-success"]
        repo = JobRepository(s)
        success_job.enabled = False
        repo.update(success_job)
        ScheduleRepository(s).add(
            Schedule(
                job_id=success_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=30
            )
        )

    with session_scope(factory) as s:
        pairs = load_active_schedules(s)

    assert pairs == []

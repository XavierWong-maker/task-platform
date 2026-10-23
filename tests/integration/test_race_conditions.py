"""竞态复现与控制：先证明"没有幂等键会重复执行"，再证明"有幂等键只执行一次"。"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType, ScheduleType
from task_platform.domain.idempotency import scheduled_key
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.faults import FlakyState
from task_platform.infrastructure.db.repositories import IdempotencyRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job
from task_platform.recovery import recover_interrupted
from task_platform.registry import TaskRegistry
from task_platform.scheduler import ClaimFn, Scheduler


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = create_db_engine(f"sqlite:///{tmp_path / 'race.db'}")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _claim_fn(factory: sessionmaker[Session]) -> ClaimFn:
    def claim(j: Job, run_id: str, key: str) -> bool:
        with session_scope(factory) as s:
            return IdempotencyRepository(s).try_claim(key, j.id, run_id)

    return claim


def _setup(
    factory: sessionmaker[Session],
) -> tuple[TaskRegistry, Job, Schedule, list[str], datetime]:
    """一个会记录每次调用的任务 + 一个固定的"已到期槽位"（所有 Scheduler 共用同一个槽位）"""
    calls: list[str] = []
    registry = TaskRegistry()
    registry.register("count", lambda: calls.append("x"))
    j = Job(name="race-job", job_type=JobType.CUSTOM, handler="count")
    with session_scope(factory) as s:
        JobRepository(s).add(j)
    schedule = Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    slot = datetime.now(UTC) - timedelta(seconds=1)
    return registry, j, schedule, calls, slot


def _tick_concurrently(
    registry: TaskRegistry,
    j: Job,
    schedule: Schedule,
    slot: datetime,
    claim: ClaimFn | None,
    n: int,
) -> list[list[JobExecution]]:
    schedulers: list[Scheduler] = []
    for _ in range(n):
        sc = Scheduler(registry, claim=claim)
        sc.add(j, schedule)
        sc._heap[0].next_run_at = slot
        schedulers.append(sc)

    barrier = threading.Barrier(n)
    results: list[list[JobExecution]] = [[] for _ in range(n)]

    def worker(i: int) -> None:
        barrier.wait()  # 所有 Scheduler 同时开始 tick
        results[i] = schedulers[i].tick()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for sc in schedulers:
        sc.stop()
    return results


def test_without_idempotency_every_scheduler_runs_the_slot(
    factory: sessionmaker[Session],
) -> None:
    """问题复现：6 个 Scheduler 对同一槽位各执行一次 => 重复执行 6 次"""
    registry, j, schedule, calls, slot = _setup(factory)

    _tick_concurrently(registry, j, schedule, slot, claim=None, n=6)

    assert len(calls) == 6


def test_with_idempotency_exactly_one_scheduler_runs_the_slot(
    factory: sessionmaker[Session],
) -> None:
    """控制：同样的并发，认领失败的 Scheduler 全部跳过，任务只执行一次"""
    registry, j, schedule, calls, slot = _setup(factory)

    results = _tick_concurrently(registry, j, schedule, slot, _claim_fn(factory), n=6)

    assert len(calls) == 1
    assert sum(len(r) for r in results) == 1


def test_orphan_claim_blocks_slot_until_recovery_releases_it(
    factory: sessionmaker[Session],
) -> None:
    """崩溃窗口：认领成功但 execution 从未落库 => 槽位被卡住，recover 释放后才能再调度"""
    registry, j, schedule, calls, slot = _setup(factory)
    key = scheduled_key(j.id, schedule, slot)
    with session_scope(factory) as s:
        assert IdempotencyRepository(s).try_claim(key, j.id, "run_ghost") is True

    blocked = _tick_concurrently(registry, j, schedule, slot, _claim_fn(factory), n=1)
    assert blocked == [[]] and calls == []  # 被孤儿键挡住

    with session_scope(factory) as s:
        report = recover_interrupted(s)
    assert report.released_keys == [key]

    resumed = _tick_concurrently(registry, j, schedule, slot, _claim_fn(factory), n=1)
    assert len(resumed[0]) == 1 and len(calls) == 1


def test_injected_failure_retries_with_same_run_id_then_succeeds() -> None:
    """任务失败一次，自动重试，最终 SUCCESS，且重试沿用同一个 run_id"""
    registry = TaskRegistry()
    state = FlakyState()

    @job(
        "flaky",
        max_attempts=3,
        backoff=BackoffStrategy.FIXED,
        base_delay_seconds=0.01,
        registry=registry,
    )
    def flaky(ctx: JobContext) -> str:
        if state.should_fail(ctx.execution_id, fail_times=1):
            raise RuntimeError("injected failure")
        return "ok"

    j = Job(name="flaky-job", job_type=JobType.CUSTOM, handler="flaky")
    scheduler = Scheduler(registry)
    scheduler.add(
        j, Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    )
    past = datetime.now(UTC) - timedelta(seconds=1)
    scheduler._heap[0].next_run_at = past

    first = scheduler.tick()[0]
    assert first.status is JobStatus.RETRYING and first.attempt == 1

    retry_entry = next(e for e in scheduler._heap if e.run_id == first.run_id)
    retry_entry.next_run_at = past
    second = scheduler.tick()[0]

    assert second.status is JobStatus.SUCCESS
    assert second.run_id == first.run_id and second.attempt == 2
    scheduler.stop()

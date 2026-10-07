from __future__ import annotations

import logging
from datetime import UTC, datetime, time, timedelta

import pytest

from task_platform.domain.enums import JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job
from task_platform.domain.schedule import Schedule
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


def _make_registry_with_job() -> tuple[TaskRegistry, Job]:
    registry = TaskRegistry()
    registry.register("demo.ok", lambda: "ok")
    job = Job(name="demo-ok", job_type=JobType.CUSTOM, handler="demo.ok")
    return registry, job


def test_tick_does_nothing_before_due_time() -> None:
    registry, job = _make_registry_with_job()
    scheduler = Scheduler(registry)
    schedule = Schedule(job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    scheduler.add(job, schedule)

    executions = scheduler.tick()

    assert executions == []


def test_tick_triggers_due_job_and_reschedule() -> None:
    registry, job = _make_registry_with_job()
    scheduler = Scheduler(registry)
    schedule = Schedule(job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    scheduler.add(job, schedule)

    # 强制堆顶已到期；直接操作内部堆的 next_run_at
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    executions = scheduler.tick()

    assert len(executions) == 1
    assert executions[0].status is JobStatus.SUCCESS
    # 重新计算的下次触发时间应该在未来
    next_run = scheduler.peek_next_run_at()
    assert next_run is not None and next_run > datetime.now(UTC)


def test_tick_skips_disabled_schedule_but_still_reschedule() -> None:
    registry, job = _make_registry_with_job()
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=job.id,
        schedule_type=ScheduleType.INTERVAL,
        interval_seconds=3600,
        enabled=False,
    )
    scheduler.add(job, schedule)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    executions = scheduler.tick()

    assert executions == []
    # 即使跳过执行，也应该重新入堆，而不是丢失这个 schedule
    assert scheduler.peek_next_run_at() is not None


def test_same_next_run_time_does_not_crash_heap() -> None:
    """回归：两个任务都是每天 01:00，next_run_at 完全相同时，
    堆比较不能退化到比较 Schedule 对象（TypeError）
    """
    registry = TaskRegistry()
    registry.register("a", lambda: "ok")
    registry.register("b", lambda: "ok")
    scheduler = Scheduler(registry)
    for name, handler in (("job-a", "a"), ("job-b", "b")):
        j = Job(name=name, job_type=JobType.CUSTOM, handler=handler)
        scheduler.add(
            j, Schedule(job_id=j.id, schedule_type=ScheduleType.DAILY, time_of_day=time(1, 0))
        )

    for entry in scheduler._heap:
        entry.next_run_at = datetime.now(UTC) - timedelta(seconds=1)
    # 同一时刻的两条：先入堆者先触发，且两个都能被触发
    executions = scheduler.tick()

    assert len(executions) == 2
    assert all(e.status is JobStatus.SUCCESS for e in executions)


def test_unregistered_handler_does_not_crash_tick(caplog: pytest.LogCaptureFixture) -> None:
    """回归：数据库里有、代码里已删除 handler 的孤儿任务，
    只记日志并跳过，不能让整个 tick()（进而 serve）崩溃；排期仍保留
    """
    registry = TaskRegistry()
    registry.register("good", lambda: "ok")
    good = Job(name="good-job", job_type=JobType.CUSTOM, handler="good")
    orphan = Job(name="orphan", job_type=JobType.CUSTOM, handler="gone.handler")
    scheduler = Scheduler(registry)
    for j in (orphan, good):
        scheduler.add(
            j, Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
        )
    for entry in scheduler._heap:
        entry.next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    with caplog.at_level(logging.ERROR, logger="task_platform.scheduler"):
        executions = scheduler.tick()

    # 孤儿任务不产生 execution；正常任务不受影响
    assert [e.job_id for e in executions] == [good.id]
    assert "job could not be executed" in caplog.text
    # 两个任务的 regular 排期都还在堆里
    assert len(scheduler._heap) == 2
    assert scheduler.peek_next_run_at() is not None
    assert scheduler.peek_next_run_at() > datetime.now(UTC)

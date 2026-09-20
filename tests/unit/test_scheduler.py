from __future__ import annotations

from datetime import UTC, datetime, timedelta

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

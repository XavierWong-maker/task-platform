"""
Scheduler 重试逻辑测试

覆盖：
1. 失败任务被安排重试，沿用同一个 run_id，attempt 递增
2. 重试链条耗尽后最终状态为 FAILED，且 regular schedule 排期被恢复，
   而不是从堆里消失
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job
from task_platform.domain.schedule import Schedule
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


def test_failed_job_is_rescheduled_for_retry_with_same_run_id() -> None:
    registry = TaskRegistry()

    @job(
        "always.fail",
        max_attempts=3,
        backoff=BackoffStrategy.FIXED,
        base_delay_seconds=0.01,
        registry=registry,
    )
    def always_fail() -> None:
        raise RuntimeError("boom")

    demo_job = Job(name="always-fail", job_type=JobType.CUSTOM, handler="always.fail")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600
    )
    scheduler.add(demo_job, schedule)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    executions = scheduler.tick()

    assert len(executions) == 1
    first_execution = executions[0]
    assert first_execution.status is JobStatus.RETRYING
    assert first_execution.attempt == 1

    # 堆里应该有一条即将到期的重试条目，run_id 与第一次一致
    retry_entry = next(e for e in scheduler._heap if e.run_id == first_execution.run_id)
    assert retry_entry.attempt_number == 2

    # 等待重试延迟过去，再触发一次
    time.sleep(0.02)
    retry_entry.next_run_at = datetime.now(UTC) - timedelta(seconds=1)
    executions_2 = scheduler.tick()

    assert len(executions_2) == 1
    second_execution = executions_2[0]
    assert second_execution.run_id == first_execution.run_id
    assert second_execution.attempt == 2
    assert second_execution.status is JobStatus.RETRYING  # 还有第 3 次机会


def test_exhausted_retries_end_in_failed_and_reschedules_regular() -> None:
    """重试链条耗尽（should_retry=False）后，最终状态是 FAILED，
    且 regular schedule 的下一轮触发时间必须被重新计算并入堆——
    不能因为一次失败重试就让这个 job 从调度堆里永久消失
    """
    registry = TaskRegistry()

    @job("always.fail2", max_attempts=1, registry=registry)
    def always_fail2() -> None:
        raise RuntimeError("boom")

    demo_job = Job(name="always-fail2", job_type=JobType.CUSTOM, handler="always.fail2")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600
    )
    scheduler.add(demo_job, schedule)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    executions = scheduler.tick()

    assert len(executions) == 1
    assert executions[0].status is JobStatus.FAILED

    # 没有残留的重试条目
    assert all(e.run_id != executions[0].run_id for e in scheduler._heap)

    # regular schedule 必须被恢复排期，而不是从堆里消失
    assert len(scheduler._heap) == 1
    assert scheduler._heap[0].attempt_number == 1
    assert scheduler._heap[0].run_id is None
    assert scheduler.peek_next_run_at() is not None
    assert scheduler.peek_next_run_at() > datetime.now(UTC)


def test_disabled_schedule_is_skipped_but_still_reschedules() -> None:
    """确认 disabled 分支（tick() 里 schedule.enabled/job.enabled 为 False 的路径）
    在现阶段重试改造后依然正确：不执行、不产生 execution，但会重新排期
    """
    registry = TaskRegistry()
    registry.register("demo.ok", lambda: "ok")
    demo_job = Job(name="demo-ok", job_type=JobType.CUSTOM, handler="demo.ok")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id,
        schedule_type=ScheduleType.INTERVAL,
        interval_seconds=3600,
        enabled=False,
    )
    scheduler.add(demo_job, schedule)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    executions = scheduler.tick()

    assert executions == []
    assert scheduler.peek_next_run_at() is not None
    assert scheduler.peek_next_run_at() > datetime.now(UTC)

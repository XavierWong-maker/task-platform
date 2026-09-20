from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job
from task_platform.executor import execute_attempt
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry


def test_execute_attempt_success_path() -> None:
    registry = TaskRegistry()
    registry.register("fast.ok", lambda: "ok")
    demo_job = Job(name="fast-ok", job_type=JobType.CUSTOM, handler="fast.ok", timeout_seconds=1.0)

    with ThreadPoolExecutor(max_workers=2) as pool:
        result = execute_attempt(demo_job, registry, pool, attempt_number=1)

    assert result.execution.status is JobStatus.SUCCESS
    assert result.execution.error_message is None
    assert result.should_retry is False


def test_execute_attempt_marks_failed_on_timeout_when_no_retries_left() -> None:
    """max_attempts=1：第一次尝试即最后一次，超时后应直接进入 FAILED，
    而不是 RETRYING —— 这样断言才只测超时机制本身，不与重试逻辑混在一起
    """
    registry = TaskRegistry()

    @job("slow.task", timeout_seconds=0.1, max_attempts=1, registry=registry)
    def slow_task() -> str:
        time.sleep(1.0)
        return "too-slow"

    demo_job = Job(name="slow-task", job_type=JobType.CUSTOM, handler="slow.task")

    with ThreadPoolExecutor(max_workers=2) as pool:
        result = execute_attempt(demo_job, registry, pool, attempt_number=1)

    assert result.execution.status is JobStatus.FAILED
    assert result.should_retry is False
    assert result.execution.error_message is not None
    assert "超过" in result.execution.error_message


def test_execute_attempt_timeout_allows_retry_when_attempts_remain() -> None:
    """默认 max_attempts=3：第一次超时后应该进入 RETRYING，而不是 FAILED，
    验证超时和重试逻辑正确衔接
    """
    registry = TaskRegistry()

    @job("slow.task.retry", timeout_seconds=0.1, max_attempts=3, registry=registry)
    def slow_task_retry() -> str:
        time.sleep(1.0)
        return "too-slow"

    demo_job = Job(name="slow-task-retry", job_type=JobType.CUSTOM, handler="slow.task.retry")

    with ThreadPoolExecutor(max_workers=2) as pool:
        result = execute_attempt(demo_job, registry, pool, attempt_number=1)

    assert result.execution.status is JobStatus.RETRYING
    assert result.should_retry is True

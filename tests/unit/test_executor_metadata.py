"""
验证 executor 正确解析并记录 timeout/retry 元数据：
- handler（@job 元数据）优先于 job 对象默认值
- 手动 register()（无元数据）时回退到 job 对象默认值
"""

from __future__ import annotations

from task_platform.domain.enums import BackoffStrategy, JobType
from task_platform.domain.models import Job, RetryPolicy
from task_platform.executor import execute
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry


def test_handler_metadata_takes_precedence_over_job_defaults() -> None:
    registry = TaskRegistry()

    @job(
        "meta.custom",
        timeout_seconds=5.0,
        max_attempts=7,
        backoff=BackoffStrategy.EXPONENTIAL,
        registry=registry,
    )
    def sample() -> str:
        return "ok"

    demo_job = Job(name="demo-meta", job_type=JobType.DEMO_SUCCESS, handler="meta.custom")
    # Job 对象自身仍是默认值 30.0s / 3 次，验证 handler 的 5.0s / 7 次生效
    assert demo_job.timeout_seconds == 30.0
    assert demo_job.retry_policy.max_attempts == 3

    execution = execute(demo_job, registry)

    assert execution.timeout_seconds == 5.0
    assert execution.max_attempts == 7


def test_falls_back_to_job_defaults_when_handler_has_no_metadate() -> None:
    registry = TaskRegistry()
    registry.register("legacy.handler", lambda: "ok")  # 无元数据的情况

    demo_job = Job(
        name="demo-legacy",
        job_type=JobType.DEMO_SUCCESS,
        handler="legacy.handler",
        timeout_seconds=15.0,
        retry_policy=RetryPolicy(max_attempts=2),
    )

    execution = execute(demo_job, registry)

    assert execution.timeout_seconds == 15.0
    assert execution.max_attempts == 2

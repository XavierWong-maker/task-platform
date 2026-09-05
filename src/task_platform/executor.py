"""
同步任务执行器
当前阶段没有 Scheduler/Worker，run 是同步阻塞调用
职责边界：接收一个 Job，创建 JobExecution，通过状态机推进状态，
用 try/except/finally 保证无论成功还是失败都会落状态和日志

阶段 2 新增：解析本次执行实际生效的 timeout/retry 参数并记录到
JobExecution 上；真正的超时打断与重试循环留给阶段 3 的 Scheduler/Worker
"""

from __future__ import annotations

import logging

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import Job, JobExecution, RetryPolicy
from task_platform.domain.models import _now as _now  # 复用与 models 一致的时钟来源
from task_platform.domain.state_machine import transition
from task_platform.jobs.context import build_context, call_handler
from task_platform.registry import RegisteredHandler, TaskRegistry

logger = logging.getLogger("task_platform.executor")


def resolve_timeout_seconds(job: Job, handler: RegisteredHandler) -> float:
    """决定本次执行实际生效的超时时间

    优先级：handler（@job 装饰器携带的元数据）> job（Job 对象上的默认值）。
    "任务自身在代码里声明的超时"应当优先于 Job 定义上尚未被显式覆盖的默认值。
    """
    if handler.timeout_seconds is not None:
        return handler.timeout_seconds
    return job.timeout_seconds


def resolve_retry_policy(job: Job, handler: RegisteredHandler) -> RetryPolicy:
    """决定本次执行实际生效的重试策略，优先级同上"""
    if handler.retry_policy is not None:
        return handler.retry_policy
    return job.retry_policy


def execute(job: Job, registry: TaskRegistry) -> JobExecution:
    """同步执行一个 Job，返回记录了结果的 JobExecution
    状态流转：PENDING -> QUEUED -> RUNNING -> SUCCESS
                                          \\-> FAILED
    """
    execution = JobExecution(job_id=job.id)
    transition(execution, JobStatus.QUEUED)
    transition(execution, JobStatus.RUNNING)
    execution.started_at = _now()
    execution.attempt += 1

    try:
        handler = registry.get(job.handler)
        effective_timeout = resolve_timeout_seconds(job, handler)
        effective_retry_policy = resolve_retry_policy(job, handler)
        execution.timeout_seconds = effective_timeout
        execution.max_attempts = effective_retry_policy.max_attempts

        context = build_context(execution.run_id)
        logger.info(
            "job started",
            extra={
                "job_id": job.id,
                "run_id": execution.run_id,
                "timeout_seconds": effective_timeout,
                "max_attempts": effective_retry_policy.max_attempts,
            },
        )
        call_handler(handler.func, context)
    except Exception as exc:  # noqa: BLE001 - 任务处理函数的异常类型不可预知，需要全部捕获
        execution.error_message = str(exc)
        transition(execution, JobStatus.FAILED)
        logger.error(
            "job failed",
            extra={"job_id": job.id, "run_id": execution.run_id, "error": str(exc)},
        )
    else:
        transition(execution, JobStatus.SUCCESS)
        logger.info("job succeeded", extra={"job_id": job.id, "run_id": execution.run_id})

    finally:
        execution.finished_at = _now()

    return execution


def cancel_before_start(job: Job) -> JobExecution:
    """
    在任务尚未开始运行前取消它（演示状态机的 CANCELED 分支）
    现阶段的 CLI 是同步执行，没有真正的"排队等待"阶段可以打断，
    这里用于演示：一个还处于 PENDING 的 execution 可以被直接取消
    """
    execution = JobExecution(job_id=job.id)
    transition(execution, JobStatus.CANCELED)
    execution.finished_at = _now()
    logger.info("job canceled", extra={"job_id": job.id, "run_id": execution.run_id})
    return execution

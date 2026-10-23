"""
同步任务执行器（现阶段：支持超时打断 + 多次尝试的重试语义）

职责边界：接收一个 Job，创建/延续一个 JobExecution，通过状态机推进状态，
用 try/except/finally 保证无论成功还是失败都会落状态和日志

execute_attempt() 是当前唯一的执行入口：
- 每次调用代表"一次尝试"（attempt_number），提交到线程池并用 Future.result(timeout=...) 等待
- 超时：无法强制终止线程，只能放弃等待，标记本次尝试失败（error_message 说明是 timeout）
- 失败后是否重试，由调用方（Scheduler）根据返回的 AttemptResult.should_retry 决定；
  本函数只负责"判断是否还有重试机会"（RetryPolicy.allows_retry），
  不负责真正安排下一次重试的时间——那是 Scheduler 的职责（避免阻塞主循环）
- 重试语义：多次尝试共享同一个 run_id（同一次"运行"的不同 attempt），
  只有 attempt_number 递增；
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import Executor, Future, wait
from dataclasses import dataclass
from typing import Any

from task_platform.domain.cancel import CancelToken, JobCanceledError
from task_platform.domain.enums import JobStatus
from task_platform.domain.models import (
    Job,
    JobExecution,
    RetryPolicy,
    utc_now,  # 复用与 models 一致的时钟来源
)
from task_platform.domain.state_machine import transition
from task_platform.jobs.context import build_context, call_handler
from task_platform.registry import RegisteredHandler, TaskRegistry

logger = logging.getLogger("task_platform.executor")
_POLL_SECONDS = 0.1


def _wait_for(future: Future[Any], token: CancelToken, *, timeout: float, grace: float) -> None:
    """等 future 结束，三种结局：完成 / 超时 / 取消后宽限期用尽。
    用 wait() 而不是 future.result(timeout)：后者的超时异常与 handler 自己抛出的
    TimeoutError 是同一个类，无法区分。
    """
    deadline = time.monotonic() + timeout
    cancel_deadline: float | None = None
    while True:
        done, _ = wait([future], timeout=min(_POLL_SECONDS, max(deadline - time.monotonic(), 0)))
        if done:
            future.result()  # 重新抛出 handler 的异常（含 JobCanceledError）
            return
        now = time.monotonic()
        if now >= deadline:
            token.cancel()  # 让配合的 handler 也能停下来
            raise TimeoutError(f"job 执行超过 {timeout}s 仍未完成（线程仍可能在后台继续运行）")
        if token.is_cancelled:
            if cancel_deadline is None:
                cancel_deadline = now + grace
            elif now >= cancel_deadline:
                raise JobCanceledError("canceled: handler did not stop within grace period")


def resolve_timeout_seconds(job: Job, handler: RegisteredHandler) -> float:
    """决定本次执行实际生效的超时时间
    优先级：handler（@job 装饰器携带的元数据）> job（Job 对象上的默认值）
    """
    if handler.timeout_seconds is not None:
        return handler.timeout_seconds
    return job.timeout_seconds


def resolve_retry_policy(job: Job, handler: RegisteredHandler) -> RetryPolicy:
    """决定本次执行实际生效的重试策略，优先级同上"""
    if handler.retry_policy is not None:
        return handler.retry_policy
    return job.retry_policy


@dataclass(slots=True)
class AttemptResult:
    """一次执行尝试的结果，供 Scheduler 决定是否重试"""

    execution: JobExecution
    retry_policy: RetryPolicy
    should_retry: bool


def execute_attempt(
    job: Job,
    registry: TaskRegistry,
    pool: Executor,
    *,
    attempt_number: int,
    run_id: str | None = None,
    on_started: Callable[[JobExecution], None] | None = None,
    cancel_token: CancelToken | None = None,
    cancel_grace_seconds: float = 5.0,
) -> AttemptResult:
    """执行一次尝试

    Args:
        job: 任务定义
        registry: 用于解析 handler
        pool: 用于提交 handler 调用、施加 timeout 的线程池
        attempt_number: 这是第几次尝试（从 1 开始）
        run_id: 重试/恢复时传入已有 run_id 以延续同一次运行；首次尝试传 None
        on_started: 进入 RUNNING 后、调用 handler 前触发，用于调用方提前持久化
        cancel_token: 取消令牌；handler 通过 ctx.check_cancelled() 感知。
            为 None 时内部新建一个（此时没人能从外部取消）
        cancel_grace_seconds: 令牌被取消后，等待 handler 自行停止的宽限期

    Returns:
        AttemptResult：包含本次 execution、生效的 retry_policy，以及是否还应该重试
    """
    token = cancel_token or CancelToken()
    execution = (
        JobExecution(job_id=job.id, run_id=run_id) if run_id else JobExecution(job_id=job.id)
    )
    transition(execution, JobStatus.QUEUED)
    transition(execution, JobStatus.RUNNING)
    execution.started_at = utc_now()
    execution.attempt = attempt_number

    handler = registry.get(job.handler)
    effective_timeout = resolve_timeout_seconds(job, handler)
    effective_retry_policy = resolve_retry_policy(job, handler)
    execution.timeout_seconds = effective_timeout
    execution.max_attempts = effective_retry_policy.max_attempts

    if on_started is not None:
        on_started(execution)

    try:
        context = build_context(execution.run_id, cancel=token)
        logger.info(
            "job attempt started",
            extra={
                "job_id": job.id,
                "run_id": execution.run_id,
                "attempt": attempt_number,
                "timeout_seconds": effective_timeout,
            },
        )
        future = pool.submit(call_handler, handler.func, context)
        _wait_for(future, token, timeout=effective_timeout, grace=cancel_grace_seconds)
    except JobCanceledError as exc:
        execution.error_message = str(exc)
        transition(execution, JobStatus.CANCELED)
        execution.finished_at = utc_now()
        logger.warning(
            "job attempt canceled",
            extra={"job_id": job.id, "run_id": execution.run_id, "attempt": attempt_number},
        )
        return AttemptResult(execution, effective_retry_policy, should_retry=False)
    except Exception as exc:  # noqa: BLE001 - 任务处理函数的异常类型不可预知，需要全部捕获
        execution.error_message = str(exc)
        should_retry = effective_retry_policy.allows_retry(attempt_number)
        if should_retry:
            transition(execution, JobStatus.RETRYING)
            logger.warning(
                "job attempt failed, will retry",
                extra={
                    "job_id": job.id,
                    "run_id": execution.run_id,
                    "attempt": attempt_number,
                    "error": str(exc),
                },
            )
        else:
            transition(execution, JobStatus.FAILED)
            logger.error(
                "job attempt failed, no more retries",
                extra={
                    "job_id": job.id,
                    "run_id": execution.run_id,
                    "attempt": attempt_number,
                    "error": str(exc),
                },
            )
        execution.finished_at = utc_now()
        return AttemptResult(execution, effective_retry_policy, should_retry)
    else:
        transition(execution, JobStatus.SUCCESS)
        execution.finished_at = utc_now()
        logger.info(
            "job succeeded",
            extra={"job_id": job.id, "run_id": execution.run_id, "attempt": attempt_number},
        )
        return AttemptResult(execution, effective_retry_policy, should_retry=False)


def cancel_before_start(job: Job) -> JobExecution:
    """
    在任务尚未开始运行前取消它（演示状态机的 CANCELED 分支）
    现阶段的 CLI 是同步执行，没有真正的"排队等待"阶段可以打断，
    这里用于演示：一个还处于 PENDING 的 execution 可以被直接取消
    """
    execution = JobExecution(job_id=job.id)
    transition(execution, JobStatus.CANCELED)
    execution.finished_at = utc_now()
    logger.info("job canceled", extra={"job_id": job.id, "run_id": execution.run_id})
    return execution

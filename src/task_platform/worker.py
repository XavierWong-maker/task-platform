"""Worker：从队列消费消息并执行任务

处理一条消息的流程：
1. 按 job_id 读任务定义；任务已不存在 -> 丢弃并 ack
2. 重复消费检查：(run_id, attempt) 已处理过，或幂等键已被别的 run 认领 -> ack 并跳过
3. 开始心跳续约租约，execute_attempt 执行；进入 RUNNING 时先 upsert 落库
4. 失败且还有次数 -> 写入 next_retry_at（由 retry_sweeper 之后投递），状态停在 RETRYING
5. 事务内 upsert 最终 execution 并写入日志；租约结束后才 ack

可靠性：
- ack 在落库之后：落库失败则消息留在 pending（至少一次投递）
- 租约 = pending 的空闲时间：持有者每隔 heartbeat 续约；空闲超过 stale_after 的消息
  被其他 Worker 接管（run_once 里按 reclaim_interval 节流，每次只领一条）
- 接管到 RUNNING 状态的记录：说明上一个持有者中途崩溃，重新执行（至少一次语义）
- 已知限制：持有者因长时间停顿丢了租约却仍在运行（僵尸）时，两边可能重复执行，
  最后写入者胜出；任务自身应尽量幂等
handler 未注册（代码里已删除）：落一条 FAILED 记录并 ack，避免这条消息反复回来。
Worker 内一次处理一条消息；并行靠启动多个 Worker 进程。
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.cancel import ActiveRuns, CancelToken
from task_platform.domain.enums import JobStatus
from task_platform.domain.models import Job, JobExecution, utc_now
from task_platform.domain.state_machine import transition
from task_platform.executor import execute_attempt
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    IdempotencyRepository,
    JobLogRepository,
    JobRepository,
)
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.infrastructure.queue.base import Delivery, TaskMessage, TaskQueue
from task_platform.registry import JobNotRegisteredError, TaskRegistry

logger = logging.getLogger("task_platform.worker")

# 这些状态说明该 (run_id, attempt) 已经被处理过（或交给了人工恢复），重复投递的消息直接跳过
_HANDLED_STATUSES = frozenset(
    {
        JobStatus.SUCCESS,
        JobStatus.FAILED,
        JobStatus.CANCELED,
        JobStatus.RETRYING,
        JobStatus.RECOVERABLE,
    }
)


class Outcome(StrEnum):
    FINISHED = "FINISHED"  # 本次尝试已到终态（SUCCESS / FAILED / CANCELED）
    RETRY_SCHEDULED = "RETRY_SCHEDULED"  # 失败，已写入 next_retry_at
    DROPPED = "DROPPED"  # 任务已不存在，消息被丢弃
    UNRUNNABLE = "UNRUNNABLE"  # handler 未注册，记为 FAILED
    DUPLICATE = "DUPLICATE"  # 重复投递：已处理过，ack 后跳过


@dataclass(frozen=True, slots=True)
class ProcessResult:
    outcome: Outcome
    execution: JobExecution | None


class Worker:
    def __init__(
        self,
        queue: TaskQueue,
        registry: TaskRegistry,
        factory: sessionmaker[Session],
        collector: LogCollector,
        *,
        consumer: str,
        max_workers: int = 4,
        stale_after_seconds: float | None = None,
        reclaim_interval_seconds: float = 5.0,
        heartbeat_interval_seconds: float | None = None,
    ) -> None:
        """
        stale_after_seconds: 不为 None 时，本 Worker 会接管空闲超过该秒数的 pending 消息
        heartbeat_interval_seconds: 不为 None 时，处理消息期间按该间隔续约；
            它与 stale_after 无关（别的 Worker 会接管，所以每个 Worker 都应当心跳）
        """
        if stale_after_seconds is not None and stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be > 0")
        if heartbeat_interval_seconds is not None and heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be > 0")
        if (
            stale_after_seconds is not None
            and heartbeat_interval_seconds is not None
            and heartbeat_interval_seconds >= stale_after_seconds
        ):
            raise ValueError("heartbeat interval must be shorter than stale_after")

        self._queue = queue
        self._registry = registry
        self._factory = factory
        self._collector = collector
        self._consumer = consumer
        self._stale_after = stale_after_seconds
        self._reclaim_interval = reclaim_interval_seconds
        self._heartbeat_interval = heartbeat_interval_seconds
        self._next_reclaim_at = 0.0
        self.active_runs = ActiveRuns()  # 供 CancelWatcher 把数据库里的取消请求转成令牌
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="worker-exec")

    def run_once(self, *, block_seconds: float = 0.0) -> ProcessResult | None:
        """先尝试接管过期消息，再取新消息并处理；都没有返回 None"""
        delivery = self._next_delivery(block_seconds)
        if delivery is None:
            return None
        return self.process(delivery)

    def process(self, delivery: Delivery) -> ProcessResult:
        message = delivery.message
        with session_scope(self._factory) as s:
            job = JobRepository(s).get(message.job_id)
        if job is None:
            # 注意日志不带 run_id：LogCollector 会按 run_id 缓存，而这条路径不会落库取走
            logger.warning(
                "job no longer exists, dropping message",
                extra={"job_id": message.job_id, "message_id": delivery.message_id},
            )
            self._queue.ack(delivery.message_id)
            return ProcessResult(Outcome.DROPPED, None)

        duplicate = self._check_duplicate(message)
        if duplicate is not None:
            reason, existing = duplicate
            logger.info(
                "duplicate delivery, skipping",
                extra={
                    "job_id": job.id,
                    "message_id": delivery.message_id,
                    "reason": reason,
                    "delivery_count": delivery.delivery_count,
                },
            )
            self._queue.ack(delivery.message_id)
            return ProcessResult(Outcome.DUPLICATE, existing)

        with self._lease(delivery):
            result = self._execute_and_persist(job, message)
        self._queue.ack(delivery.message_id)
        return result

    def close(self) -> None:
        """等待线程池里已提交的任务收尾（与 Scheduler.stop 语义一致）"""
        self._pool.shutdown(wait=True)

    # ---------------- 内部 ----------------

    def _next_delivery(self, block_seconds: float) -> Delivery | None:
        stale_after = self._stale_after
        if stale_after is not None and time.monotonic() >= self._next_reclaim_at:
            claimed = self._queue.claim_stale(self._consumer, min_idle_seconds=stale_after, count=1)
            if claimed:
                logger.warning(
                    "took over stale message (previous consumer presumed dead)",
                    extra={
                        "message_id": claimed[0].message_id,
                        "delivery_count": claimed[0].delivery_count,
                    },
                )
                return claimed[0]
            self._next_reclaim_at = time.monotonic() + self._reclaim_interval
        return self._queue.consume(self._consumer, block_seconds=block_seconds)

    def _check_duplicate(self, message: TaskMessage) -> tuple[str, JobExecution | None] | None:
        with session_scope(self._factory) as s:
            existing = ExecutionRepository(s).get_attempt(message.run_id, message.attempt)
            owner = (
                IdempotencyRepository(s).get_run_id(message.idempotency_key)
                if message.idempotency_key is not None
                else None
            )
        if existing is not None and existing.status in _HANDLED_STATUSES:
            return f"attempt already {existing.status.value}", existing
        if owner is not None and owner != message.run_id:
            # Dispatcher 投递"成功但客户端超时"后补投，留下了 run_id 不同的旧消息
            return f"idempotency key owned by {owner}", None
        return None

    @contextmanager
    def _lease(self, delivery: Delivery) -> Iterator[None]:
        """处理期间按间隔续约；没有配置心跳时什么都不做"""
        interval = self._heartbeat_interval
        if interval is None:
            yield
            return

        stop = threading.Event()
        extra = {"message_id": delivery.message_id, "job_id": delivery.message.job_id}

        def beat() -> None:
            while not stop.wait(interval):
                # noinspection PyBroadException
                try:
                    if not self._queue.renew(delivery.message_id, consumer=self._consumer):
                        logger.warning("lease lost: message was acked or taken over", extra=extra)
                        return
                except Exception:
                    logger.exception("heartbeat failed", extra=extra)

        thread = threading.Thread(target=beat, name="worker-heartbeat", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=interval + 1.0)

    def _execute_and_persist(self, job: Job, message: TaskMessage) -> ProcessResult:
        token = CancelToken()
        self.active_runs.register(message.run_id, token)
        try:
            result = execute_attempt(
                job,
                self._registry,
                self._pool,
                attempt_number=message.attempt,
                run_id=message.run_id,
                on_started=self._persist_started,
                cancel_token=token,
            )
        except JobNotRegisteredError:
            failed = self._unrunnable(job, message)
            self._persist_final(failed)
            return ProcessResult(Outcome.UNRUNNABLE, failed)
        finally:
            self.active_runs.unregister(message.run_id)

        execution = result.execution
        if result.should_retry:
            delay = result.retry_policy.delay_for_attempt(message.attempt)
            execution.next_retry_at = utc_now() + timedelta(seconds=delay)
        self._persist_final(execution)
        outcome = Outcome.RETRY_SCHEDULED if result.should_retry else Outcome.FINISHED
        return ProcessResult(outcome, execution)

    def _persist_started(self, execution: JobExecution) -> None:
        # upsert：Dispatcher 可能已经落了 QUEUED 行，这里把它推进到 RUNNING
        with session_scope(self._factory) as s:
            ExecutionRepository(s).upsert(execution)

    def _persist_final(self, execution: JobExecution) -> None:
        logs = self._collector.drain(execution.run_id, attempt=execution.attempt)
        with session_scope(self._factory) as s:
            ExecutionRepository(s).upsert(execution)
            if logs:
                JobLogRepository(s).add_many(logs)

    def _unrunnable(self, job: Job, message: TaskMessage) -> JobExecution:
        execution = JobExecution(job_id=job.id, run_id=message.run_id, attempt=message.attempt)
        for status in (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.FAILED):
            transition(execution, status)
        now = utc_now()
        execution.started_at = now
        execution.finished_at = now
        execution.error_message = f"handler not registered: {job.handler}"
        logger.error(
            "handler not registered, marking attempt failed",
            extra={"job_id": job.id, "run_id": message.run_id, "attempt": message.attempt},
        )
        return execution

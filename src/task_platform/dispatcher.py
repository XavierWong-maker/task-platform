"""Dispatcher：只负责"什么时候该执行"，把到期的触发变成队列消息

与 Scheduler 的区别：Scheduler.tick() 同步执行任务；Dispatcher.tick() 只认领幂等键并投递消息，
执行由 Worker 完成。重试不在这里排期：失败的尝试把 next_retry_at 落库，
由 retry_sweeper 扫描并投递（排期持久化，进程重启不丢）。

tick 内的顺序：认领幂等键 -> 投递消息 -> 尽力落库 QUEUED 记录。
消息是"待办"的事实来源；QUEUED 行只是给 API/历史看的投影，落库失败只记日志。
投递失败（如 Redis 不可用）：释放幂等键，触发项以原槽位时刻放回堆里，下一轮 tick 重试。
Dispatcher 不依赖数据库：认领/释放/落库都通过注入的回调完成。
"""

from __future__ import annotations

import heapq
import itertools
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from task_platform.domain.enums import JobStatus
from task_platform.domain.idempotency import scheduled_key
from task_platform.domain.models import Job, JobExecution, new_run_id, utc_now
from task_platform.domain.schedule import Schedule, compute_next_run
from task_platform.domain.state_machine import transition
from task_platform.infrastructure.queue.base import TaskMessage, TaskQueue
from task_platform.scheduler import ClaimFn

logger = logging.getLogger("task_platform.dispatcher")
ReleaseFn = Callable[[str], None]
EnqueuedFn = Callable[[JobExecution], None]


@dataclass(order=True)
class _Entry:
    """按 next_run_at、再按入堆序号 seq 排序；同一时刻的两个任务不会去比较 Job/Schedule"""

    next_run_at: datetime = field(compare=True)
    seq: int = field(compare=True)
    schedule: Schedule = field(compare=False)
    job: Job = field(compare=False)


class Dispatcher:
    def __init__(
        self,
        queue: TaskQueue,
        *,
        claim: ClaimFn | None = None,
        release: ReleaseFn | None = None,
        on_enqueued: EnqueuedFn | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._queue = queue
        self._claim = claim
        self._release = release
        self._on_enqueued = on_enqueued
        self._clock = clock
        self._heap: list[_Entry] = []
        self._seq = itertools.count()

    def _push(self, next_run_at: datetime, schedule: Schedule, job: Job) -> None:
        heapq.heappush(self._heap, _Entry(next_run_at, next(self._seq), schedule, job))

    def add(self, job: Job, schedule: Schedule) -> None:
        self._push(compute_next_run(schedule, self._clock()), schedule, job)

    def peek_next_run_at(self) -> datetime | None:
        return self._heap[0].next_run_at if self._heap else None

    def tick(self) -> list[JobExecution]:
        """处理所有到期触发，返回本轮成功投递的 QUEUED execution"""
        now = self._clock()
        # 先把到期项全部弹出再处理：投递失败的项会以同一时刻放回堆里，不能在本轮再次被弹出
        due: list[_Entry] = []
        while self._heap and self._heap[0].next_run_at <= now:
            due.append(heapq.heappop(self._heap))

        executions: list[JobExecution] = []
        for entry in due:
            execution = self._dispatch(entry, now)
            if execution is not None:
                executions.append(execution)
        return executions

    def _reschedule(self, entry: _Entry, now: datetime) -> None:
        self._push(compute_next_run(entry.schedule, now), entry.schedule, entry.job)

    def _dispatch(self, entry: _Entry, now: datetime) -> JobExecution | None:
        job, schedule = entry.job, entry.schedule
        if not (schedule.enabled and job.enabled):
            logger.info(
                "skip disabled job/schedule",
                extra={"job_id": job.id, "schedule_id": schedule.id},
            )
            self._reschedule(entry, now)
            return None

        key = scheduled_key(job.id, schedule, entry.next_run_at)
        run_id = new_run_id()
        if self._claim is not None:
            # noinspection PyBroadException
            try:
                claimed = self._claim(job, run_id, key)
            except Exception:
                logger.exception(
                    "idempotency claim failed, skipping this trigger",
                    extra={"job_id": job.id, "idempotency_key": key},
                )
                self._reschedule(entry, now)
                return None
            if not claimed:
                logger.info(
                    "skip duplicate trigger (idempotency key already claimed)",
                    extra={"job_id": job.id, "idempotency_key": key},
                )
                self._reschedule(entry, now)
                return None

        message = TaskMessage(job_id=job.id, run_id=run_id, attempt=1, idempotency_key=key)
        # noinspection PyBroadException
        try:
            self._queue.publish(message)
        except Exception:
            logger.exception(
                "publish failed, will retry this trigger on next tick",
                extra={"job_id": job.id, "run_id": run_id, "idempotency_key": key},
            )
            self._release_quietly(key)
            self._push(entry.next_run_at, schedule, job)  # 保持原槽位，不丢触发
            return None

        execution = JobExecution(job_id=job.id, run_id=run_id)
        transition(execution, JobStatus.QUEUED)
        execution.attempt = 1
        if self._on_enqueued is not None:
            # noinspection PyBroadException
            try:
                self._on_enqueued(execution)
            except Exception:
                logger.exception(
                    "failed to persist QUEUED execution (message already published)",
                    extra={"job_id": job.id, "run_id": run_id},
                )
        logger.info(
            "job dispatched",
            extra={"job_id": job.id, "run_id": run_id, "idempotency_key": key},
        )
        self._reschedule(entry, now)
        return execution

    def _release_quietly(self, key: str) -> None:
        if self._release is None:
            return
        # noinspection PyBroadException
        try:
            self._release(key)
        except Exception:
            logger.exception("failed to release idempotency key", extra={"idempotency_key": key})

"""
Scheduler：调度器主循环
职责边界：
- 计算哪些任务"现在应该运行"
- 不负责具体执行逻辑（执行本身委托给 executor.execute_attempt）
- 用 heapq 维护一个"下一次触发时间"的最小堆

堆里有两类条目：
1. Regular 触发（attempt_number=1, run_id=None）：到期后执行，
   然后用 compute_next_run() 计算下一轮 schedule 触发时间，重新入堆
2. Retry 触发（attempt_number>1, run_id=上一次尝试的 run_id）：
   到期后用相同 run_id、递增的 attempt_number 再尝试一次；
   这类条目不参与"下一轮 schedule"的计算——重试耗尽后（should_retry=False）
   直接结束，不再产生新的堆条目

重试延迟通过 RetryPolicy.delay_for_attempt() 计算，作为一次性堆条目追加，
不在主循环里 sleep 等待，避免阻塞其他到期任务。
"""

from __future__ import annotations

import heapq
import logging
import signal
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import FrameType

from task_platform.domain.models import Job, JobExecution, _now
from task_platform.domain.schedule import Schedule, compute_next_run
from task_platform.executor import execute_attempt
from task_platform.registry import TaskRegistry

logger = logging.getLogger("task_platform.scheduler")


@dataclass(order=True)
class _HeapEntry:
    """堆中的一项：按 next_run_at 排序，其余字段不参与比较"""

    next_run_at: datetime = field(compare=True)
    schedule: Schedule = field(compare=True)
    job: Job = field(compare=True)
    attempt_number: int = field(default=1, compare=False)
    run_id: str | None = field(default=None, compare=False)


class Scheduler:
    """维护一组 (Job, Schedule)，按到期时间触发执行；失败任务按重试策略自动重新排期"""

    def __init__(self, registry: TaskRegistry, *, max_workers: int = 4) -> None:
        self._registry = registry
        self._heap: list[_HeapEntry] = []
        self._running = False
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="scheduler-worker"
        )

    def add(self, job: Job, schedule: Schedule) -> None:
        """注册一个（job, schedule）组合，计算首次触发时间并入堆"""
        next_run_at = compute_next_run(schedule, _now())
        heapq.heappush(self._heap, _HeapEntry(next_run_at, schedule, job))

    def _pop_due(self, now: datetime) -> list[_HeapEntry]:
        """弹出所有到期（next_run_at <= now）的条目"""
        due: list[_HeapEntry] = []
        while self._heap and self._heap[0].next_run_at <= now:
            due.append(heapq.heappop(self._heap))
        return due

    def tick(self) -> list[JobExecution]:
        """执行一轮检查：触发所有到期任务/重试，安排后续排期
        返回本轮实际触发的 JobExecution 列表（供测试/CLI 观察）

        方无论本次触发是 regular 还是 retry，只要这条链路"不再需要重试"
        （成功 / 彻底失败 / disabled 被跳过），就统一重新计算并排入下一轮 regular
        触发时间——保证一次失败重试不会让任务永久从调度堆里消失。
        只有"还需要重试"时才用一次性的重试条目占位，不重排 regular。
        """
        now = _now()
        due_entries = self._pop_due(now)
        executions: list[JobExecution] = []

        for entry in due_entries:
            if not (entry.schedule.enabled and entry.job.enabled):
                logger.info(
                    "skip disabled job/schedule",
                    extra={"job_id": entry.job.id, "schedule_id": entry.schedule.id},
                )
                next_run_at = compute_next_run(entry.schedule, now)
                heapq.heappush(self._heap, _HeapEntry(next_run_at, entry.schedule, entry.job))
                continue

            is_retry_entry = entry.attempt_number > 1
            logger.info(
                "job triggered",
                extra={
                    "job_id": entry.job.id,
                    "schedule_id": entry.schedule.id,
                    "attempt": entry.attempt_number,
                    "is_retry": is_retry_entry,
                },
            )
            result = execute_attempt(
                entry.job,
                self._registry,
                self._pool,
                attempt_number=entry.attempt_number,
                run_id=entry.run_id,
            )
            executions.append(result.execution)

            if result.should_retry:
                delay = result.retry_policy.delay_for_attempt(entry.attempt_number)
                retry_at = _now() + timedelta(seconds=delay)
                heapq.heappush(
                    self._heap,
                    _HeapEntry(
                        retry_at,
                        entry.schedule,
                        entry.job,
                        attempt_number=entry.attempt_number + 1,
                        run_id=result.execution.run_id,
                    ),
                )
                logger.info(
                    "job scheduled for retry",
                    extra={
                        "job_id": entry.job.id,
                        "run_id": result.execution.run_id,
                        "next_attempt": entry.attempt_number + 1,
                        "delay_seconds": delay,
                    },
                )
                continue  # 重试链条占位，暂不重排 regular

            # 不再需要重试（成功，或重试耗尽彻底失败）：恢复/推进 regular 排期
            next_run_at = compute_next_run(entry.schedule, now)
            heapq.heappush(self._heap, _HeapEntry(next_run_at, entry.schedule, entry.job))

        return executions

    def run_forever(self, poll_interval_seconds: float = 1.0) -> None:
        """阻塞主循环：每隔 poll_interval_seconds 调用一次 tick()

        优雅关闭：注册 SIGINT/SIGTERM handler，收到信号后立即停止接收新的tick，
        然后等待线程池中已提交的任务运行完毕再返回（而不是直接丢弃）。
        注意：仅在主线程调用有效（signal.signal 的限制）；
        Windows 下 SIGTERM 不会触发该 handler，只有 SIGINT（Ctrl+C）有效。
        """
        previous_sigint = signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        previous_sigterm: object = None
        if hasattr(signal, "SIGTERM"):
            previous_sigterm = signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

        self._running = True
        try:
            while self._running:
                self.tick()
                time_module.sleep(poll_interval_seconds)
        finally:
            logger.info("scheduler shutting down, waiting for in-flight jobs to finish")
            self._pool.shutdown(wait=True)
            logger.info("scheduler shutdown complete")
            signal.signal(signal.SIGINT, previous_sigint)
            if previous_sigterm is not None:
                signal.signal(signal.SIGTERM, previous_sigterm)  # type: ignore[arg-type]

    def _handle_shutdown_signal(self, signum: int, frame: FrameType | None) -> None:
        """信号回调：只负责置位 self._running = False，不在这里做耗时操作
        （信号处理函数应尽量简短，真正的清理工作放在 run_forever 的 finally 里）
        """
        logger.info("received shutdown signal", extra={"signum": signum})
        self._running = False

    def stop(self) -> None:
        """程序化停止（供测试/非信号场景调用），语义与信号触发一致：
        不再接收新 tick，且等待线程池中已提交的任务跑完
        """
        self._running = False
        self._pool.shutdown(wait=False)

    def peek_next_run_at(self) -> datetime | None:
        """查看堆顶（最近一次将要触发）的时间，主要供测试断言使用"""
        return self._heap[0].next_run_at if self._heap else None

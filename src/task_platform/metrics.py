"""指标采集：从数据库（执行记录）和队列算出一份快照。

不使用进程内计数器：API / dispatch / worker 是不同进程，只有数据库是跨进程一致的。
- 成功率 = SUCCESS / (SUCCESS + FAILED)；CANCELED / RETRYING 不计入，分母为 0 时为 None
- 耗时只统计 SUCCESS / FAILED 的尝试
- window 不为空时只统计 started_at 落在窗口内的记录（QUEUED 尚未开始，不在其中）
- 队列不可用时 queue_* 为 None，不让整份快照失败
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import utc_now
from task_platform.infrastructure.db.repositories import ExecutionRepository
from task_platform.infrastructure.queue.base import TaskQueue

logger = logging.getLogger("task_platform.metrics")


@dataclass(frozen=True, slots=True)
class JobMetrics:
    job: str
    status_counts: dict[str, int]  # 所有 JobStatus 都有键，没有则为 0
    retry_attempts: int
    duration_count: int
    duration_sum: float
    duration_max: float

    @property
    def success_count(self) -> int:
        return self.status_counts[JobStatus.SUCCESS.value]

    @property
    def failed_count(self) -> int:
        return self.status_counts[JobStatus.FAILED.value]

    @property
    def success_rate(self) -> float | None:
        finished = self.success_count + self.failed_count
        return self.success_count / finished if finished else None

    @property
    def failure_rate(self) -> float | None:
        finished = self.success_count + self.failed_count
        return self.failed_count / finished if finished else None

    @property
    def avg_duration_seconds(self) -> float | None:
        return self.duration_sum / self.duration_count if self.duration_count else None


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    generated_at: datetime
    window: timedelta | None
    jobs: list[JobMetrics]
    queue_depth: int | None  # 未消费消息数；未提供队列或队列不可用时为 None
    queue_pending: int | None  # 已消费未 ack 的消息数

    def get(self, job: str) -> JobMetrics | None:
        return next((j for j in self.jobs if j.job == job), None)


def read_queue_stats(queue: TaskQueue | None) -> tuple[int | None, int | None]:
    if queue is None:
        return None, None
    # noinspection PyBroadException
    try:
        return queue.depth(), queue.pending_count()
    except Exception:
        logger.exception("failed to read queue metrics")
        return None, None


def collect_metrics(
    session: Session,
    queue: TaskQueue | None = None,
    *,
    window: timedelta | None = None,
    now: datetime | None = None,
) -> MetricsSnapshot:
    if window is not None and window <= timedelta(0):
        raise ValueError("window must be > 0")
    generated_at = now if now is not None else utc_now()
    since = generated_at - window if window is not None else None
    repo = ExecutionRepository(session)

    statuses: defaultdict[str, dict[str, int]] = defaultdict(
        lambda: {s.value: 0 for s in JobStatus}
    )
    for job, status, count in repo.count_by_job_and_status(since=since):
        statuses[job][status.value] = count

    retries = repo.count_retry_attempts_by_job(since=since)

    duration_count: defaultdict[str, int] = defaultdict(int)
    duration_sum: defaultdict[str, float] = defaultdict(float)
    duration_max: dict[str, float] = {}
    for job, seconds in repo.list_durations(since=since):
        duration_count[job] += 1
        duration_sum[job] += seconds
        duration_max[job] = max(duration_max.get(job, seconds), seconds)

    names = sorted(set(statuses) | set(retries) | set(duration_count))
    jobs = [
        JobMetrics(
            job=name,
            status_counts=statuses[name],
            retry_attempts=retries.get(name, 0),
            duration_count=duration_count[name],
            duration_sum=duration_sum[name],
            duration_max=duration_max.get(name, 0.0),
        )
        for name in names
    ]
    depth, pending = read_queue_stats(queue)
    return MetricsSnapshot(generated_at, window, jobs, depth, pending)

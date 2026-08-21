"""
核心领域对象：Job、JobExecution、RetryPolicy。
目前纯内存 dataclass，不涉及数据库持久化（后续优化）
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType


def _now() -> datetime:
    return datetime.now(UTC)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """描述失败重试规则
    Attributes:
        max_attempts: 最大尝试次数（含第一次），必须 >= 1
        backoff: 退避策略
        base_delay_seconds: 基础延迟，FIXED 策略下每次重试都等待这么久
        EXPONENTIAL 策略下作为指数的基数（base * 2 ** (attempt - 1)）
        max_delay_seconds: 重试延迟上限，避免指数退避无限增长
    """

    max_attempts: int = 3
    backoff: BackoffStrategy = BackoffStrategy.FIXED
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be >= 0")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds 不能小于 base_delay_seconds")

    def delay_for_attempt(self, attempt: int) -> float:
        """返回 attempt 失败后、发起下一次重试前应等待的秒数
        attempt 从 1 开始计数，表示"刚刚完成的这一次尝试"
        """
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        if self.backoff is BackoffStrategy.FIXED:
            delay = self.base_delay_seconds
        else:
            delay = self.max_delay_seconds * (2 ** (attempt - 1))
        return min(delay, self.max_delay_seconds)

    def allows_retry(self, attempt: int) -> bool:
        """给定已经尝试的次数，是否还需要再重试一次"""
        return attempt < self.max_attempts


@dataclass(slots=True)
class Job:
    """描述一个可执行任务的定义（不是某一次运行）"""

    name: str
    job_type: JobType
    handler: str  # 可执行函数的注册名，由 TaskRegistry 解析
    id: str = field(default_factory=lambda: _new_id("job"))
    timeout_seconds: float = 30.0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    enabled: bool = True
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be >= 0")


@dataclass(slots=True)
class JobExecution:
    """记录一次任务运行实例"""

    job_id: str
    run_id: str = field(default_factory=lambda: _new_id("run"))
    status: JobStatus = JobStatus.PENDING
    attempt: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float:
        """执行耗时：未开始或未结束时返回 0"""
        if self.started_at is None or self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()

"""
领域枚举定义
使用 Enum 避免在代码中散落魔法字符串（如 "SUCCESS" / "success"）
"""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    """
    一次任务执行（JobExecution）的状态
    状态机（详见 state_machine.py）：

        PENDING -> QUEUED -> RUNNING -> SUCCESS
                               |
                               +-> RETRYING -> QUEUED
                               |
                               +-> FAILED
        PENDING/QUEUED/RUNNING -> CANCELED
    """

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    RETRYING = "RETRYING"
    FAILED = "FAILED"
    CANCELED = "CANCELED"

    def is_terminal(self) -> bool:
        """是否为终止状态"""
        return self in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELED)


class JobType(StrEnum):
    """任务类型，后续扩展"""

    DEMO_SUCCESS = "DEMO_SUCCESS"
    DEMO_FAILURE = "DEMO_FAILURE"
    DEMO_SLOW = "DEMO_SLOW"


class BackoffStrategy(StrEnum):
    """重试退避策略"""

    FIXED = "FIXED"
    EXPONENTIAL = "EXPONENTIAL"

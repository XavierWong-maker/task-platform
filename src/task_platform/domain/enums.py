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
    RECOVERABLE = "RECOVERABLE"  # 进程中断时仍为 RUNNING，等待人工/后续步骤处理

    def is_terminal(self) -> bool:
        """是否为终止状态"""
        return self in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELED)


class JobType(StrEnum):
    """任务类型，后续扩展"""

    DEMO_SUCCESS = "DEMO_SUCCESS"
    DEMO_FAILURE = "DEMO_FAILURE"
    DEMO_SLOW = "DEMO_SLOW"
    CUSTOM = "CUSTOM"  # discover_jobs 发现但无显式 Job 定义的任务


class BackoffStrategy(StrEnum):
    """重试退避策略"""

    FIXED = "FIXED"
    EXPONENTIAL = "EXPONENTIAL"


class ScheduleType(StrEnum):
    """调度规则类型"""

    INTERVAL = "INTERVAL"  # 固定间隔
    DAILY = "DAILY"  # 简单 cron: 每天固定时刻


class NodeStatus(StrEnum):
    """工作流节点的运行状态

    PENDING -> RUNNING -> SUCCESS / FAILED / CANCELED
    PENDING -> SKIPPED   （上游 FAILED / SKIPPED / CANCELED，本节点不会运行）
    PENDING -> CANCELED
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELED = "CANCELED"

    def is_terminal(self) -> bool:
        return self not in (NodeStatus.PENDING, NodeStatus.RUNNING)


class WorkflowStatus(StrEnum):
    """工作流整体状态，由各节点状态推导（见 workflow_state.derive_workflow_status）"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_FAILED = "PARTIAL_FAILED"  # 有成功也有失败/跳过
    FAILED = "FAILED"  # 没有任何节点成功
    CANCELED = "CANCELED"

    def is_terminal(self) -> bool:
        return self not in (WorkflowStatus.PENDING, WorkflowStatus.RUNNING)

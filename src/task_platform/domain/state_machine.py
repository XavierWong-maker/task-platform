"""JobExecution 状态机

只负责一件事：给定"当前状态"和"目标状态"，判断这次转换是否合法
不允许任意跳跃（例如 SUCCESS 不能直接回到 RUNNING）

    PENDING -> QUEUED -> RUNNING -> SUCCESS
                           |
                           +-> RETRYING -> QUEUED
                           |
                           +-> FAILED
    PENDING/QUEUED/RUNNING -> CANCELED
"""

from __future__ import annotations

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import JobExecution


class InvalidStateTransitionError(Exception):
    """当尝试进行不被允许的状态转换时抛出"""

    def __init__(self, current: JobStatus, target: JobStatus) -> None:
        super().__init__(f"不允许从 {current.value} 转换到 {target.value}")
        self.current = current
        self.target = target


# 合法转换表：key 为当前状态，value 为该状态允许转换到的目标状态集合
_ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.QUEUED, JobStatus.CANCELED}),
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCESS, JobStatus.RETRYING, JobStatus.FAILED, JobStatus.CANCELED}
    ),
    JobStatus.RETRYING: frozenset({JobStatus.QUEUED}),
    # 终止状态：不允许再转换到任何状态
    JobStatus.SUCCESS: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELED: frozenset(),
}


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    """判断 current -> target 是否为合法转换"""
    return target in _ALLOWED_TRANSITIONS[current]


def transition(execution: JobExecution, target: JobStatus) -> JobExecution:
    """将 execution 的状态推进到 target，非法转换则抛出异常
    注意这是修改 JobExecution.status 的唯一入口——业务代码不应直接赋值
    `execution.status = ...`，否则状态机的约束形同虚设。
    """
    current = execution.status
    if not can_transition(current, target):
        raise InvalidStateTransitionError(current, target)
    execution.status = target
    return execution

"""崩溃恢复：把中断的运行标为 RECOVERABLE，并释放孤儿幂等键。

处理三类残留：
1. RUNNING：进程在执行中退出 -> RECOVERABLE
2. 悬空的 RETRYING：重试排期只在内存堆里，进程退出后没人会再执行它 -> RECOVERABLE
3. 孤儿幂等键：认领了槽位但 execution 从未落库 -> 释放，槽位可再次调度

只应在"确认没有其他 serve 进程在运行"时调用（目前没有心跳/租约，无法自动判断）。
恢复后用 `resume <run_id>` 沿用同一个 run_id 继续（attempt + 1）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import utc_now
from task_platform.domain.state_machine import transition
from task_platform.infrastructure.db.repositories import ExecutionRepository, IdempotencyRepository

INTERRUPTED_MESSAGE = "interrupted: process exited while RUNNING"


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    recovered: list[str]  # RUNNING -> RECOVERABLE，"run_id#attempt"
    released_keys: list[str]
    recovered_retries: list[str] = field(default_factory=list)  # 悬空 RETRYING -> RECOVERABLE


def recover_interrupted(session: Session) -> RecoveryReport:
    executions = ExecutionRepository(session)

    recovered: list[str] = []
    for ex in executions.list_by_status(JobStatus.RUNNING):
        transition(ex, JobStatus.RECOVERABLE)
        ex.finished_at = utc_now()
        ex.error_message = ex.error_message or INTERRUPTED_MESSAGE
        executions.update(ex)
        recovered.append(f"{ex.run_id}#{ex.attempt}")

    recovered_retries: list[str] = []
    for ex in executions.list_dangling_retries():
        transition(ex, JobStatus.RECOVERABLE)  # finished_at / error_message 保留上次失败的原样
        executions.update(ex)
        recovered_retries.append(f"{ex.run_id}#{ex.attempt}")

    keys = IdempotencyRepository(session)
    released = keys.list_orphan_keys()
    for key in released:
        keys.release(key)

    return RecoveryReport(
        recovered=recovered, released_keys=released, recovered_retries=recovered_retries
    )

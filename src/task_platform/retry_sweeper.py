"""重试扫描：把"重试时刻已到"的持久化排期投递成队列消息

流程（每条到期的 RETRYING 尝试）：
1. claim_retry：原子地把 next_retry_at 清空，多个 sweeper 并发时只有一个赢
2. 向队列投递 attempt+1 的消息（沿用同一个 run_id，重试不再认领幂等键）
3. 投递失败：把 next_retry_at 写回去，下一轮再试

认领后、投递前进程崩溃：留下 next_retry_at 为空的悬空 RETRYING，由 recover + resume 处理。
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.models import utc_now
from task_platform.infrastructure.db.repositories import ExecutionRepository
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.queue.base import TaskMessage, TaskQueue

logger = logging.getLogger("task_platform.retry_sweeper")


def sweep_due_retries(
    factory: sessionmaker[Session],
    queue: TaskQueue,
    *,
    now: datetime | None = None,
    limit: int = 100,
) -> list[str]:
    """返回本次成功投递的重试，格式 "run_id#已失败的attempt" """
    current = now if now is not None else utc_now()
    with session_scope(factory) as s:
        due = ExecutionRepository(s).list_due_retries(current, limit=limit)

    published: list[str] = []
    for ex in due:
        with session_scope(factory) as s:
            claimed = ExecutionRepository(s).claim_retry(ex.run_id, ex.attempt)
        if not claimed:
            continue  # 被别的 sweeper 抢先了
        # noinspection PyBroadException
        try:
            queue.publish(TaskMessage(ex.job_id, ex.run_id, attempt=ex.attempt + 1))
        except Exception:
            logger.exception(
                "retry publish failed, restoring schedule",
                extra={"job_id": ex.job_id, "run_id": ex.run_id, "attempt": ex.attempt},
            )
            with session_scope(factory) as s:
                ExecutionRepository(s).set_next_retry_at(ex.run_id, ex.attempt, ex.next_retry_at)
            continue
        published.append(f"{ex.run_id}#{ex.attempt}")
        logger.info(
            "retry dispatched",
            extra={"job_id": ex.job_id, "run_id": ex.run_id, "next_attempt": ex.attempt + 1},
        )
    return published

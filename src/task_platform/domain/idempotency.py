"""幂等键：标识"同一份业务工作"，同一个键同一时间只允许被一次运行认领。

格式：<job_id>:<business_date>[:<partition>]
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from task_platform.domain.enums import ScheduleType
from task_platform.domain.schedule import Schedule

MAX_KEY_LENGTH = 255


def build_idempotency_key(job_id: str, business_date: date, partition: str | None = None) -> str:
    if not job_id:
        raise ValueError("job_id must not be empty")
    key = f"{job_id}:{business_date.isoformat()}"
    if partition is not None:
        if not partition.strip():
            raise ValueError("partition must not be blank")
        key = f"{key}:{partition}"
    if len(key) > MAX_KEY_LENGTH:
        raise ValueError(f"idempotency key longer than {MAX_KEY_LENGTH}")
    return key


def scheduled_key(job_id: str, schedule: Schedule, schedule_at: datetime) -> str:
    """调度触发的幂等键：由"计划触发时刻"推导，多个 Scheduler 实例对同一槽位得到同一个键"""
    if schedule_at.tzinfo is None:
        raise ValueError("scheduled_at must be timezone-aware")
    at = schedule_at.astimezone(UTC)
    if schedule.schedule_type is ScheduleType.DAILY:
        return build_idempotency_key(job_id, at.date(), schedule.id)
    return build_idempotency_key(job_id, at.date(), f"{schedule.id}@{at.strftime('%H%M%S.%f')}")

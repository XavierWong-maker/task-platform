"""
Schedule：描述一个 Job 何时该被触发
阶段 3 只支持两种最简单的规则：
- INTERVAL：每隔固定秒数触发一次
- DAILY：每天某个固定时刻触发一次（对应蓝图里的"简单 Cron"）

compute_next_run() 是这个模块的核心：给定当前时间，算出"下一次应该运行的时间点"，
Scheduler 主循环只需要不断问这个问题，不需要自己理解 interval/cron 的细节。
"""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta

from task_platform.domain.enums import ScheduleType


def _new_id() -> str:
    return f"sch_{uuid.uuid4().hex[:12]}"


@dataclass(slots=True)
class Schedule:
    """一条调度规则，绑定到某个job_id"""

    job_id: str
    schedule_type: ScheduleType
    id: str = field(default_factory=_new_id)
    # INTERVAL 专用
    interval_seconds: float | None = None
    # DAILY 专用
    time_of_day: time | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        if self.schedule_type is ScheduleType.INTERVAL:
            if self.interval_seconds is None or self.interval_seconds <= 0:
                raise ValueError("INTERVAL 类型必须提供 > 0 的 interval_seconds")
        elif self.schedule_type is ScheduleType.DAILY:
            if self.time_of_day is None:
                raise ValueError("DAILY 类型必须提供 time_of_day")


def compute_next_run(schedule: Schedule, after: datetime) -> datetime:
    """计算 schedule 在 after 时刻之后的下一次触发时间（严格大于 after）

    Args:
        schedule: 调度规则
        after: 基准时间，通常是"当前时间"或"上一次触发时间"
    """
    if schedule.schedule_type is ScheduleType.INTERVAL:
        assert schedule.interval_seconds is not None
        return after + timedelta(seconds=schedule.interval_seconds)

    if schedule.schedule_type is ScheduleType.DAILY:
        assert schedule.time_of_day is not None
        candidate = datetime.combine(after.date(), schedule.time_of_day, tzinfo=UTC)
        if candidate <= after:
            candidate += timedelta(days=1)
        return candidate

    raise ValueError(f"未知的 schedule_type: {schedule.schedule_type}")

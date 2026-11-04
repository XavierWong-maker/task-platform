"""基础告警：对数据库与队列做一次评估，返回当前正在触发的告警。

规则（阈值见 AlertRules，环境变量 TASK_PLATFORM_ALERT_* 覆盖默认值）：
- consecutive_failures：某任务最近的"最终结果"（SUCCESS / FAILED）连续 N 次为 FAILED
- timeouts：窗口内超时的尝试数 >= N（按 executor 的超时错误消息前缀识别）
- queue_backlog：队列未消费消息数 >= N

已停用的任务不告警；队列读不到时不产生告警（read_queue_stats 会记日志）。
AlertTracker 用于常驻进程：只在"新触发 / 已恢复"时返回，避免每轮重复刷日志。
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy.orm import Session

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import utc_now
from task_platform.executor import TIMEOUT_ERROR_PREFIX
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobRepository
from task_platform.infrastructure.queue.base import TaskQueue
from task_platform.metrics import read_queue_stats

logger = logging.getLogger("task_platform.alerts")

ENV_PREFIX = "TASK_PLATFORM_ALERT_"
RULE_CONSECUTIVE_FAILURES = "consecutive_failures"
RULE_TIMEOUTS = "timeouts"
RULE_QUEUE_BACKLOG = "queue_backlog"
_FAILURE_LOOKBACK = 50  # 连续失败最多回看多少条最终结果


class Severity(StrEnum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class AlertRules:
    consecutive_failures: int = 3
    timeout_count: int = 3
    timeout_window: timedelta = timedelta(hours=1)
    queue_depth: int = 100

    def __post_init__(self) -> None:
        if self.consecutive_failures < 1:
            raise ValueError("consecutive_failures must be >= 1")
        if self.timeout_count < 1:
            raise ValueError("timeout_count must be >= 1")
        if self.timeout_window <= timedelta(0):
            raise ValueError("timeout_window must be > 0")
        if self.queue_depth < 1:
            raise ValueError("queue_depth must be >= 1")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AlertRules:
        """配置优先级：环境变量 > 默认值"""
        source = os.environ if env is None else env
        d = cls()
        window = source.get(f"{ENV_PREFIX}TIMEOUT_WINDOW_SECONDS", d.timeout_window.total_seconds())
        return cls(
            consecutive_failures=int(
                source.get(f"{ENV_PREFIX}CONSECUTIVE_FAILURES", d.consecutive_failures)
            ),
            timeout_count=int(source.get(f"{ENV_PREFIX}TIMEOUT_COUNT", d.timeout_count)),
            timeout_window=timedelta(seconds=float(window)),
            queue_depth=int(source.get(f"{ENV_PREFIX}QUEUE_DEPTH", d.queue_depth)),
        )


@dataclass(frozen=True, slots=True)
class Alert:
    rule: str
    subject: str  # 任务名或队列名
    severity: Severity
    message: str
    value: float
    threshold: float

    @property
    def key(self) -> str:
        return f"{self.rule}:{self.subject}"


def evaluate_alerts(
    session: Session,
    queue: TaskQueue | None = None,
    *,
    rules: AlertRules | None = None,
    queue_name: str = "queue",
    now: datetime | None = None,
) -> list[Alert]:
    active_rules = rules if rules is not None else AlertRules()
    current = now if now is not None else utc_now()
    executions = ExecutionRepository(session)
    jobs = [j for j in JobRepository(session).list_all() if j.enabled]
    alerts: list[Alert] = []

    lookback = max(_FAILURE_LOOKBACK, active_rules.consecutive_failures)
    for job in jobs:
        streak = 0
        for status in executions.recent_final_statuses(job.id, limit=lookback):
            if status is not JobStatus.FAILED:
                break
            streak += 1
        if streak >= active_rules.consecutive_failures:
            alerts.append(
                Alert(
                    rule=RULE_CONSECUTIVE_FAILURES,
                    subject=job.name,
                    severity=Severity.CRITICAL,
                    message=f"任务 {job.name} 已连续失败 {streak} 次",
                    value=streak,
                    threshold=active_rules.consecutive_failures,
                )
            )

    enabled_names = {j.name for j in jobs}
    window_seconds = int(active_rules.timeout_window.total_seconds())
    timeouts = executions.count_timeouts_by_job(
        TIMEOUT_ERROR_PREFIX, since=current - active_rules.timeout_window
    )
    for name in sorted(timeouts):
        if name in enabled_names and timeouts[name] >= active_rules.timeout_count:
            alerts.append(
                Alert(
                    rule=RULE_TIMEOUTS,
                    subject=name,
                    severity=Severity.WARNING,
                    message=f"任务 {name} 在最近 {window_seconds} 秒内超时 {timeouts[name]} 次",
                    value=timeouts[name],
                    threshold=active_rules.timeout_count,
                )
            )

    if queue is not None:
        depth, _pending = read_queue_stats(queue)
        if depth is not None and depth >= active_rules.queue_depth:
            alerts.append(
                Alert(
                    rule=RULE_QUEUE_BACKLOG,
                    subject=queue_name,
                    severity=Severity.WARNING,
                    message=f"队列 {queue_name} 积压 {depth} 条消息未消费",
                    value=depth,
                    threshold=active_rules.queue_depth,
                )
            )
    return alerts


class AlertTracker:
    """记住上一轮正在触发的告警，只报告变化：(新触发, 已恢复)"""

    def __init__(self) -> None:
        self._active: dict[str, Alert] = {}

    def update(self, alerts: Sequence[Alert]) -> tuple[list[Alert], list[Alert]]:
        current = {a.key: a for a in alerts}
        fired = [a for key, a in current.items() if key not in self._active]
        resolved = [a for key, a in self._active.items() if key not in current]
        self._active = current
        return fired, resolved


def log_alert(alert: Alert, *, resolved: bool = False) -> None:
    """告警写入结构化日志：extra 字段会展开为 JSON 顶层键，便于日志系统按 alert_rule 过滤"""
    extra = {
        "alert_rule": alert.rule,
        "alert_subject": alert.subject,
        "alert_value": alert.value,
        "alert_threshold": alert.threshold,
    }
    if resolved:
        logger.info(f"alert resolved: {alert.message}", extra=extra)
        return
    level = logging.ERROR if alert.severity is Severity.CRITICAL else logging.WARNING
    logger.log(level, f"alert firing: {alert.message}", extra={**extra, "severity": alert.severity})

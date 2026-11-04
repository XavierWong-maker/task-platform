"""alerts 子命令：评估一次告警规则，或用 --watch 常驻轮询。"""

from __future__ import annotations

import sys

from sqlalchemy.orm import Session, sessionmaker

from task_platform.alerts import Alert, AlertRules, AlertTracker, evaluate_alerts, log_alert
from task_platform.cli.queue_commands import _open_queue
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.queue.factory import get_queue_stream
from task_platform.runtime import run_loop, stop_on_signals


def run_alerts(factory: sessionmaker[Session], *, with_queue: bool, watch: float | None) -> int:
    """一次性模式：无告警返回 0，有告警返回 1；--watch 模式持续运行直到 Ctrl+C"""
    if watch is not None and watch <= 0:
        print("--watch 必须大于 0", file=sys.stderr)
        return 1
    try:
        rules = AlertRules.from_env()
    except ValueError as exc:
        print(f"告警阈值配置不合法：{exc}", file=sys.stderr)
        return 1

    queue = None
    if with_queue:
        queue = _open_queue()
        if queue is None:
            return 1
    queue_name = get_queue_stream()
    tracker = AlertTracker()

    def check() -> list[Alert]:
        with session_scope(factory) as s:
            alerts = evaluate_alerts(s, queue, rules=rules, queue_name=queue_name)
        fired, resolved = tracker.update(alerts)
        for _alert in fired:
            log_alert(_alert)
        for _alert in resolved:
            log_alert(_alert, resolved=True)
        return alerts

    if watch is None:
        alerts = check()
        if not alerts:
            print("无告警")
            return 0
        for alert in alerts:
            print(f"[{alert.severity.value}] {alert.rule} {alert.subject}: {alert.message}")
        return 1

    print(f"告警检查已启动，每 {watch} 秒一次。按 Ctrl+C 退出。")
    with stop_on_signals() as stop:
        run_loop(check, interval=watch, stop=stop)
    return 0

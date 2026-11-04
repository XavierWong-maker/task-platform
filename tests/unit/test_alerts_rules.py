from __future__ import annotations

from datetime import timedelta

import pytest

from task_platform.alerts import Alert, AlertRules, AlertTracker, Severity


def test_rule_defaults() -> None:
    rules = AlertRules()
    assert (rules.consecutive_failures, rules.timeout_count, rules.queue_depth) == (3, 3, 100)
    assert rules.timeout_window == timedelta(hours=1)


def test_rules_from_env_overrides_defaults() -> None:
    rules = AlertRules.from_env(
        {
            "TASK_PLATFORM_ALERT_CONSECUTIVE_FAILURES": "5",
            "TASK_PLATFORM_ALERT_TIMEOUT_COUNT": "2",
            "TASK_PLATFORM_ALERT_TIMEOUT_WINDOW_SECONDS": "600",
            "TASK_PLATFORM_ALERT_QUEUE_DEPTH": "10",
        }
    )
    assert (rules.consecutive_failures, rules.timeout_count, rules.queue_depth) == (5, 2, 10)
    assert rules.timeout_window == timedelta(seconds=600)
    assert AlertRules.from_env({}) == AlertRules()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"consecutive_failures": 0},
        {"timeout_count": 0},
        {"timeout_window": timedelta(0)},
        {"queue_depth": 0},
    ],
)
def test_invalid_rules_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        AlertRules(**kwargs)  # type: ignore[arg-type]


def test_invalid_env_value_raises() -> None:
    with pytest.raises(ValueError):
        AlertRules.from_env({"TASK_PLATFORM_ALERT_QUEUE_DEPTH": "many"})


def _alert(subject: str) -> Alert:
    return Alert("r", subject, Severity.WARNING, "m", 1, 1)


def test_tracker_reports_only_changes() -> None:
    tracker = AlertTracker()

    assert tracker.update([_alert("x")]) == ([_alert("x")], [])
    assert tracker.update([_alert("x")]) == ([], [])  # 持续触发：不重复报告
    assert tracker.update([_alert("x"), _alert("y")]) == ([_alert("y")], [])
    assert tracker.update([_alert("y")]) == ([], [_alert("x")])  # x 恢复
    assert tracker.update([]) == ([], [_alert("y")])
    assert tracker.update([_alert("x")]) == ([_alert("x")], [])  # 再次触发要重新报告

from datetime import UTC, datetime, time, timedelta

import pytest

from task_platform.domain.enums import ScheduleType
from task_platform.domain.schedule import Schedule, compute_next_run


def test_interval_next_run_adds_seconds() -> None:
    schedule = Schedule(job_id="job_1", schedule_type=ScheduleType.INTERVAL, interval_seconds=30)
    now = datetime(2026, 1, 1, tzinfo=UTC)

    next_run = compute_next_run(schedule, now)

    assert next_run == now + timedelta(seconds=30)


def test_daily_next_run_today_if_not_passed_yet() -> None:
    schedule = Schedule(job_id="job_1", schedule_type=ScheduleType.DAILY, time_of_day=time(1, 0))
    now = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)

    next_run = compute_next_run(schedule, now)

    assert next_run == datetime(2026, 1, 1, 1, 0, tzinfo=UTC)


def test_daily_next_run_rolls_to_tomorrow_if_passed() -> None:
    schedule = Schedule(job_id="job_1", schedule_type=ScheduleType.DAILY, time_of_day=time(1, 0))
    now = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)

    next_run = compute_next_run(schedule, now)

    assert next_run == datetime(2026, 1, 2, 1, 0, tzinfo=UTC)


def test_interval_requires_positive_seconds() -> None:
    with pytest.raises(ValueError):
        Schedule(job_id="job_1", schedule_type=ScheduleType.INTERVAL, interval_seconds=0)


def test_daily_requires_time_of_day() -> None:
    with pytest.raises(ValueError):
        Schedule(job_id="job_1", schedule_type=ScheduleType.DAILY)

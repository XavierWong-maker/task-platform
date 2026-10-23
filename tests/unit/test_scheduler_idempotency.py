from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from datetime import time as time_cls

import pytest

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType, ScheduleType
from task_platform.domain.idempotency import scheduled_key
from task_platform.domain.models import Job
from task_platform.domain.schedule import Schedule
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


class FakeClaims:
    """内存版认领表，模拟多个 Scheduler 实例共享的数据库"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.claimed: dict[str, str] = {}
        self.calls = 0

    def __call__(self, job: Job, run_id: str, key: str) -> bool:
        with self._lock:
            self.calls += 1
            if key in self.claimed:
                return False
            self.claimed[key] = run_id
            return True


def _setup() -> tuple[TaskRegistry, Job, Schedule]:
    registry = TaskRegistry()
    registry.register("ok", lambda: "ok")
    j = Job(name="ok-job", job_type=JobType.CUSTOM, handler="ok")
    s = Schedule(job_id=j.id, schedule_type=ScheduleType.DAILY, time_of_day=time_cls(1, 0))
    return registry, j, s


def test_scheduled_key_is_stable_per_slot_and_distinct_across_schedules() -> None:
    _, j, s = _setup()
    other = Schedule(job_id=j.id, schedule_type=ScheduleType.DAILY, time_of_day=time_cls(13, 0))
    slot = datetime(2026, 10, 17, 1, 0, tzinfo=UTC)

    assert scheduled_key(j.id, s, slot) == scheduled_key(j.id, s, slot)
    assert scheduled_key(j.id, s, slot) != scheduled_key(j.id, other, slot)
    assert scheduled_key(j.id, s, slot) != scheduled_key(j.id, s, slot + timedelta(days=1))


def test_scheduled_key_rejects_naive_datetime() -> None:
    _, j, s = _setup()
    with pytest.raises(ValueError):
        scheduled_key(j.id, s, datetime(2026, 10, 17, 1, 0))


def test_two_schedulers_same_slot_only_one_executes() -> None:
    registry, j, s = _setup()
    claims = FakeClaims()
    slot = datetime.now(UTC) - timedelta(seconds=1)

    results = []
    for _ in range(2):  # 模拟两个 Scheduler 实例同时到期同一槽位
        scheduler = Scheduler(registry, claim=claims)
        scheduler.add(j, s)
        scheduler._heap[0].next_run_at = slot
        results.append(scheduler.tick())
        # 被跳过的实例也必须重新排期，而不是丢失任务
        assert scheduler.peek_next_run_at() is not None
        assert scheduler.peek_next_run_at() > datetime.now(UTC)

    assert len(results[0]) == 1 and results[1] == []
    assert claims.claimed[scheduled_key(j.id, s, slot)] == results[0][0].run_id


def test_retry_attempts_do_not_claim_again() -> None:
    registry = TaskRegistry()

    @job(
        "fail.twice",
        max_attempts=2,
        backoff=BackoffStrategy.FIXED,
        base_delay_seconds=0.01,
        registry=registry,
    )
    def fail_twice() -> None:
        raise RuntimeError("boom")

    j = Job(name="fail-twice", job_type=JobType.CUSTOM, handler="fail.twice")
    s = Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    claims = FakeClaims()
    scheduler = Scheduler(registry, claim=claims)
    scheduler.add(j, s)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    first = scheduler.tick()[0]
    assert first.status is JobStatus.RETRYING

    time.sleep(0.02)
    retry_entry = next(e for e in scheduler._heap if e.run_id == first.run_id)
    retry_entry.next_run_at = datetime.now(UTC) - timedelta(seconds=1)
    second = scheduler.tick()[0]

    assert second.run_id == first.run_id and second.attempt == 2
    assert claims.calls == 1  # 重试沿用原 run，不重复认领


def test_claim_exception_skips_trigger_but_keeps_schedule() -> None:
    registry, j, s = _setup()

    def broken_claim(job: Job, run_id: str, key: str) -> bool:
        raise RuntimeError("db down")

    scheduler = Scheduler(registry, claim=broken_claim)
    scheduler.add(j, s)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    assert scheduler.tick() == []
    assert scheduler.peek_next_run_at() is not None
    assert scheduler.peek_next_run_at() > datetime.now(UTC)

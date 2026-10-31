from __future__ import annotations

from datetime import UTC, datetime, timedelta

from task_platform.dispatcher import Dispatcher
from task_platform.domain.enums import JobStatus, JobType, ScheduleType
from task_platform.domain.idempotency import scheduled_key
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue

T0 = datetime(2026, 10, 27, tzinfo=UTC)
SLOT = T0 + timedelta(seconds=60)  # interval=60 的首个槽位


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class FakeClaims:
    """内存版认领表，模拟多个 Dispatcher 共享的数据库"""

    def __init__(self) -> None:
        self.claimed: dict[str, str] = {}
        self.released: list[str] = []

    def __call__(self, job: Job, run_id: str, key: str) -> bool:
        if key in self.claimed:
            return False
        self.claimed[key] = run_id
        return True

    def release(self, key: str) -> None:
        self.released.append(key)
        self.claimed.pop(key, None)


class FlakyQueue(MemoryQueue):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next = True

    def publish(self, message: TaskMessage) -> str:
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("redis down")
        return super().publish(message)


def _job_and_schedule(name: str = "d-job", **schedule_overrides: object) -> tuple[Job, Schedule]:
    j = Job(name=name, job_type=JobType.CUSTOM, handler="h")
    s = Schedule(
        job_id=j.id,
        schedule_type=ScheduleType.INTERVAL,
        interval_seconds=60,
        **schedule_overrides,  # type: ignore[arg-type]
    )
    return j, s


def test_tick_before_due_time_publishes_nothing() -> None:
    clock, queue = Clock(T0), MemoryQueue()
    d = Dispatcher(queue, clock=clock)
    d.add(*_job_and_schedule())

    clock.now = T0 + timedelta(seconds=30)

    assert d.tick() == []
    assert queue.depth() == 0


def test_due_trigger_publishes_message_and_returns_queued_execution() -> None:
    clock, queue = Clock(T0), MemoryQueue()
    seen: list[JobExecution] = []
    d = Dispatcher(queue, on_enqueued=seen.append, clock=clock)
    j, s = _job_and_schedule()
    d.add(j, s)

    clock.now = T0 + timedelta(seconds=61)
    [execution] = d.tick()

    assert execution.status is JobStatus.QUEUED and execution.attempt == 1
    assert seen == [execution]
    delivery = queue.consume("w1")
    assert delivery is not None
    assert delivery.message == TaskMessage(
        j.id, execution.run_id, attempt=1, idempotency_key=scheduled_key(j.id, s, SLOT)
    )
    assert d.peek_next_run_at() == T0 + timedelta(seconds=121)  # 已重新排期


def test_disabled_schedule_is_skipped_but_rescheduled() -> None:
    clock, queue = Clock(T0), MemoryQueue()
    d = Dispatcher(queue, clock=clock)
    d.add(*_job_and_schedule(enabled=False))

    clock.now = T0 + timedelta(seconds=61)

    assert d.tick() == []
    assert queue.depth() == 0
    assert d.peek_next_run_at() == T0 + timedelta(seconds=121)


def test_two_jobs_with_identical_slot_both_dispatch() -> None:
    """回归：next_run_at 完全相同时，堆比较不能退化到比较 Job/Schedule"""
    clock, queue = Clock(T0), MemoryQueue()
    d = Dispatcher(queue, clock=clock)
    d.add(*_job_and_schedule("job-a"))
    d.add(*_job_and_schedule("job-b"))

    clock.now = T0 + timedelta(seconds=61)

    assert len(d.tick()) == 2
    assert queue.depth() == 2


def test_two_dispatchers_same_slot_only_one_publishes() -> None:
    queue, claims = MemoryQueue(), FakeClaims()
    j, s = _job_and_schedule()

    results = []
    for _ in range(2):  # 模拟两个 Dispatcher 实例同时到期同一槽位
        clock = Clock(T0)
        d = Dispatcher(queue, claim=claims, clock=clock)
        d.add(j, s)
        clock.now = T0 + timedelta(seconds=61)
        results.append(d.tick())
        assert d.peek_next_run_at() == T0 + timedelta(seconds=121)  # 被跳过的也要重新排期

    assert len(results[0]) == 1 and results[1] == []
    assert queue.depth() == 1
    assert claims.claimed[scheduled_key(j.id, s, SLOT)] == results[0][0].run_id


def test_claim_exception_skips_trigger_but_keeps_schedule() -> None:
    clock, queue = Clock(T0), MemoryQueue()

    def broken_claim(job: Job, run_id: str, key: str) -> bool:
        raise RuntimeError("db down")

    d = Dispatcher(queue, claim=broken_claim, clock=clock)
    d.add(*_job_and_schedule())
    clock.now = T0 + timedelta(seconds=61)

    assert d.tick() == []
    assert queue.depth() == 0
    assert d.peek_next_run_at() == T0 + timedelta(seconds=121)


def test_publish_failure_releases_key_and_retries_same_slot_next_tick() -> None:
    clock, queue, claims = Clock(T0), FlakyQueue(), FakeClaims()
    d = Dispatcher(queue, claim=claims, release=claims.release, clock=clock)
    j, s = _job_and_schedule()
    d.add(j, s)
    clock.now = T0 + timedelta(seconds=61)

    assert d.tick() == []  # 第一次投递失败
    key = scheduled_key(j.id, s, SLOT)
    assert claims.released == [key] and key not in claims.claimed
    assert d.peek_next_run_at() == SLOT  # 触发项仍停在原槽位
    assert queue.depth() == 0

    [execution] = d.tick()  # 队列恢复后，同一个槽位被补投
    assert queue.depth() == 1
    assert claims.claimed[key] == execution.run_id


def test_enqueued_callback_failure_does_not_lose_the_published_message() -> None:
    clock, queue = Clock(T0), MemoryQueue()

    def boom(execution: JobExecution) -> None:
        raise RuntimeError("db down")

    d = Dispatcher(queue, on_enqueued=boom, clock=clock)
    d.add(*_job_and_schedule())
    clock.now = T0 + timedelta(seconds=61)

    assert len(d.tick()) == 1
    assert queue.depth() == 1

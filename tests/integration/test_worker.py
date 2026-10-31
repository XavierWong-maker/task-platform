from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution, utc_now
from task_platform.faults import FlakyState
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    IdempotencyRepository,
    JobLogRepository,
    JobRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.logging.collector import LogCollector, collecting_logs
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry
from task_platform.retry_sweeper import sweep_due_retries
from task_platform.worker import Outcome, Worker


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class SpyQueue(MemoryQueue):
    def __init__(self) -> None:
        super().__init__()
        self.renews = 0

    def renew(self, message_id: str, *, consumer: str) -> bool:
        self.renews += 1
        return super().renew(message_id, consumer=consumer)


class Harness:
    def __init__(
        self,
        factory: sessionmaker[Session],
        collector: LogCollector,
        *,
        queue: MemoryQueue | None = None,
    ) -> None:
        self.factory = factory
        self.collector = collector
        self.clock = FakeClock()
        self.queue = queue if queue is not None else MemoryQueue(clock=self.clock)
        self.registry = TaskRegistry()
        self.calls: list[str] = []
        self._workers: list[Worker] = []
        flaky = FlakyState()
        calls = self.calls

        @job("ok", registry=self.registry)
        def ok() -> str:
            return "ok"

        @job("count", registry=self.registry)
        def count_job(ctx: JobContext) -> str:
            calls.append(ctx.execution_id)
            return "ok"

        @job("slow", registry=self.registry)
        def slow() -> str:
            time.sleep(0.4)
            return "slow ok"

        @job("boom", max_attempts=3, base_delay_seconds=30.0, registry=self.registry)
        def boom() -> None:
            raise RuntimeError("boom")

        @job("flaky", max_attempts=3, base_delay_seconds=0.01, registry=self.registry)
        def flaky_job(ctx: JobContext) -> str:
            if flaky.should_fail(ctx.execution_id, fail_times=1):
                raise RuntimeError("injected failure")
            return "ok"

        self.worker = self.make_worker("w1")

    def make_worker(self, consumer: str, **kwargs: Any) -> Worker:
        worker = Worker(
            self.queue,
            self.registry,
            self.factory,
            self.collector,
            consumer=consumer,
            max_workers=2,
            **kwargs,
        )
        self._workers.append(worker)
        return worker

    def close(self) -> None:
        for worker in self._workers:
            worker.close()

    def add_job(self, name: str, handler: str) -> Job:
        j = Job(name=name, job_type=JobType.CUSTOM, handler=handler)
        with session_scope(self.factory) as s:
            JobRepository(s).add(j)
        return j

    def send(self, j: Job, run_id: str = "run_1", attempt: int = 1, key: str | None = None) -> None:
        self.queue.publish(TaskMessage(j.id, run_id, attempt, idempotency_key=key))

    def attempts(self, run_id: str) -> list[JobExecution]:
        with session_scope(self.factory) as s:
            return ExecutionRepository(s).list_by_run(run_id)

    def logs(self, run_id: str) -> list[str]:
        with session_scope(self.factory) as s:
            return [log.message for log in JobLogRepository(s).list_by_run(run_id).items]


@pytest.fixture
def factory(tmp_path: Path) -> Iterator[sessionmaker[Session]]:
    engine = create_db_engine(f"sqlite:///{tmp_path / 'worker.db'}")
    init_db(engine)
    yield make_session_factory(engine)
    engine.dispose()


@pytest.fixture
def collector() -> Iterator[LogCollector]:
    with collecting_logs() as c:
        yield c


@pytest.fixture
def harness(factory: sessionmaker[Session], collector: LogCollector) -> Iterator[Harness]:
    h = Harness(factory, collector)
    yield h
    h.close()


# ---------------- 基本处理（步骤 4） ----------------


def test_run_once_on_empty_queue_returns_none(harness: Harness) -> None:
    assert harness.worker.run_once() is None


def test_success_persists_execution_and_logs_then_acks(harness: Harness) -> None:
    j = harness.add_job("ok-job", "ok")
    harness.send(j)

    result = harness.worker.run_once()

    assert result is not None and result.outcome is Outcome.FINISHED
    [ex] = harness.attempts("run_1")
    assert ex.status is JobStatus.SUCCESS
    assert ex.started_at is not None and ex.finished_at is not None
    assert any("job attempt started" in m for m in harness.logs("run_1"))
    assert (harness.queue.depth(), harness.queue.pending_count()) == (0, 0)


def test_worker_upgrades_existing_queued_row_instead_of_duplicating(harness: Harness) -> None:
    j = harness.add_job("ok-job", "ok")
    with session_scope(harness.factory) as s:  # 模拟 Dispatcher 落下的 QUEUED 投影
        ExecutionRepository(s).add(
            JobExecution(job_id=j.id, run_id="run_1", attempt=1, status=JobStatus.QUEUED)
        )
    harness.send(j)

    harness.worker.run_once()

    [ex] = harness.attempts("run_1")
    assert ex.status is JobStatus.SUCCESS


def test_failure_with_attempts_left_schedules_persistent_retry(harness: Harness) -> None:
    j = harness.add_job("boom-job", "boom")
    harness.send(j)

    before = utc_now()
    result = harness.worker.run_once()
    after = utc_now()

    assert result is not None and result.outcome is Outcome.RETRY_SCHEDULED
    [ex] = harness.attempts("run_1")
    assert ex.status is JobStatus.RETRYING
    assert ex.next_retry_at is not None
    assert before + timedelta(seconds=29) <= ex.next_retry_at <= after + timedelta(seconds=31)
    assert harness.queue.pending_count() == 0  # 已 ack，重试交给 sweeper


def test_failure_on_last_attempt_is_final(harness: Harness) -> None:
    j = harness.add_job("boom-job", "boom")
    harness.send(j, attempt=3)  # max_attempts=3

    result = harness.worker.run_once()

    assert result is not None and result.outcome is Outcome.FINISHED
    [ex] = harness.attempts("run_1")
    assert ex.status is JobStatus.FAILED and ex.next_retry_at is None


def test_retry_cycle_through_sweeper_ends_in_success_with_same_run_id(
    harness: Harness,
) -> None:
    j = harness.add_job("flaky-job", "flaky")
    harness.send(j)

    first = harness.worker.run_once()
    assert first is not None and first.outcome is Outcome.RETRY_SCHEDULED

    far_future = datetime.now(UTC) + timedelta(hours=1)
    assert sweep_due_retries(harness.factory, harness.queue, now=far_future) == ["run_1#1"]

    second = harness.worker.run_once()
    assert second is not None and second.outcome is Outcome.FINISHED
    assert [(e.attempt, e.status) for e in harness.attempts("run_1")] == [
        (1, JobStatus.RETRYING),
        (2, JobStatus.SUCCESS),
    ]


def test_message_for_deleted_job_is_dropped_and_acked(harness: Harness) -> None:
    harness.queue.publish(TaskMessage("job_missing", "run_x"))

    result = harness.worker.run_once()

    assert result is not None and result.outcome is Outcome.DROPPED
    assert harness.attempts("run_x") == []
    assert (harness.queue.depth(), harness.queue.pending_count()) == (0, 0)


def test_unregistered_handler_is_recorded_as_failed_and_acked(harness: Harness) -> None:
    j = harness.add_job("gone-job", "gone.handler")
    harness.send(j)

    result = harness.worker.run_once()

    assert result is not None and result.outcome is Outcome.UNRUNNABLE
    [ex] = harness.attempts("run_1")
    assert ex.status is JobStatus.FAILED
    assert "handler not registered" in (ex.error_message or "")
    assert any("handler not registered" in m for m in harness.logs("run_1"))
    assert harness.queue.pending_count() == 0


def test_persist_failure_leaves_message_pending_for_redelivery(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    j = harness.add_job("ok-job", "ok")
    harness.send(j)

    def broken_persist(execution: JobExecution) -> None:
        raise RuntimeError("db down")

    monkeypatch.setattr(harness.worker, "_persist_final", broken_persist)

    with pytest.raises(RuntimeError):
        harness.worker.run_once()

    assert harness.queue.pending_count() == 1  # 没有 ack：至少一次投递


# ---------------- 重复消费保护（步骤 5） ----------------


def test_redelivery_of_finished_attempt_is_skipped_and_not_reexecuted(harness: Harness) -> None:
    j = harness.add_job("count-job", "count")
    harness.send(j)
    first = harness.worker.run_once()
    assert first is not None and first.outcome is Outcome.FINISHED
    assert harness.calls == ["run_1"]

    harness.send(j)  # 模拟"落库后、ack 前崩溃"造成的重复投递
    second = harness.worker.run_once()

    assert second is not None and second.outcome is Outcome.DUPLICATE
    assert harness.calls == ["run_1"]  # 没有再执行一遍
    assert harness.queue.pending_count() == 0  # 重复消息也被 ack 掉了


def test_redelivery_of_retrying_attempt_is_skipped(harness: Harness) -> None:
    j = harness.add_job("boom-job", "boom")
    harness.send(j)
    harness.worker.run_once()
    [before] = harness.attempts("run_1")

    harness.send(j)  # 同一个 attempt 再次投递
    result = harness.worker.run_once()

    assert result is not None and result.outcome is Outcome.DUPLICATE
    [after] = harness.attempts("run_1")
    assert after.next_retry_at == before.next_retry_at
    assert harness.queue.pending_count() == 0


def test_message_with_stale_idempotency_key_owner_is_skipped(harness: Harness) -> None:
    j = harness.add_job("count-job", "count")
    with session_scope(harness.factory) as s:
        assert IdempotencyRepository(s).try_claim("k1", j.id, "run_new") is True

    harness.send(j, run_id="run_old", key="k1")  # 旧消息：key 已属于 run_new
    stale = harness.worker.run_once()
    assert stale is not None and stale.outcome is Outcome.DUPLICATE
    assert harness.calls == []

    harness.send(j, run_id="run_new", key="k1")  # 真正的持有者照常执行
    owner = harness.worker.run_once()
    assert owner is not None and owner.outcome is Outcome.FINISHED
    assert harness.calls == ["run_new"]


# ---------------- 崩溃接管与心跳（步骤 5） ----------------


def test_crashed_workers_message_is_taken_over_after_lease_expires(harness: Harness) -> None:
    j = harness.add_job("ok-job", "ok")
    harness.send(j)
    dead = harness.queue.consume("dead-worker")  # 取走后"崩溃"：既没执行完也没 ack
    assert dead is not None
    with session_scope(harness.factory) as s:  # 崩溃前已经落了 RUNNING
        ExecutionRepository(s).add(
            JobExecution(
                job_id=j.id,
                run_id="run_1",
                attempt=1,
                status=JobStatus.RUNNING,
                started_at=utc_now(),
            )
        )
    rescuer = harness.make_worker("w2", stale_after_seconds=30.0, reclaim_interval_seconds=0.0)

    harness.clock.now = 10.0
    assert rescuer.run_once() is None  # 租约未过期，不能抢

    harness.clock.now = 31.0
    result = rescuer.run_once()

    assert result is not None and result.outcome is Outcome.FINISHED
    [ex] = harness.attempts("run_1")
    assert ex.status is JobStatus.SUCCESS
    assert harness.queue.pending_count() == 0


def test_heartbeat_renews_lease_while_job_is_running(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    spy = SpyQueue()
    h = Harness(factory, collector, queue=spy)
    try:
        j = h.add_job("slow-job", "slow")
        worker = h.make_worker("w1", heartbeat_interval_seconds=0.05)
        h.send(j)

        result = worker.run_once()

        assert result is not None and result.outcome is Outcome.FINISHED
        assert spy.renews >= 2  # 任务跑了约 0.4s，间隔 0.05s
    finally:
        h.close()


def test_worker_rejects_heartbeat_not_shorter_than_stale_after(harness: Harness) -> None:
    with pytest.raises(ValueError):
        harness.make_worker("w3", stale_after_seconds=10.0, heartbeat_interval_seconds=10.0)

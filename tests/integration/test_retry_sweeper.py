from __future__ import annotations

import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.queue.base import TaskMessage
from task_platform.infrastructure.queue.memory import MemoryQueue
from task_platform.recovery import recover_interrupted
from task_platform.retry_sweeper import sweep_due_retries

NOW = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)
DUE_AT = NOW - timedelta(seconds=1)
FUTURE_AT = NOW + timedelta(seconds=60)


class DownQueue(MemoryQueue):
    def __init__(self) -> None:
        super().__init__()
        self.down = True

    def publish(self, message: TaskMessage) -> str:
        if self.down:
            raise ConnectionError("redis down")
        return super().publish(message)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = create_db_engine(f"sqlite:///{tmp_path / 'sweeper.db'}")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _seed(factory: sessionmaker[Session]) -> Job:
    job = Job(name="sweep-job", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        for run_id, status, retry_at in (
            ("run_due", JobStatus.RETRYING, DUE_AT),
            ("run_future", JobStatus.RETRYING, FUTURE_AT),
            ("run_plain", JobStatus.RETRYING, None),  # 旧路径留下的、没有持久化排期的
            ("run_done", JobStatus.SUCCESS, None),
        ):
            repo.add(
                JobExecution(
                    job_id=job.id, run_id=run_id, attempt=1, status=status, next_retry_at=retry_at
                )
            )
    return job


def _retry_at(factory: sessionmaker[Session], run_id: str) -> datetime | None:
    with session_scope(factory) as s:
        ex = ExecutionRepository(s).get_attempt(run_id, 1)
    assert ex is not None
    return ex.next_retry_at


def test_next_retry_at_roundtrips(factory: sessionmaker[Session]) -> None:
    _seed(factory)
    assert _retry_at(factory, "run_due") == DUE_AT
    assert _retry_at(factory, "run_plain") is None


def test_sweep_publishes_only_due_retries_and_claims_them(
    factory: sessionmaker[Session],
) -> None:
    job = _seed(factory)
    queue = MemoryQueue()

    assert sweep_due_retries(factory, queue, now=NOW) == ["run_due#1"]

    assert queue.depth() == 1
    delivery = queue.consume("w1")
    assert delivery is not None
    assert delivery.message == TaskMessage(job.id, "run_due", attempt=2)
    assert _retry_at(factory, "run_due") is None  # 已认领
    assert _retry_at(factory, "run_future") == FUTURE_AT  # 未到期的不动


def test_second_sweep_is_a_noop(factory: sessionmaker[Session]) -> None:
    _seed(factory)
    queue = MemoryQueue()
    sweep_due_retries(factory, queue, now=NOW)

    assert sweep_due_retries(factory, queue, now=NOW) == []
    assert queue.depth() == 1


def test_publish_failure_restores_schedule_and_next_sweep_retries(
    factory: sessionmaker[Session],
) -> None:
    _seed(factory)
    queue = DownQueue()

    assert sweep_due_retries(factory, queue, now=NOW) == []
    assert _retry_at(factory, "run_due") == DUE_AT  # 排期已写回

    queue.down = False
    assert sweep_due_retries(factory, queue, now=NOW) == ["run_due#1"]


def test_concurrent_sweepers_publish_each_retry_exactly_once(
    factory: sessionmaker[Session],
) -> None:
    _seed(factory)
    queue = MemoryQueue()
    n = 4
    barrier = threading.Barrier(n)
    results: list[list[str]] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        published = sweep_due_retries(factory, queue, now=NOW)
        with lock:
            results.append(published)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(len(r) for r in results) == 1
    assert queue.depth() == 1


def test_recover_ignores_retries_that_still_have_a_schedule(
    factory: sessionmaker[Session],
) -> None:
    _seed(factory)
    with session_scope(factory) as s:
        report = recover_interrupted(s)

    assert report.recovered_retries == ["run_plain#1"]  # run_due / run_future 仍在等 sweeper

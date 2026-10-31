"""阶段 8 验收：
多 Worker 负载分担、单 Worker 崩溃不丢任务、Dispatcher -> 队列 -> 多 Worker -> 重试 全链路。

用线程模拟多个 Worker 进程（共享同一个数据库和同一个队列）。
队列分别用内存实现和真实 Redis；连不上 Redis 时 redis 参数的用例会被 skip。
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import redis
from sqlalchemy.orm import Session, sessionmaker

from task_platform.dispatcher import Dispatcher
from task_platform.domain.enums import JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job, JobExecution, utc_now
from task_platform.domain.schedule import Schedule
from task_platform.faults import FlakyState
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    IdempotencyRepository,
    JobRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.logging.collector import LogCollector, collecting_logs
from task_platform.infrastructure.queue.base import TaskMessage, TaskQueue
from task_platform.infrastructure.queue.memory import MemoryQueue
from task_platform.infrastructure.queue.redis_stream import (
    RedisStreamQueue,
    create_redis_client,
    get_redis_url,
)
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry
from task_platform.retry_sweeper import sweep_due_retries
from task_platform.worker import Outcome, Worker


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture(params=["memory", "redis"])
def queue(request: pytest.FixtureRequest) -> Iterator[TaskQueue]:
    if request.param == "memory":
        yield MemoryQueue()
        return

    client = create_redis_client()
    try:
        client.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        client.close()
        pytest.skip(f"Redis not reachable at {get_redis_url()}")
    stream = f"task_platform:test:{uuid.uuid4().hex}"
    yield RedisStreamQueue(client, stream=stream)
    client.delete(stream)
    client.close()


class Cluster:
    """共享数据库 + 共享队列 + 若干 Worker 的小型"集群"（Worker 用线程模拟进程）"""

    def __init__(
        self, factory: sessionmaker[Session], collector: LogCollector, queue: TaskQueue
    ) -> None:
        self.factory = factory
        self.collector = collector
        self.queue = queue
        self.registry = TaskRegistry()
        self.calls: list[str] = []
        self.workers: dict[str, Worker] = {}
        lock = threading.Lock()
        flaky = FlakyState()
        calls = self.calls

        @job("count", registry=self.registry)
        def count_job(ctx: JobContext) -> str:
            time.sleep(0.03)  # 留出时间让别的 Worker 有机会取到消息
            with lock:
                calls.append(ctx.execution_id)
            return "ok"

        @job("flaky", max_attempts=3, base_delay_seconds=0.01, registry=self.registry)
        def flaky_job(ctx: JobContext) -> str:
            time.sleep(0.03)
            if flaky.should_fail(ctx.execution_id, fail_times=1):
                raise RuntimeError("injected failure")
            return "ok"

    def add_worker(self, name: str, **kwargs: Any) -> Worker:
        worker = Worker(
            self.queue,
            self.registry,
            self.factory,
            self.collector,
            consumer=name,
            max_workers=2,
            **kwargs,
        )
        self.workers[name] = worker
        return worker

    def add_job(self, name: str, handler: str) -> Job:
        j = Job(name=name, job_type=JobType.CUSTOM, handler=handler)
        with session_scope(self.factory) as s:
            JobRepository(s).add(j)
        return j

    def attempts(self, run_id: str) -> list[JobExecution]:
        with session_scope(self.factory) as s:
            return ExecutionRepository(s).list_by_run(run_id)

    def drain(self) -> dict[str, int]:
        """每个 Worker 一个线程同时开工，直到连续 3 次取不到消息；返回各 Worker 处理的条数"""
        counts = {name: 0 for name in self.workers}
        errors: list[BaseException] = []
        barrier = threading.Barrier(len(self.workers))

        def run(name: str, worker: Worker) -> None:
            try:
                barrier.wait()
                idle = 0
                while idle < 3:
                    result = worker.run_once(block_seconds=0.05)
                    if result is None:
                        idle += 1
                    else:
                        idle = 0
                        counts[name] += 1
            except BaseException as exc:  # 线程里的异常不能悄悄吞掉
                errors.append(exc)

        threads = [threading.Thread(target=run, args=item) for item in self.workers.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        return counts

    def close(self) -> None:
        for worker in self.workers.values():
            worker.close()


@pytest.fixture
def cluster(tmp_path: Path, queue: TaskQueue) -> Iterator[Cluster]:
    engine = create_db_engine(f"sqlite:///{tmp_path / 'cluster.db'}")
    init_db(engine)
    with collecting_logs() as collector:
        c = Cluster(make_session_factory(engine), collector, queue)
        yield c
        c.close()
    engine.dispose()


def test_multiple_workers_share_load_and_execute_each_message_once(cluster: Cluster) -> None:
    j = cluster.add_job("count-job", "count")
    run_ids = [f"run_{i:02d}" for i in range(30)]
    for run_id in run_ids:
        cluster.queue.publish(TaskMessage(j.id, run_id))
    for name in ("w1", "w2", "w3"):
        cluster.add_worker(name)

    counts = cluster.drain()

    assert sum(counts.values()) == 30
    assert sum(1 for c in counts.values() if c > 0) >= 2  # 不是只有一个 Worker 在干活
    assert sorted(cluster.calls) == run_ids  # 每个任务恰好执行一次
    with session_scope(cluster.factory) as s:
        page = ExecutionRepository(s).list_paginated(page_size=100)
    assert page.total == 30
    assert all(e.status is JobStatus.SUCCESS for e in page.items)
    assert (cluster.queue.depth(), cluster.queue.pending_count()) == (0, 0)


def test_crashed_workers_task_is_not_lost(cluster: Cluster) -> None:
    j = cluster.add_job("count-job", "count")
    cluster.queue.publish(TaskMessage(j.id, "run_1"))
    dead = cluster.queue.consume("dead-worker")  # 取走后崩溃：没执行完，也没有 ack
    assert dead is not None
    with session_scope(cluster.factory) as s:  # 崩溃前已经落了 RUNNING
        ExecutionRepository(s).add(
            JobExecution(
                job_id=j.id,
                run_id="run_1",
                attempt=1,
                status=JobStatus.RUNNING,
                started_at=utc_now(),
            )
        )
    rescuer = cluster.add_worker("rescuer", stale_after_seconds=0.3, reclaim_interval_seconds=0.0)

    assert rescuer.run_once() is None  # 租约还没过期，不能抢
    time.sleep(0.4)
    result = rescuer.run_once()

    assert result is not None and result.outcome is Outcome.FINISHED
    assert cluster.calls == ["run_1"]
    [ex] = cluster.attempts("run_1")
    assert ex.status is JobStatus.SUCCESS
    assert cluster.queue.pending_count() == 0


def test_dispatcher_to_workers_pipeline_with_retry(cluster: Cluster) -> None:
    cluster.add_worker("w1")
    cluster.add_worker("w2")

    def claim(j: Job, run_id: str, key: str) -> bool:
        with session_scope(cluster.factory) as s:
            return IdempotencyRepository(s).try_claim(key, j.id, run_id)

    def on_enqueued(execution: JobExecution) -> None:
        with session_scope(cluster.factory) as s:
            ExecutionRepository(s).add(execution)

    t0 = datetime(2026, 10, 30, tzinfo=UTC)
    clock = Clock(t0)
    dispatcher = Dispatcher(cluster.queue, claim=claim, on_enqueued=on_enqueued, clock=clock)
    for i in range(6):
        j = cluster.add_job(f"flaky-{i}", "flaky")
        dispatcher.add(
            j,
            Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=60),
        )

    clock.now = t0 + timedelta(seconds=61)
    queued = dispatcher.tick()
    assert len(queued) == 6
    run_ids = sorted(e.run_id for e in queued)

    first = cluster.drain()  # 每个任务第一次尝试都被注入失败
    assert sum(first.values()) == 6

    far_future = datetime.now(UTC) + timedelta(hours=1)
    assert len(sweep_due_retries(cluster.factory, cluster.queue, now=far_future)) == 6
    second = cluster.drain()
    assert sum(second.values()) == 6

    for run_id in run_ids:
        assert [(e.attempt, e.status) for e in cluster.attempts(run_id)] == [
            (1, JobStatus.RETRYING),
            (2, JobStatus.SUCCESS),
        ]
    assert cluster.queue.pending_count() == 0
    assert all(first[n] + second[n] > 0 for n in cluster.workers)  # 两个 Worker 都参与了

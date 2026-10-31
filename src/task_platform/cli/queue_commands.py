"""dispatch / worker 子命令的实现。独立成模块，避免 cli/main.py 的 _main 继续膨胀。"""

from __future__ import annotations

import logging
import os
import socket
import sys

import redis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from task_platform.bootstrap import load_active_schedules
from task_platform.cancellation import CancelWatcher
from task_platform.dispatcher import Dispatcher
from task_platform.domain.models import Job, JobExecution
from task_platform.infrastructure.db.repositories import ExecutionRepository, IdempotencyRepository
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.infrastructure.queue.base import TaskQueue
from task_platform.infrastructure.queue.factory import build_queue
from task_platform.infrastructure.queue.redis_stream import get_redis_url
from task_platform.registry import TaskRegistry
from task_platform.retry_sweeper import sweep_due_retries
from task_platform.runtime import run_loop, stop_on_signals
from task_platform.worker import Worker

logger = logging.getLogger("task_platform.cli")


def default_consumer_name() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _open_queue() -> TaskQueue | None:
    try:
        return build_queue()
    except redis.exceptions.ConnectionError:
        print(
            f"无法连接 Redis：{get_redis_url()}。"
            "请确认容器已启动，并在当前终端设置 TASK_PLATFORM_REDIS_URL。",
            file=sys.stderr,
        )
        return None


def run_dispatch(factory: sessionmaker[Session], *, poll_interval: float) -> int:
    """按数据库里已启用的调度向队列投递任务，并投递到期的重试。
    调度规则只在启动时加载一次；之后新增的规则需要重启 dispatch（与 serve 一致）。"""
    with session_scope(factory) as s:
        pairs = load_active_schedules(s)
    if not pairs:
        print(
            "数据库中没有任何已启用的 (Job, Schedule)，无法启动调度。"
            "请先用 `schedule` 子命令为任务添加调度规则。",
            file=sys.stderr,
        )
        return 1

    def claim(_job: Job, run_id: str, key: str) -> bool:
        with session_scope(factory) as se:
            return IdempotencyRepository(se).try_claim(key, _job.id, run_id)

    def release(key: str) -> None:
        with session_scope(factory) as se:
            IdempotencyRepository(se).release(key)

    def on_enqueued(execution: JobExecution) -> None:
        try:
            with session_scope(factory) as se:
                ExecutionRepository(se).add(execution)
        except IntegrityError:
            # Worker 比我们更快：已经写入了这次尝试的记录，QUEUED 投影不再需要
            logger.info("execution row already exists, skip QUEUED projection")

    queue_or_none = _open_queue()
    if queue_or_none is None:
        return 1

    queue: TaskQueue = queue_or_none
    dispatcher = Dispatcher(queue, claim=claim, release=release, on_enqueued=on_enqueued)
    for job, schedule in pairs:
        dispatcher.add(job, schedule)

    def step() -> None:
        dispatcher.tick()
        sweep_due_retries(factory, queue)

    print(f"Dispatcher 已启动：{len(pairs)} 条 (job, schedule) 组合。按 Ctrl+C 优雅退出。")
    with stop_on_signals() as stop:
        run_loop(step, interval=poll_interval, stop=stop)
    print("Dispatcher 已优雅退出。")
    return 0


def run_worker(
    factory: sessionmaker[Session],
    registry: TaskRegistry,
    collector: LogCollector,
    *,
    consumer: str | None,
    block_seconds: float,
    stale_after: float,
    heartbeat_interval: float | None,
) -> int:
    name = consumer or default_consumer_name()
    stale = stale_after if stale_after > 0 else None
    heartbeat = (
        heartbeat_interval
        if heartbeat_interval is not None
        else (stale / 3 if stale is not None else None)
    )
    queue = _open_queue()
    if queue is None:
        return 1
    try:
        worker = Worker(
            queue,
            registry,
            factory,
            collector,
            consumer=name,
            stale_after_seconds=stale,
            heartbeat_interval_seconds=heartbeat,
        )
    except ValueError as exc:
        print(f"参数不合法：{exc}", file=sys.stderr)
        return 1
    watcher = CancelWatcher(factory, worker.active_runs)

    print(f"Worker {name} 已启动。按 Ctrl+C 优雅退出。")
    watcher.start()
    try:
        with stop_on_signals() as stop:
            # 消费本身会阻塞 block_seconds，所以循环间隔为 0；收到信号后最多再等一次阻塞
            run_loop(lambda: worker.run_once(block_seconds=block_seconds), interval=0.0, stop=stop)
    finally:
        watcher.stop()
        worker.close()
    print(f"Worker {name} 已优雅退出。")
    return 0

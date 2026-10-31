"""装配回归：dispatch 投递 -> worker 消费执行，经由真实 CLI 入口。
队列用内存实现（同一进程内共享），不需要 Redis。"""

from __future__ import annotations

import signal
import threading
from pathlib import Path

import pytest
from sqlalchemy import select

from task_platform.cli.main import main
from task_platform.domain.enums import JobStatus
from task_platform.infrastructure.db.models import ExecutionRecord
from task_platform.infrastructure.db.session import (
    create_db_engine,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.queue.memory import MemoryQueue


def test_dispatch_then_worker_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = f"sqlite:///{tmp_path / 'queue_cli.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    queue = MemoryQueue()

    def fake_build_queue() -> MemoryQueue:
        # 在队列构建时才启动定时器：此刻离安装信号 handler 只有几微秒，
        # 避免启动（迁移、同步任务）较慢时 SIGINT 抢在 handler 之前到达
        threading.Timer(1.5, lambda: signal.raise_signal(signal.SIGINT)).start()
        return queue

    monkeypatch.setattr("task_platform.cli.queue_commands.build_queue", fake_build_queue)

    assert main(["schedule", "demo-success", "--interval", "0.2"]) == 0

    assert main(["dispatch", "--poll-interval", "0.05"]) == 0
    assert queue.depth() >= 1  # Dispatcher 只投递，没有执行任何东西

    assert main(["worker", "--block-seconds", "0.2"]) == 0
    assert queue.pending_count() == 0  # 优雅退出：已取出的消息都已 ack

    engine = create_db_engine(url)
    try:
        with session_scope(make_session_factory(engine)) as s:
            statuses = [row[0] for row in s.execute(select(ExecutionRecord.status))]
    finally:
        engine.dispose()
    assert JobStatus.SUCCESS in statuses

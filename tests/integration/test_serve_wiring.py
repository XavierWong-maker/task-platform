from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import func, select

from task_platform.cli.main import main
from task_platform.infrastructure.db.models import ExecutionRecord
from task_platform.infrastructure.db.session import (
    create_db_engine,
    make_session_factory,
    session_scope,
)


def test_serve_actually_triggers_scheduled_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """装配回归：serve 必须把数据库里的调度加入 Scheduler 并真正触发执行"""
    url = f"sqlite:///{tmp_path / 'serve_wiring.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    assert main(["schedule", "demo-success", "--interval", "0.2"]) == 0

    # serve 会注册 SIGINT handler，只能在主线程运行；用线程里的 stop 信号代替 Ctrl+C
    import signal

    threading.Timer(2.0, lambda: signal.raise_signal(signal.SIGINT)).start()
    start = time.monotonic()
    assert main(["serve", "--poll-interval", "0.05"]) == 0
    assert time.monotonic() - start < 10

    engine = create_db_engine(url)
    try:
        with session_scope(make_session_factory(engine)) as s:
            count = s.scalar(select(func.count()).select_from(ExecutionRecord))
    finally:
        engine.dispose()

    assert isinstance(count, int)
    assert count >= 1

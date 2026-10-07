"""日志收集器：把带 run_id 的日志按 run 缓冲在内存里，由调用方在持久化 execution 时取走。

run_id 的来源（按优先级）：
1. LogRecord 上的 run_id 属性（executor/scheduler 用 extra={"run_id": ...} 传入）
2. logger 名为 task_platform.job.<run_id>（JobContext.logger 的命名约定）
没有 run_id 的日志（如 "job triggered"）直接忽略。
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime

from task_platform.domain.models import JobLog

_ROOT_LOGGER_NAME = "task_platform"
_JOB_LOGGER_PREFIX = "task_platform.job."


def _extract_run_id(record: logging.LogRecord) -> str | None:
    run_id = getattr(record, "run_id", None)
    if isinstance(run_id, str):
        return run_id
    if record.name.startswith(_JOB_LOGGER_PREFIX):
        return record.name.removeprefix(_JOB_LOGGER_PREFIX)
    return None


class LogCollector(logging.Handler):
    """线程安全：handler 可能在线程的 worker 线程里被调用"""

    def __init__(self, level: int = logging.INFO) -> None:
        super().__init__(level)
        self._buffer_lock = threading.Lock()
        self._by_run: dict[str, list[JobLog]] = defaultdict(list)

    def emit(self, record: logging.LogRecord) -> None:
        run_id = _extract_run_id(record)
        if run_id is None:
            return
        entry = JobLog(
            run_id=run_id,
            attempt=0,  # 占位，drain 时按实际 attempt 填入
            level=record.levelname,
            message=record.getMessage(),
            created_at=datetime.fromtimestamp(record.created, tz=UTC),
        )
        with self._buffer_lock:
            self._by_run[run_id].append(entry)

    def drain(self, run_id: str, *, attempt: int) -> list[JobLog]:
        """取走并清空某个 run 目前缓冲的日志，统一标记为给定 attempt"""
        with self._buffer_lock:
            entries = self._by_run.pop(run_id, [])
        return [replace(entry, attempt=attempt) for entry in entries]


@contextmanager
def collecting_logs(level: int = logging.INFO) -> Iterator[LogCollector]:
    """在 with 块内把 LogCollector 挂到 task_platform logger 上，退出时无条件卸载"""
    logger = logging.getLogger(_ROOT_LOGGER_NAME)
    collector = LogCollector(level)
    previous_level = logger.level
    logger.addHandler(collector)
    if logger.getEffectiveLevel() > level:
        logger.setLevel(level)
    try:
        yield collector
    finally:
        logger.removeHandler(collector)
        logger.setLevel(previous_level)

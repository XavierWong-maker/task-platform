"""长期运行进程（dispatch / worker）共用的运行时工具：信号转停止事件、带容错的循环。"""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import FrameType
from typing import Any

logger = logging.getLogger("task_platform.runtime")


@contextmanager
def stop_on_signals() -> Iterator[threading.Event]:
    """with 块内：收到 SIGINT/SIGTERM 就置位返回的事件；退出时恢复原有 handler。
    只能在主线程使用（signal.signal 的限制）；Windows 下只有 SIGINT（Ctrl+C）生效。
    """
    event = threading.Event()

    def _handler(signum: int, _: FrameType | None) -> None:
        logger.info("received shutdown signal", extra={"signum": signum})
        event.set()

    previous: dict[Any, Any] = {}
    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if isinstance(sig, int):
            previous[sig] = signal.signal(sig, _handler)
    try:
        yield event
    finally:
        for sig, handler in previous.items():
            if handler is not None:
                signal.signal(sig, handler)


def run_loop(
    step: Callable[[], object],
    *,
    interval: float,
    stop: threading.Event,
    error_backoff: float = 1.0,
) -> None:
    """反复执行 step，直到 stop 被置位。step 抛异常只记日志并退避，不让进程退出。
    当前这一步总是会执行完才检查 stop（优雅关闭：不打断正在处理的任务）。
    """
    while not stop.is_set():
        # noinspection PyBroadException
        try:
            step()
        except Exception:
            logger.exception("loop step failed, backing off")
            stop.wait(error_backoff)
            continue
        stop.wait(interval)

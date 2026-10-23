"""取消令牌与活跃运行表（纯内存、线程安全，不依赖数据库）"""

from __future__ import annotations

import threading


class JobCanceledError(Exception):
    """任务被取消。继承 Exception: handler 里若写了宽泛的 'except Exception', 会吞掉它"""

    def __init__(self, message: str = "job canceled") -> None:
        super().__init__(message)


class CancelToken:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        if self._event.is_set():
            raise JobCanceledError()

    def wait(self, timeout: float) -> bool:
        """可被取消打断的 sleep：返回 Ture 表示期间被取消"""
        return self._event.wait(timeout)


class ActiveRuns:
    """run_id -> CancelToken: 当前进程正在执行的运行"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: dict[str, CancelToken] = {}

    def register(self, run_id: str, token: CancelToken) -> None:
        with self._lock:
            self._tokens[run_id] = token

    def unregister(self, run_id: str) -> None:
        with self._lock:
            self._tokens.pop(run_id, None)

    def cancel(self, run_id: str) -> bool:
        with self._lock:
            token = self._tokens.get(run_id)
        if token is None:
            return False
        token.cancel()
        return True

    def active_ids(self) -> list[str]:
        with self._lock:
            return list(self._tokens)

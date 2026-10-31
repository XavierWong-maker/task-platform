"""内存版队列：模拟 Redis Streams 消费者组语义，用于单元测试和本地开发。

线程安全；时钟可注入，测试里用假时钟即可验证"空闲超时接管"，不需要真的 sleep。
"""

from __future__ import annotations

import itertools
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from task_platform.infrastructure.queue.base import Delivery, TaskMessage


@dataclass(slots=True)
class _Pending:
    message: TaskMessage
    consumer: str
    delivered_at: float
    delivery_count: int


class MemoryQueue:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._cond = threading.Condition()
        self._ready: deque[tuple[str, TaskMessage]] = deque()
        self._pending: dict[str, _Pending] = {}
        self._seq = itertools.count(1)

    def publish(self, message: TaskMessage) -> str:
        with self._cond:
            message_id = f"{next(self._seq)}-0"
            self._ready.append((message_id, message))
            self._cond.notify()
        return message_id

    def consume(self, consumer: str, *, block_seconds: float = 0.0) -> Delivery | None:
        with self._cond:
            if not self._ready and block_seconds > 0:
                self._cond.wait_for(lambda: bool(self._ready), timeout=block_seconds)
            if not self._ready:
                return None
            message_id, message = self._ready.popleft()
            self._pending[message_id] = _Pending(message, consumer, self._clock(), 1)
            return Delivery(message_id, message, consumer, 1)

    def ack(self, message_id: str) -> bool:
        with self._cond:
            return self._pending.pop(message_id, None) is not None

    def claim_stale(
        self, consumer: str, *, min_idle_seconds: float, count: int = 10
    ) -> list[Delivery]:
        now = self._clock()
        claimed: list[Delivery] = []
        with self._cond:
            for message_id, entry in self._pending.items():
                if len(claimed) >= count:
                    break
                if now - entry.delivered_at < min_idle_seconds:
                    continue
                entry.consumer = consumer
                entry.delivered_at = now
                entry.delivery_count += 1
                claimed.append(Delivery(message_id, entry.message, consumer, entry.delivery_count))
        return claimed

    def renew(self, message_id: str, *, consumer: str) -> bool:
        with self._cond:
            entry = self._pending.get(message_id)
            if entry is None or entry.consumer != consumer:
                return False
            entry.delivered_at = self._clock()
            return True

    def depth(self) -> int:
        with self._cond:
            return len(self._ready)

    def pending_count(self) -> int:
        with self._cond:
            return len(self._pending)

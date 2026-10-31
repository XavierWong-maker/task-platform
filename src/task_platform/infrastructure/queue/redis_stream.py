"""Redis Streams 实现的 TaskQueue

- publish   -> XADD（消息体放在 payload 字段，JSON）
- consume   -> XREADGROUP（消费者组，count=1）
- ack       -> XACK + XDEL（同一事务；否则已处理的条目会一直留在 stream 里）
- claim_stale -> XAUTOCLAIM（接管空闲超时的 pending），再用 XPENDING 取累计投递次数

配置：TASK_PLATFORM_REDIS_URL（环境变量 > 默认值）。
client 在边界处标注为 Any：redis-py 的同步/异步返回值是联合类型，对 mypy 不友好。
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import redis
from redis.exceptions import ResponseError

from task_platform.infrastructure.queue.base import Delivery, InvalidMessageError, TaskMessage

logger = logging.getLogger("task_platform.queue")

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_STREAM = "task_platform:jobs"
DEFAULT_GROUP = "workers"
_PAYLOAD_FIELD = "payload"


def get_redis_url() -> str:
    """配置优先级：环境变量 > 默认值"""
    return os.environ.get("TASK_PLATFORM_REDIS_URL", DEFAULT_REDIS_URL)


def create_redis_client(url: str | None = None) -> Any:
    return redis.Redis.from_url(url or get_redis_url(), decode_responses=True)


class RedisStreamQueue:
    def __init__(
        self, client: Any, *, stream: str = DEFAULT_STREAM, group: str = DEFAULT_GROUP
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._ensure_group()

    def _ensure_group(self) -> None:
        """幂等创建消费者组；id="0" 保证组创建之前已入队的消息也会被投递"""
        try:
            self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def publish(self, message: TaskMessage) -> str:
        message_id = self._client.xadd(self._stream, {_PAYLOAD_FIELD: message.to_json()})
        return str(message_id)

    def consume(self, consumer: str, *, block_seconds: float = 0.0) -> Delivery | None:
        deadline = time.monotonic() + max(block_seconds, 0.0)
        while True:
            block_ms = int(max(deadline - time.monotonic(), 0.0) * 1000)
            # 注意：Redis 的 BLOCK 0 表示永久阻塞，所以非阻塞时必须不传 block
            kwargs: dict[str, Any] = {"block": block_ms} if block_ms > 0 else {}
            result = self._client.xreadgroup(
                self._group, consumer, {self._stream: ">"}, count=1, **kwargs
            )
            if not result:
                return None
            deliveries = self._to_deliveries(result[0][1], consumer, claimed=False)
            if deliveries:
                return deliveries[0]
            # 读到的是毒消息（已丢弃），继续读下一条

    def ack(self, message_id: str) -> bool:
        pipe = self._client.pipeline(transaction=True)
        pipe.xack(self._stream, self._group, message_id)
        pipe.xdel(self._stream, message_id)
        acked, _deleted = pipe.execute()
        return int(acked) == 1

    def claim_stale(
        self, consumer: str, *, min_idle_seconds: float, count: int = 10
    ) -> list[Delivery]:
        result = self._client.xautoclaim(
            self._stream,
            self._group,
            consumer,
            min_idle_time=int(min_idle_seconds * 1000),
            start_id="0-0",
            count=count,
        )
        return self._to_deliveries(result[1], consumer, claimed=True)

    def renew(self, message_id: str, *, consumer: str) -> bool:
        """先确认仍是持有者，再 XCLAIM JUSTID 重置空闲计时（JUSTID 不会增加投递次数）。
        检查与续约之间有极小的竞态窗口；即使发生，重复执行也由 Worker 的数据库检查兜底。"""
        rows = self._client.xpending_range(
            self._stream, self._group, min=message_id, max=message_id, count=1
        )
        if not rows or rows[0]["consumer"] != consumer:
            return False
        claimed = self._client.xclaim(
            self._stream,
            self._group,
            consumer,
            min_idle_time=0,
            message_ids=[message_id],
            justid=True,
        )
        return bool(claimed)

    def depth(self) -> int:
        """stream 里剩余条目 = 未消费 + pending（已 ack 的会被 XDEL），所以相减即未消费数"""
        pipe = self._client.pipeline(transaction=True)
        pipe.xlen(self._stream)
        pipe.xpending(self._stream, self._group)
        length, summary = pipe.execute()
        return int(length) - int(summary["pending"])

    def pending_count(self) -> int:
        return int(self._client.xpending(self._stream, self._group)["pending"])

    def _to_deliveries(self, entries: list[Any], consumer: str, *, claimed: bool) -> list[Delivery]:
        deliveries: list[Delivery] = []
        for message_id, fields in entries:
            raw = fields.get(_PAYLOAD_FIELD) if fields else None
            try:
                if not isinstance(raw, str):
                    raise InvalidMessageError("missing payload field")
                message = TaskMessage.from_json(raw)
            except InvalidMessageError as exc:
                logger.error(
                    "dropping invalid queue message",
                    extra={"message_id": message_id, "error": str(exc)},
                )
                self.ack(message_id)
                continue
            count = self._times_delivered(message_id) if claimed else 1
            deliveries.append(Delivery(message_id, message, consumer, count))
        return deliveries

    def _times_delivered(self, message_id: str) -> int:
        rows = self._client.xpending_range(
            self._stream, self._group, min=message_id, max=message_id, count=1
        )
        return int(rows[0]["times_delivered"]) if rows else 1

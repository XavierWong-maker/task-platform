"""任务队列的消息模型与协议

Scheduler 是生产者，Worker 是消费者，两者只通过 TaskQueue 协议交互。
消息只携带"引用"（job_id / run_id / attempt），不携带任务定义：
任务定义以数据库为准，Worker 崩溃也不会丢失定义。

语义对齐 Redis Streams 的消费者组：
- consume 取出的消息进入 pending，直到被 ack
- 未 ack 且空闲超过阈值的消息，可被其他消费者 claim_stale 接管
- 因此投递语义是"至少一次"，重复消费由 (run_id, attempt) 唯一约束与幂等键兜底
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Protocol


class InvalidMessageError(Exception):
    """队列里的原始数据无法还原为 TaskMessage"""


@dataclass(frozen=True, slots=True)
class TaskMessage:
    job_id: str
    run_id: str
    attempt: int = 1
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, str) or not self.job_id.strip():
            raise ValueError("job_id must be a non-empty string")
        if not isinstance(self.run_id, str) or not self.run_id.strip():
            raise ValueError("run_id must be a non-empty string")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 1:
            raise ValueError("attempt must be an integer >= 1")
        if self.idempotency_key is not None and (
            not isinstance(self.idempotency_key, str) or not self.idempotency_key.strip()
        ):
            raise ValueError("idempotency_key must be None or a non-empty string")

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> TaskMessage:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise InvalidMessageError(f"not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise InvalidMessageError("message must be a JSON object")
        missing = {"job_id", "run_id", "attempt"} - data.keys()
        if missing:
            raise InvalidMessageError(f"missing fields: {sorted(missing)}")
        try:
            # 未知字段被忽略：新版本 Scheduler 加字段，旧 Worker 仍可消费
            return cls(
                job_id=data["job_id"],
                run_id=data["run_id"],
                attempt=data["attempt"],
                idempotency_key=data.get("idempotency_key"),
            )
        except ValueError as exc:
            raise InvalidMessageError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class Delivery:
    """一次投递：消息 + 队列里的消息 ID + 当前持有者 + 累计投递次数"""

    message_id: str
    message: TaskMessage
    consumer: str
    delivery_count: int = 1


class TaskQueue(Protocol):
    def publish(self, message: TaskMessage) -> str:
        """入队，返回队列分配的 message_id"""
        pass

    def consume(self, consumer: str, *, block_seconds: float = 0.0) -> Delivery | None:
        """取一条新消息；没有则最多阻塞 block_seconds，仍没有返回 None。
        取出后消息进入 pending，必须 ack"""
        pass

    def ack(self, message_id: str) -> bool:
        """确认处理完成；返回是否确实移除了一条 pending（重复 ack 返回 False）"""
        pass

    def claim_stale(
        self, consumer: str, *, min_idle_seconds: float, count: int = 10
    ) -> list[Delivery]:
        """接管空闲超过 min_idle_seconds 的 pending 消息，delivery_count 加一"""
        pass

    def renew(self, message_id: str, *, consumer: str) -> bool:
        """续约（心跳）：重置 pending 消息的空闲计时，防止被其他消费者接管。
        消息已被 ack、或已被别的消费者接管时返回 False"""
        pass

    def depth(self) -> int:
        """尚未被消费的消息数"""
        pass

    def pending_count(self) -> int:
        """已被消费但尚未 ack 的消息数"""
        pass

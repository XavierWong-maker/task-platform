"""RedisStreamQueue 集成测试：需要可连接的 Redis（TASK_PLATFORM_REDIS_URL），否则整体 skip。"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
import redis

from task_platform.infrastructure.queue.base import TaskMessage, TaskQueue
from task_platform.infrastructure.queue.redis_stream import (
    RedisStreamQueue,
    create_redis_client,
    get_redis_url,
)


@pytest.fixture
def client() -> Iterator[Any]:
    c = create_redis_client()
    try:
        c.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        pytest.skip(f"Redis not reachable at {get_redis_url()}")
    yield c
    c.close()


@pytest.fixture
def stream(client: Any) -> Iterator[str]:
    name = f"task_platform:test:{uuid.uuid4().hex}"
    yield name
    client.delete(name)  # 连同消费者组一起清掉


@pytest.fixture
def queue(client: Any, stream: str) -> RedisStreamQueue:
    return RedisStreamQueue(client, stream=stream)


def _msg(n: int) -> TaskMessage:
    return TaskMessage("job_1", f"run_{n}", idempotency_key=f"k{n}")


def test_satisfies_protocol_and_group_creation_is_idempotent(client: Any, stream: str) -> None:
    first: TaskQueue = RedisStreamQueue(client, stream=stream)
    RedisStreamQueue(client, stream=stream)  # 组已存在，不应抛 BUSYGROUP
    assert first.depth() == 0


def test_publish_consume_roundtrip_fifo(queue: RedisStreamQueue) -> None:
    first_id = queue.publish(_msg(1))
    queue.publish(_msg(2))
    assert queue.depth() == 2

    first = queue.consume("w1")
    second = queue.consume("w1")

    assert first is not None and second is not None
    assert first.message_id == first_id
    assert [first.message, second.message] == [_msg(1), _msg(2)]
    assert first.consumer == "w1" and first.delivery_count == 1
    assert (queue.depth(), queue.pending_count()) == (0, 2)


def test_consume_on_empty_queue_returns_none_without_blocking(queue: RedisStreamQueue) -> None:
    start = time.monotonic()
    assert queue.consume("w1") is None
    assert time.monotonic() - start < 1.0


def test_blocking_consume_times_out(queue: RedisStreamQueue) -> None:
    start = time.monotonic()
    assert queue.consume("w1", block_seconds=0.2) is None
    assert time.monotonic() - start >= 0.15


def test_blocking_consume_wakes_up_on_publish(queue: RedisStreamQueue) -> None:
    threading.Timer(0.1, lambda: queue.publish(_msg(1))).start()
    delivery = queue.consume("w1", block_seconds=3.0)
    assert delivery is not None and delivery.message == _msg(1)


def test_ack_removes_pending_and_deletes_entry(
    queue: RedisStreamQueue, client: Any, stream: str
) -> None:
    queue.publish(_msg(1))
    delivery = queue.consume("w1")
    assert delivery is not None

    assert queue.ack(delivery.message_id) is True
    assert queue.ack(delivery.message_id) is False
    assert queue.pending_count() == 0
    assert client.xlen(stream) == 0


def test_claim_stale_takes_over_idle_pending_message(queue: RedisStreamQueue) -> None:
    queue.publish(_msg(1))
    original = queue.consume("w1")  # w1 之后"崩溃"，不再 ack
    assert original is not None

    assert queue.claim_stale("w2", min_idle_seconds=60) == []  # 还没空闲够久
    time.sleep(0.15)
    [taken] = queue.claim_stale("w2", min_idle_seconds=0.1)

    assert taken.message_id == original.message_id
    assert taken.message == original.message
    assert taken.consumer == "w2" and taken.delivery_count == 2
    assert queue.claim_stale("w3", min_idle_seconds=5) == []  # 空闲计时已重置
    assert queue.ack(taken.message_id) is True
    assert queue.pending_count() == 0


def test_invalid_payload_is_dropped_and_skipped(
    queue: RedisStreamQueue, client: Any, stream: str
) -> None:
    client.xadd(stream, {"payload": "not json"})
    client.xadd(stream, {"other": "field"})
    queue.publish(_msg(1))

    delivery = queue.consume("w1")

    assert delivery is not None and delivery.message == _msg(1)
    assert queue.pending_count() == 1  # 两条毒消息已被 ack
    assert client.xlen(stream) == 1  # 且已被删除


def test_concurrent_consumers_each_message_delivered_exactly_once(
    queue: RedisStreamQueue,
) -> None:
    total = 100
    for i in range(total):
        queue.publish(_msg(i))

    seen: list[str] = []
    lock = threading.Lock()

    def worker(name: str) -> None:
        while (delivery := queue.consume(name)) is not None:
            with lock:
                seen.append(delivery.message.run_id)
            queue.ack(delivery.message_id)

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(seen) == sorted(f"run_{i}" for i in range(total))
    assert queue.pending_count() == 0


def test_renew_resets_idle_without_bumping_delivery_count(queue: RedisStreamQueue) -> None:
    queue.publish(_msg(1))
    delivery = queue.consume("w1")
    assert delivery is not None

    time.sleep(0.2)
    assert queue.renew(delivery.message_id, consumer="w1") is True
    assert queue.claim_stale("w2", min_idle_seconds=0.15) == []  # 空闲计时已被重置

    time.sleep(0.2)
    [taken] = queue.claim_stale("w2", min_idle_seconds=0.15)
    assert taken.delivery_count == 2  # 续约没有额外增加投递次数


def test_renew_fails_for_other_consumer_or_after_ack(queue: RedisStreamQueue) -> None:
    queue.publish(_msg(1))
    delivery = queue.consume("w1")
    assert delivery is not None

    assert queue.renew(delivery.message_id, consumer="w2") is False
    queue.ack(delivery.message_id)
    assert queue.renew(delivery.message_id, consumer="w1") is False

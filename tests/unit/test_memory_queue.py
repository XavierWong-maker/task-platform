from __future__ import annotations

import threading

from task_platform.infrastructure.queue.base import TaskMessage, TaskQueue
from task_platform.infrastructure.queue.memory import MemoryQueue


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _msg(n: int) -> TaskMessage:
    return TaskMessage("job_1", f"run_{n}")


def test_memory_queue_satisfies_protocol() -> None:
    queue: TaskQueue = MemoryQueue()  # mypy 会检查协议是否被满足
    assert queue.depth() == 0


def test_fifo_and_consumed_message_becomes_pending() -> None:
    q = MemoryQueue()
    q.publish(_msg(1))
    q.publish(_msg(2))
    assert q.depth() == 2

    first = q.consume("w1")
    second = q.consume("w1")

    assert first is not None and second is not None
    assert [first.message.run_id, second.message.run_id] == ["run_1", "run_2"]
    assert first.consumer == "w1" and first.delivery_count == 1
    assert (q.depth(), q.pending_count()) == (0, 2)


def test_consume_on_empty_queue_returns_none() -> None:
    assert MemoryQueue().consume("w1") is None


def test_blocking_consume_wakes_up_on_publish() -> None:
    q = MemoryQueue()
    threading.Timer(0.05, lambda: q.publish(_msg(1))).start()
    delivery = q.consume("w1", block_seconds=2.0)
    assert delivery is not None and delivery.message.run_id == "run_1"


def test_blocking_consume_times_out() -> None:
    assert MemoryQueue().consume("w1", block_seconds=0.05) is None


def test_ack_removes_pending_and_is_idempotent() -> None:
    q = MemoryQueue()
    q.publish(_msg(1))
    delivery = q.consume("w1")
    assert delivery is not None

    assert q.ack(delivery.message_id) is True
    assert q.ack(delivery.message_id) is False
    assert q.ack("no-such-id") is False
    assert q.pending_count() == 0


def test_claim_stale_respects_idle_threshold_and_bumps_delivery_count() -> None:
    clock = FakeClock()
    q = MemoryQueue(clock=clock)
    q.publish(_msg(1))
    original = q.consume("w1")  # t=0，w1 随后"崩溃"，不再 ack
    assert original is not None

    clock.now = 10.0
    assert q.claim_stale("w2", min_idle_seconds=30) == []  # 还没空闲够久

    clock.now = 31.0
    [taken] = q.claim_stale("w2", min_idle_seconds=30)
    assert taken.message_id == original.message_id
    assert taken.consumer == "w2" and taken.delivery_count == 2

    assert q.claim_stale("w3", min_idle_seconds=30) == []  # 空闲计时已重置
    assert q.ack(taken.message_id) is True
    assert q.pending_count() == 0


def test_acked_messages_are_never_claimed() -> None:
    clock = FakeClock()
    q = MemoryQueue(clock=clock)
    q.publish(_msg(1))
    delivery = q.consume("w1")
    assert delivery is not None
    q.ack(delivery.message_id)

    clock.now = 1000.0
    assert q.claim_stale("w2", min_idle_seconds=1) == []


def test_claim_stale_honors_count() -> None:
    clock = FakeClock()
    q = MemoryQueue(clock=clock)
    for i in range(3):
        q.publish(_msg(i))
        q.consume("w1")

    clock.now = 100.0
    assert len(q.claim_stale("w2", min_idle_seconds=1, count=2)) == 2


def test_concurrent_consumers_each_message_delivered_exactly_once() -> None:
    q = MemoryQueue()
    total = 200
    for i in range(total):
        q.publish(_msg(i))

    seen: list[str] = []
    lock = threading.Lock()

    def worker(name: str) -> None:
        while (delivery := q.consume(name)) is not None:
            with lock:
                seen.append(delivery.message.run_id)
            q.ack(delivery.message_id)

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(seen) == sorted(f"run_{i}" for i in range(total))
    assert q.pending_count() == 0


def test_renew_resets_idle_time_and_checks_ownership() -> None:
    clock = FakeClock()
    q = MemoryQueue(clock=clock)
    q.publish(_msg(1))
    delivery = q.consume("w1")
    assert delivery is not None

    clock.now = 25.0
    assert q.renew(delivery.message_id, consumer="w1") is True
    clock.now = 40.0  # 距上次续约 15s < 30s
    assert q.claim_stale("w2", min_idle_seconds=30) == []
    assert q.renew(delivery.message_id, consumer="w2") is False  # 不是持有者

    clock.now = 56.0  # 距上次续约 31s
    [taken] = q.claim_stale("w2", min_idle_seconds=30)
    assert taken.delivery_count == 2
    assert q.renew(delivery.message_id, consumer="w1") is False  # 已被接管

    q.ack(taken.message_id)
    assert q.renew(taken.message_id, consumer="w2") is False  # 已 ack

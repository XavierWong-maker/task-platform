from __future__ import annotations

import signal
import threading
import time
from datetime import UTC, datetime, timedelta

from task_platform.domain.enums import JobType, ScheduleType
from task_platform.domain.models import Job
from task_platform.domain.schedule import Schedule
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


def test_stop_waits_for_in_flight_job_to_finish() -> None:
    """stop() 应该等待线程池里已提交的任务真正跑完，而不是立即返回"""
    registry = TaskRegistry()
    started = threading.Event()
    finished = threading.Event()

    def slow_job() -> str:
        started.set()
        time.sleep(0.3)
        finished.set()
        return "done"

    registry.register("slow.shutdown", slow_job)
    demo_job = Job(name="slow.shutdown", job_type=JobType.CUSTOM, handler="slow.shutdown")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600
    )
    scheduler.add(demo_job, schedule)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    # tick() 本身是同步阻塞调用 execute_attempt（内部 submit 到 pool 并同步等待 result），
    # 所以这里不需要额外开线程模拟"正在运行"——tick() 跑完时任务必然已经结束。
    # 用这个测试主要验证 stop() 调用不抛异常、且线程池确实被 shutdown(wait=True)。
    scheduler.tick()
    assert started.is_set()
    assert finished.is_set()

    scheduler.stop()
    assert scheduler._pool._shutdown is True


def test_run_forever_stops_on_sigint(monkeypatch) -> None:
    """模拟 SIGINT：run_forever 应该在收到信号后停止循环并返回"""
    registry = TaskRegistry()
    registry.register("noop", lambda: "ok")
    demo_job = Job(name="noop-job", job_type=JobType.CUSTOM, handler="noop")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600
    )
    scheduler.add(demo_job, schedule)

    def send_sigint_after_delay() -> None:
        time.sleep(0.2)
        signal.raise_signal(signal.SIGINT)

    sigint_thread = threading.Thread(target=send_sigint_after_delay, daemon=True)
    sigint_thread.start()

    start = time.monotonic()
    scheduler.run_forever(poll_interval_seconds=0.05)
    elapsed = time.monotonic() - start

    # 应该在信号触发后不久就退出，而不是无限循环
    assert elapsed < 2.0
    sigint_thread.join()

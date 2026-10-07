from __future__ import annotations

import signal
import threading
import time
from datetime import UTC, datetime, timedelta

from task_platform.domain.enums import JobType, ScheduleType
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


def test_run_forever_invokes_on_tick_with_executions() -> None:
    registry = TaskRegistry()
    registry.register("noop", lambda: "ok")
    demo_job = Job(name="noop-job", job_type=JobType.CUSTOM, handler="noop")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=0.05
    )
    scheduler.add(demo_job, schedule)
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(hours=1)

    seen: list[list[JobExecution]] = []

    def _on_tick(executions: list[JobExecution]) -> None:
        seen.append(executions)
        if len(seen) >= 2:
            scheduler.stop()

    scheduler.run_forever(poll_interval_seconds=0.01, on_tick=_on_tick)

    assert len(seen) >= 2
    assert any(execs for execs in seen)  # 至少一次 tick 真正触发了任务


def test_run_forever_without_on_tick_still_works() -> None:
    """on_tick 默认 None, 兼容调用方式"""
    registry = TaskRegistry()
    registry.register("noop2", lambda: "ok")
    demo_job = Job(name="noop2-job", job_type=JobType.CUSTOM, handler="noop2")
    scheduler = Scheduler(registry)
    schedule = Schedule(
        job_id=demo_job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600
    )
    scheduler.add(demo_job, schedule)

    def send_sigint_after_delay() -> None:
        time.sleep(0.1)
        signal.raise_signal(signal.SIGINT)

    sigint_thread = threading.Thread(target=send_sigint_after_delay, daemon=True)
    sigint_thread.start()
    scheduler.run_forever(poll_interval_seconds=0.02)
    sigint_thread.join()

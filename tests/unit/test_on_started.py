from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from task_platform.domain.enums import JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.executor import execute_attempt
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


def test_on_started_fires_while_running_before_handler() -> None:
    registry = TaskRegistry()
    seen: list[object] = []
    registry.register("h", lambda: seen.append("handler"))
    j = Job(name="h-job", job_type=JobType.CUSTOM, handler="h")

    def on_started(ex: JobExecution) -> None:
        seen.append(("started", ex.status, ex.attempt))

    with ThreadPoolExecutor(max_workers=1) as pool:
        result = execute_attempt(j, registry, pool, attempt_number=1, on_started=on_started)

    assert seen == [("started", JobStatus.RUNNING, 1), "handler"]
    assert result.execution.status is JobStatus.SUCCESS


def _due_scheduler(on_start) -> tuple[Scheduler, Job]:  # type: ignore[no-untyped-def]
    registry = TaskRegistry()
    registry.register("ok", lambda: "ok")
    j = Job(name="ok-job", job_type=JobType.CUSTOM, handler="ok")
    scheduler = Scheduler(registry, on_start=on_start)
    scheduler.add(
        j, Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    )
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)
    return scheduler, j


def test_scheduler_forwards_on_start() -> None:
    started: list[JobStatus] = []
    scheduler, _ = _due_scheduler(lambda ex: started.append(ex.status))
    assert len(scheduler.tick()) == 1
    assert started == [JobStatus.RUNNING]


def test_on_start_failure_skips_trigger_but_keeps_schedule() -> None:
    def boom(ex: JobExecution) -> None:
        raise RuntimeError("db down")

    scheduler, _ = _due_scheduler(boom)
    assert scheduler.tick() == []
    assert scheduler.peek_next_run_at() is not None
    assert scheduler.peek_next_run_at() > datetime.now(UTC)

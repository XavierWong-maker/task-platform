from task_platform.bootstrap import build_jobs, build_registry
from task_platform.domain.enums import JobStatus
from task_platform.executor import cancel_before_start, execute


def test_execute_success_path() -> None:
    jobs = build_jobs()
    registry = build_registry()
    job = jobs["demo-success"]

    execution = execute(job, registry)

    assert execution.status == JobStatus.SUCCESS
    assert execution.attempt == 1
    assert execution.error_message is None
    assert execution.started_at is not None
    assert execution.finished_at is not None
    assert execution.duration_seconds is not None


def test_execute_failure_path_still_records_state() -> None:
    jobs = build_jobs()
    registry = build_registry()
    job = jobs["demo-failure"]

    execution = execute(job, registry)

    assert execution.status is JobStatus.FAILED
    assert execution.attempt == 1
    assert execution.error_message is not None
    assert "demo_failure" in execution.error_message
    # finally 块保证即使失败也记录了 finished_at
    assert execution.finished_at is not None


def test_cancel_before_start() -> None:
    jobs = build_jobs()
    job = jobs["demo-slow"]

    execution = cancel_before_start(job)

    assert execution.status is JobStatus.CANCELED
    assert execution.finished_at is not None

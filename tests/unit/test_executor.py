from concurrent.futures import ThreadPoolExecutor

from task_platform.bootstrap import build_jobs, build_registry
from task_platform.domain.enums import JobStatus
from task_platform.executor import cancel_before_start, execute_attempt


def test_execute_success_path() -> None:
    registry = build_registry()
    jobs = build_jobs(registry)
    job = jobs["demo-success"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        result = execute_attempt(job, registry, pool, attempt_number=1)
    execution = result.execution

    assert execution.status == JobStatus.SUCCESS
    assert execution.attempt == 1
    assert execution.error_message is None
    assert execution.started_at is not None
    assert execution.finished_at is not None
    assert execution.duration_seconds is not None


def test_execute_failure_path_still_records_state() -> None:
    registry = build_registry()
    jobs = build_jobs(registry)
    job = jobs["demo-failure"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        result = execute_attempt(job, registry, pool, attempt_number=1)
    execution = result.execution

    # demo-failure 用默认 RetryPolicy（max_attempts=3），attempt=1 时还应该允许重试，
    # 所以第一次尝试失败后状态是 RETRYING，不是 FAILED —— 这是阶段 3 引入重试语义后的行为变化
    assert execution.status is JobStatus.RETRYING
    assert result.should_retry is True
    assert execution.attempt == 1
    assert execution.error_message is not None
    assert "demo_failure" in execution.error_message
    assert execution.finished_at is not None


def test_execute_failure_final_attempt_marks_failed() -> None:
    """验证用完所有重试次数后，最终状态是 FAILED"""
    registry = build_registry()
    jobs = build_jobs(registry)
    job = jobs["demo-failure"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        # demo-failure 默认 max_attempts=3，第 3 次尝试应该不再允许重试
        result = execute_attempt(job, registry, pool, attempt_number=3)
    execution = result.execution

    assert execution.status is JobStatus.FAILED
    assert result.should_retry is False


def test_cancel_before_start() -> None:
    registry = build_registry()
    jobs = build_jobs(registry)
    job = jobs["demo-slow"]

    execution = cancel_before_start(job)

    assert execution.status is JobStatus.CANCELED
    assert execution.finished_at is not None

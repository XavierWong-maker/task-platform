from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from task_platform.domain.cancel import CancelToken
from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job
from task_platform.executor import execute_attempt
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry


def test_cooperative_handler_ends_canceled_without_retry() -> None:
    registry = TaskRegistry()
    started = threading.Event()

    @job("coop", timeout_seconds=30.0, max_attempts=3, registry=registry)
    def coop(ctx: JobContext) -> None:
        started.set()
        while True:
            ctx.check_cancelled()
            ctx.cancel.wait(0.01)

    j = Job(name="coop", job_type=JobType.CUSTOM, handler="coop")
    token = CancelToken()
    threading.Thread(target=lambda: (started.wait(2), token.cancel()), daemon=True).start()

    with ThreadPoolExecutor(max_workers=1) as pool:
        result = execute_attempt(j, registry, pool, attempt_number=1, cancel_token=token)

    assert result.execution.status is JobStatus.CANCELED
    assert result.should_retry is False  # 还有 2 次额度，但取消不重试
    assert result.execution.finished_at is not None


def test_uncooperative_handler_is_abandoned_after_grace() -> None:
    registry = TaskRegistry()
    registry.register("sleepy", lambda: time.sleep(0.6))
    j = Job(name="sleepy", job_type=JobType.CUSTOM, handler="sleepy", timeout_seconds=30.0)
    token = CancelToken()
    threading.Timer(0.05, token.cancel).start()

    with ThreadPoolExecutor(max_workers=1) as pool:
        start = time.monotonic()
        result = execute_attempt(
            j, registry, pool, attempt_number=1, cancel_token=token, cancel_grace_seconds=0.05
        )
        elapsed = time.monotonic() - start

    assert result.execution.status is JobStatus.CANCELED
    assert elapsed < 0.4  # 没有等 handler 睡满 0.6s
    assert "grace" in (result.execution.error_message or "")


def test_timeout_fires_token_so_cooperative_handler_stops() -> None:
    registry = TaskRegistry()
    stopped = threading.Event()

    @job("loop", timeout_seconds=0.1, max_attempts=1, registry=registry)
    def loop(ctx: JobContext) -> None:
        while not ctx.cancel.is_cancelled:
            ctx.cancel.wait(0.01)
        stopped.set()

    j = Job(name="loop", job_type=JobType.CUSTOM, handler="loop")
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = execute_attempt(j, registry, pool, attempt_number=1)

    assert result.execution.status is JobStatus.FAILED
    assert "超过" in (result.execution.error_message or "")
    assert stopped.wait(2.0)  # 超时后线程真的停了，而不是留在后台空转


def test_handler_raising_timeout_error_is_not_mistaken_for_job_timeout() -> None:
    registry = TaskRegistry()

    def raises_timeout() -> None:
        raise TimeoutError("upstream timed out")

    registry.register("t", raises_timeout)
    j = Job(name="t", job_type=JobType.CUSTOM, handler="t", timeout_seconds=30.0)
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = execute_attempt(j, registry, pool, attempt_number=3)

    assert result.execution.status is JobStatus.FAILED
    assert result.execution.error_message == "upstream timed out"

from __future__ import annotations

import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.cancellation import CancelOutcome, CancelWatcher, request_cancel
from task_platform.cli.main import main
from task_platform.domain.cancel import ActiveRuns, CancelToken
from task_platform.domain.enums import JobStatus, JobType, ScheduleType
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = create_db_engine(f"sqlite:///{tmp_path / 'cancel.db'}")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _seed(factory: sessionmaker[Session], status: JobStatus, run_id: str = "run_1") -> Job:
    j = Job(name="c-job", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(j)
        ExecutionRepository(s).add(
            JobExecution(job_id=j.id, run_id=run_id, attempt=1, status=status)
        )
    return j


def test_request_cancel_outcomes(factory: sessionmaker[Session]) -> None:
    _seed(factory, JobStatus.RUNNING, "r_running")
    _seed_more = Job(name="c-job-2", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(_seed_more)
        repo = ExecutionRepository(s)
        for run_id, status in (
            ("r_recover", JobStatus.RECOVERABLE),
            ("r_done", JobStatus.SUCCESS),
            ("r_retry", JobStatus.RETRYING),
        ):
            repo.add(JobExecution(job_id=_seed_more.id, run_id=run_id, attempt=1, status=status))

    with session_scope(factory) as s:
        assert request_cancel(s, "r_running") is CancelOutcome.REQUESTED
        assert request_cancel(s, "r_recover") is CancelOutcome.CANCELED
        assert request_cancel(s, "r_done") is CancelOutcome.ALREADY_FINISHED
        assert request_cancel(s, "r_retry") is CancelOutcome.NOT_CANCELLABLE
        assert request_cancel(s, "nope") is CancelOutcome.NOT_FOUND

    with session_scope(factory) as s:
        repo = ExecutionRepository(s)
        running = repo.get_attempt("r_running", 1)
        recovered = repo.get_attempt("r_recover", 1)
        assert running is not None and running.cancel_requested is True
        assert running.status is JobStatus.RUNNING  # 只置标志，状态由执行方推进
        assert recovered is not None and recovered.status is JobStatus.CANCELED


def test_watcher_only_fires_tokens_that_were_requested(factory: sessionmaker[Session]) -> None:
    _seed(factory, JobStatus.RUNNING, "r_a")
    active = ActiveRuns()
    token_a, token_b = CancelToken(), CancelToken()
    active.register("r_a", token_a)
    active.register("r_b", token_b)  # 库里没有这条
    watcher = CancelWatcher(factory, active)

    assert watcher.poll_once() == []  # 还没人请求取消
    with session_scope(factory) as s:
        request_cancel(s, "r_a")

    assert watcher.poll_once() == ["r_a"]
    assert token_a.is_cancelled and not token_b.is_cancelled


def test_cancel_running_job_end_to_end(factory: sessionmaker[Session]) -> None:
    registry = TaskRegistry()
    started = threading.Event()

    @job("coop", timeout_seconds=30.0, max_attempts=3, registry=registry)
    def coop(ctx: JobContext) -> None:
        started.set()
        while True:
            ctx.check_cancelled()
            ctx.cancel.wait(0.02)

    j = Job(name="coop-job", job_type=JobType.CUSTOM, handler="coop")
    with session_scope(factory) as s:
        JobRepository(s).add(j)

    def on_start(ex: JobExecution) -> None:
        with session_scope(factory) as s:
            ExecutionRepository(s).add(ex)

    scheduler = Scheduler(registry, on_start=on_start)
    scheduler.add(
        j, Schedule(job_id=j.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=3600)
    )
    scheduler._heap[0].next_run_at = datetime.now(UTC) - timedelta(seconds=1)

    box: list[JobExecution] = []
    ticker = threading.Thread(target=lambda: box.extend(scheduler.tick()))
    ticker.start()
    assert started.wait(5)

    [run_id] = scheduler.active_runs.active_ids()
    with session_scope(factory) as s:
        assert request_cancel(s, run_id) is CancelOutcome.REQUESTED
    assert CancelWatcher(factory, scheduler.active_runs).poll_once() == [run_id]

    ticker.join(5)
    assert not ticker.is_alive()
    assert box[0].status is JobStatus.CANCELED
    assert scheduler.active_runs.active_ids() == []  # 令牌已注销
    scheduler.stop()


def test_resume_and_cancel_run_via_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = f"sqlite:///{tmp_path / 'cli_cancel.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    assert main(["list"]) == 0

    engine = create_db_engine(url)
    factory = make_session_factory(engine)
    with session_scope(factory) as s:
        j = JobRepository(s).get_by_name("demo-success")
        assert j is not None
        repo = ExecutionRepository(s)
        repo.add(
            JobExecution(job_id=j.id, run_id="r_resume", attempt=1, status=JobStatus.RECOVERABLE)
        )
        repo.add(
            JobExecution(job_id=j.id, run_id="r_cancel", attempt=1, status=JobStatus.RECOVERABLE)
        )
        repo.add(JobExecution(job_id=j.id, run_id="r_done", attempt=1, status=JobStatus.SUCCESS))

    assert main(["resume", "r_resume"]) == 0
    with session_scope(factory) as s:
        attempts = ExecutionRepository(s).list_by_run("r_resume")
    assert [(a.attempt, a.status) for a in attempts] == [
        (1, JobStatus.RECOVERABLE),
        (2, JobStatus.SUCCESS),
    ]

    assert main(["resume", "r_done"]) == 1  # 非 RECOVERABLE 不能恢复
    assert main(["resume", "nope"]) == 1

    assert main(["cancel-run", "r_cancel"]) == 0
    assert main(["resume", "r_cancel"]) == 1  # 已被取消，不能再恢复
    assert main(["cancel-run", "r_done"]) == 1
    engine.dispose()

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.cli.main import main
from task_platform.domain.enums import JobStatus, JobType
from task_platform.domain.models import Job, JobExecution
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    IdempotencyRepository,
    JobRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.recovery import INTERRUPTED_MESSAGE, recover_interrupted


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = create_db_engine("sqlite://")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _seed(factory: sessionmaker[Session]) -> Job:
    job = Job(name="rec-job", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        ex = ExecutionRepository(s)
        ex.add(
            JobExecution(job_id=job.id, run_id="run_crashed", attempt=1, status=JobStatus.RUNNING)
        )
        ex.add(JobExecution(job_id=job.id, run_id="run_done", attempt=1, status=JobStatus.SUCCESS))
        keys = IdempotencyRepository(s)
        keys.try_claim("k_crashed", job.id, "run_crashed")
        keys.try_claim("k_done", job.id, "run_done")
        keys.try_claim("k_orphan", job.id, "run_never_started")
    return job


def test_recover_marks_running_and_releases_only_orphan_keys(
    factory: sessionmaker[Session],
) -> None:
    _seed(factory)
    with session_scope(factory) as s:
        report = recover_interrupted(s)

    assert report.recovered == ["run_crashed#1"]
    assert report.released_keys == ["k_orphan"]

    with session_scope(factory) as s:
        crashed = ExecutionRepository(s).get_attempt("run_crashed", 1)
        done = ExecutionRepository(s).get_attempt("run_done", 1)
        keys = IdempotencyRepository(s)
        assert crashed is not None and crashed.status is JobStatus.RECOVERABLE
        assert crashed.finished_at is not None
        assert crashed.error_message == INTERRUPTED_MESSAGE
        assert done is not None and done.status is JobStatus.SUCCESS  # 已完成的不动
        assert keys.get_run_id("k_crashed") == "run_crashed"  # 恢复前不允许重复写入
        assert keys.get_run_id("k_done") == "run_done"
        assert keys.get_run_id("k_orphan") is None


def test_recover_is_idempotent(factory: sessionmaker[Session]) -> None:
    _seed(factory)
    with session_scope(factory) as s:
        recover_interrupted(s)
    with session_scope(factory) as s:
        report = recover_interrupted(s)
    assert report.recovered == [] and report.released_keys == []


def test_recover_command_via_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = f"sqlite:///{tmp_path / 'recover.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    assert main(["list"]) == 0  # 建库并同步 demo 任务

    engine = create_db_engine(url)
    factory = make_session_factory(engine)
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-success")
        assert job is not None
        ExecutionRepository(s).add(
            JobExecution(job_id=job.id, run_id="run_x", attempt=1, status=JobStatus.RUNNING)
        )

    assert main(["recover"]) == 0

    with session_scope(factory) as s:
        ex = ExecutionRepository(s).get_attempt("run_x", 1)
    assert ex is not None and ex.status is JobStatus.RECOVERABLE
    engine.dispose()


def test_recover_marks_only_dangling_retries(factory: sessionmaker[Session]) -> None:
    job = Job(name="retry-job", job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(job)
        repo = ExecutionRepository(s)
        # 悬空：只有一次尝试且停在 RETRYING
        repo.add(
            JobExecution(job_id=job.id, run_id="r_dangling", attempt=1, status=JobStatus.RETRYING)
        )
        # 已被接替：attempt 1 RETRYING，attempt 2 SUCCESS -> 正常历史，不动
        repo.add(
            JobExecution(job_id=job.id, run_id="r_chain", attempt=1, status=JobStatus.RETRYING)
        )
        repo.add(JobExecution(job_id=job.id, run_id="r_chain", attempt=2, status=JobStatus.SUCCESS))
        # 多次重试：只有最后一次 RETRYING 是悬空的
        repo.add(JobExecution(job_id=job.id, run_id="r_two", attempt=1, status=JobStatus.RETRYING))
        repo.add(JobExecution(job_id=job.id, run_id="r_two", attempt=2, status=JobStatus.RETRYING))

    with session_scope(factory) as s:
        report = recover_interrupted(s)

    assert report.recovered_retries == ["r_dangling#1", "r_two#2"]
    assert report.recovered == []

    with session_scope(factory) as s:
        repo = ExecutionRepository(s)
        status = {
            (e.run_id, e.attempt): e.status
            for run in ("r_dangling", "r_chain", "r_two")
            for e in repo.list_by_run(run)
        }
    assert status[("r_dangling", 1)] is JobStatus.RECOVERABLE
    assert status[("r_chain", 1)] is JobStatus.RETRYING  # 历史不动
    assert status[("r_two", 1)] is JobStatus.RETRYING
    assert status[("r_two", 2)] is JobStatus.RECOVERABLE


def test_dangling_retry_can_be_recovered_and_resumed_via_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = f"sqlite:///{tmp_path / 'retry_resume.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    assert main(["list"]) == 0

    engine = create_db_engine(url)
    factory = make_session_factory(engine)
    with session_scope(factory) as s:
        j = JobRepository(s).get_by_name("demo-success")
        assert j is not None
        ExecutionRepository(s).add(
            JobExecution(
                job_id=j.id,
                run_id="r_retry",
                attempt=1,
                status=JobStatus.RETRYING,
                error_message="boom",
            )
        )

    assert main(["resume", "r_retry"]) == 1  # 恢复前不能 resume
    assert main(["recover"]) == 0
    assert main(["resume", "r_retry"]) == 0

    with session_scope(factory) as s:
        attempts = ExecutionRepository(s).list_by_run("r_retry")
    assert [(a.attempt, a.status) for a in attempts] == [
        (1, JobStatus.RECOVERABLE),
        (2, JobStatus.SUCCESS),
    ]
    assert attempts[0].error_message == "boom"  # 上次失败原因保留
    engine.dispose()

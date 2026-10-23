from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobType
from task_platform.domain.models import Job
from task_platform.infrastructure.db.repositories import IdempotencyRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = create_db_engine(f"sqlite:///{tmp_path / 'idem.db'}")
    init_db(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


def _add_job(factory: sessionmaker[Session], name: str = "job-1") -> Job:
    job = Job(name=name, job_type=JobType.CUSTOM, handler="demo.ok")
    with session_scope(factory) as s:
        JobRepository(s).add(job)
    return job


def test_first_claim_wins_second_is_rejected(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    with session_scope(factory) as s:
        repo = IdempotencyRepository(s)
        assert repo.try_claim("k1", job.id, "run_a") is True
        assert repo.try_claim("k1", job.id, "run_b") is False

    with session_scope(factory) as s:
        assert IdempotencyRepository(s).get_run_id("k1") == "run_a"


def test_different_keys_are_independent(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    with session_scope(factory) as s:
        repo = IdempotencyRepository(s)
        assert repo.try_claim("k1", job.id, "run_a") is True
        assert repo.try_claim("k2", job.id, "run_b") is True


def test_get_unknown_key_returns_none(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        assert IdempotencyRepository(s).get_run_id("nope") is None


def test_release_allows_reclaim(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    with session_scope(factory) as s:
        repo = IdempotencyRepository(s)
        repo.try_claim("k1", job.id, "run_a")
        assert repo.release("k1") is True
        assert repo.release("k1") is False
        assert repo.try_claim("k1", job.id, "run_b") is True


def test_claim_rolls_back_with_transaction(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    with pytest.raises(RuntimeError):
        with session_scope(factory) as s:
            IdempotencyRepository(s).try_claim("k1", job.id, "run_a")
            raise RuntimeError("boom")

    with session_scope(factory) as s:
        assert IdempotencyRepository(s).get_run_id("k1") is None


def test_deleting_job_cascades_to_keys(factory: sessionmaker[Session]) -> None:
    job = _add_job(factory)
    with session_scope(factory) as s:
        IdempotencyRepository(s).try_claim("k1", job.id, "run_a")
    with session_scope(factory) as s:
        JobRepository(s).delete(job.id)
    with session_scope(factory) as s:
        assert IdempotencyRepository(s).get_run_id("k1") is None


def test_concurrent_claims_have_exactly_one_winner(factory: sessionmaker[Session]) -> None:
    """竞态复现：N 个线程同时认领同一个键，数据库保证只有一个成功"""
    job = _add_job(factory)
    n = 8
    barrier = threading.Barrier(n)
    results: list[bool] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        with session_scope(factory) as s:
            won = IdempotencyRepository(s).try_claim("race", job.id, f"run_{i}")
        with lock:
            results.append(won)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == n
    assert results.count(True) == 1

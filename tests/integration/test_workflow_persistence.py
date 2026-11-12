from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import JobStatus, JobType, NodeStatus, WorkflowStatus
from task_platform.domain.models import Job, RetryPolicy
from task_platform.domain.workflow import TaskNode, WorkflowDefinition
from task_platform.domain.workflow_run import new_workflow_run
from task_platform.faults import FlakyState
from task_platform.infrastructure.db.models import WorkflowNodeRunRecord
from task_platform.infrastructure.db.repositories import JobLogRepository, JobRepository
from task_platform.infrastructure.db.session import (
    create_db_engine,
    init_db,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.db.workflow_repositories import (
    WorkflowRepository,
    WorkflowRunRepository,
)
from task_platform.infrastructure.logging.collector import LogCollector, collecting_logs
from task_platform.jobs.context import JobContext
from task_platform.registry import TaskRegistry
from task_platform.workflow_engine import WorkflowEngine
from task_platform.workflow_service import run_workflow

N = NodeStatus


@pytest.fixture
def factory(tmp_path: Path) -> Iterator[sessionmaker[Session]]:
    # 引擎会在多个线程里写库，必须用文件库（内存库每个线程各是一份）
    engine = create_db_engine(f"sqlite:///{tmp_path / 'workflow.db'}")
    init_db(engine)
    yield make_session_factory(engine)
    engine.dispose()


@pytest.fixture
def collector() -> Iterator[LogCollector]:
    with collecting_logs() as c:
        yield c


def _n(name: str, *deps: str) -> TaskNode:
    return TaskNode(name=name, job=name, depends_on=tuple(deps))


def _wf(*nodes: TaskNode, name: str = "wf") -> WorkflowDefinition:
    return WorkflowDefinition(name, tuple(nodes))


def _etl() -> WorkflowDefinition:
    return _wf(
        _n("extract"),
        _n("clean", "extract"),
        _n("stats", "extract"),
        _n("report", "clean", "stats"),
        _n("audit", "clean"),
    )


def _setup(
    factory: sessionmaker[Session],
    handlers: dict[str, Callable[..., object]],
    retry: RetryPolicy | None = None,
) -> WorkflowEngine:
    """注册 handler，并把对应的 Job 写进数据库（job_executions 有指向 jobs 的外键）"""
    registry = TaskRegistry()
    jobs: dict[str, Job] = {}
    with session_scope(factory) as s:
        repo = JobRepository(s)
        for name, fn in handlers.items():
            registry.register(name, fn)
            job = Job(
                name=name,
                job_type=JobType.CUSTOM,
                handler=name,
                retry_policy=retry or RetryPolicy(max_attempts=1),
            )
            repo.add(job)
            jobs[name] = job
    return WorkflowEngine(registry, jobs)


def _ok() -> str:
    return "ok"


def _boom() -> None:
    raise RuntimeError("boom")


# ---------------- 定义 ----------------


def test_definition_roundtrip_and_replace(factory: sessionmaker[Session]) -> None:
    first = _wf(_n("a"), _n("b", "a"))
    with session_scope(factory) as s:
        WorkflowRepository(s).save(first)
    with session_scope(factory) as s:
        assert WorkflowRepository(s).get("wf") == first
        assert WorkflowRepository(s).get("nope") is None

    second = _wf(_n("x"))
    with session_scope(factory) as s:
        WorkflowRepository(s).save(second)
    with session_scope(factory) as s:
        assert WorkflowRepository(s).get("wf") == second
        assert WorkflowRepository(s).list_names() == ["wf"]


def test_list_names_sorted(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        repo = WorkflowRepository(s)
        repo.save(_wf(_n("a"), name="zeta"))
        repo.save(_wf(_n("a"), name="alpha"))
    with session_scope(factory) as s:
        assert WorkflowRepository(s).list_names() == ["alpha", "zeta"]


# ---------------- 运行历史 ----------------


def test_successful_run_is_persisted_with_attempts_and_logs(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    definition = _wf(_n("a"), _n("b", "a"))
    engine = _setup(factory, {"a": _ok, "b": _ok})

    run = run_workflow(engine, definition, factory, collector)

    with session_scope(factory) as s:
        loaded = WorkflowRunRepository(s).get(run.run_id)
        logs = JobLogRepository(s).list_by_run(run.nodes["a"].run_id or "").items
    assert loaded is not None
    assert loaded.status is WorkflowStatus.SUCCESS
    assert loaded.statuses() == run.statuses()
    assert (loaded.started_at, loaded.finished_at) == (run.started_at, run.finished_at)
    assert [e.status for e in loaded.nodes["a"].attempts] == [JobStatus.SUCCESS]
    assert any("job attempt started" in log.message for log in logs)


def test_nodes_are_loaded_in_definition_order(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    definition = _etl()
    engine = _setup(factory, {n.name: _ok for n in definition.nodes})

    run = run_workflow(engine, definition, factory, collector)

    with session_scope(factory) as s:
        loaded = WorkflowRunRepository(s).get(run.run_id)
    assert loaded is not None
    assert list(loaded.nodes) == [n.name for n in definition.nodes]


def test_failure_and_skips_are_persisted(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    engine = _setup(
        factory,
        {"extract": _ok, "clean": _boom, "stats": _ok, "report": _ok, "audit": _ok},
    )

    run = run_workflow(engine, _etl(), factory, collector)

    with session_scope(factory) as s:
        loaded = WorkflowRunRepository(s).get(run.run_id)
        partial = WorkflowRunRepository(s).list_paginated(status=WorkflowStatus.PARTIAL_FAILED)
        success = WorkflowRunRepository(s).list_paginated(status=WorkflowStatus.SUCCESS)
    assert loaded is not None
    assert loaded.statuses() == {
        "extract": N.SUCCESS,
        "clean": N.FAILED,
        "stats": N.SUCCESS,
        "report": N.SKIPPED,
        "audit": N.SKIPPED,
    }
    assert loaded.nodes["clean"].error_message == "boom"
    assert loaded.nodes["report"].attempts == []
    assert loaded.nodes["report"].error_message is not None
    assert [r.run_id for r in partial.items] == [run.run_id]
    assert success.total == 0


def test_retry_attempts_and_their_logs_are_attributed_correctly(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    flaky = FlakyState()

    def handler(ctx: JobContext) -> str:
        if flaky.should_fail(ctx.execution_id, fail_times=1):
            raise RuntimeError("injected")
        return "ok"

    engine = _setup(
        factory, {"a": handler}, retry=RetryPolicy(max_attempts=3, base_delay_seconds=0.01)
    )

    run = run_workflow(engine, _wf(_n("a")), factory, collector)

    job_run_id = run.nodes["a"].run_id or ""
    with session_scope(factory) as s:
        loaded = WorkflowRunRepository(s).get(run.run_id)
        first = [m.message for m in JobLogRepository(s).list_by_run(job_run_id, attempt=1).items]
        second = [m.message for m in JobLogRepository(s).list_by_run(job_run_id, attempt=2).items]
    assert loaded is not None
    assert [(e.attempt, e.status) for e in loaded.nodes["a"].attempts] == [
        (1, JobStatus.RETRYING),
        (2, JobStatus.SUCCESS),
    ]
    assert any("will retry" in m for m in first)
    assert not any("will retry" in m for m in second)
    assert any("job succeeded" in m for m in second)


def test_run_keeps_definition_snapshot_after_definition_changes(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    original = _wf(_n("a"), _n("b", "a"))
    engine = _setup(factory, {"a": _ok, "b": _ok})
    run = run_workflow(engine, original, factory, collector)

    changed = _wf(_n("a"))
    with session_scope(factory) as s:
        WorkflowRepository(s).save(changed)

    with session_scope(factory) as s:
        assert WorkflowRunRepository(s).get_definition(run.run_id) == original
        assert WorkflowRepository(s).get("wf") == changed


def test_list_paginated_filters_and_orders_newest_first(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    engine = _setup(factory, {"a": _ok})
    runs = [run_workflow(engine, _wf(_n("a")), factory, collector) for _ in range(3)]
    other = run_workflow(engine, _wf(_n("a"), name="other"), factory, collector)

    with session_scope(factory) as s:
        repo = WorkflowRunRepository(s)
        page1 = repo.list_paginated(workflow="wf", page=1, page_size=2)
        page2 = repo.list_paginated(workflow="wf", page=2, page_size=2)
        others = repo.list_paginated(workflow="other")
        everything = repo.list_paginated()

    assert [r.run_id for r in page1.items] == [runs[2].run_id, runs[1].run_id]
    assert [r.run_id for r in page2.items] == [runs[0].run_id]
    assert (page1.total, page1.total_pages) == (3, 2)
    assert [r.run_id for r in others.items] == [other.run_id]
    assert everything.total == 4
    assert page1.items[0].status is WorkflowStatus.SUCCESS


def test_deleting_workflow_cascades_to_runs_and_node_runs(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    engine = _setup(factory, {"a": _ok})
    run = run_workflow(engine, _wf(_n("a")), factory, collector)

    with session_scope(factory) as s:
        assert WorkflowRepository(s).delete("wf") is True
        assert WorkflowRepository(s).delete("wf") is False

    with session_scope(factory) as s:
        assert WorkflowRunRepository(s).get(run.run_id) is None
        assert s.scalar(select(func.count()).select_from(WorkflowNodeRunRecord)) == 0


def test_save_requires_started_run(factory: sessionmaker[Session]) -> None:
    definition = _wf(_n("a"))
    with session_scope(factory) as s:
        WorkflowRepository(s).save(definition)
    with pytest.raises(ValueError), session_scope(factory) as s:
        WorkflowRunRepository(s).save(new_workflow_run(definition), definition)


def test_get_unknown_run_returns_none(factory: sessionmaker[Session]) -> None:
    with session_scope(factory) as s:
        assert WorkflowRunRepository(s).get("wfrun_nope") is None
        assert WorkflowRunRepository(s).get_definition("wfrun_nope") is None

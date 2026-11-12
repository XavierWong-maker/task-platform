from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.enums import WorkflowStatus
from task_platform.domain.workflow_replay import ReplayError
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
from task_platform.workflow_service import (
    RunNotFoundError,
    rerun_node,
    retry_workflow,
    run_workflow,
)
from tests.integration.test_workflow_persistence import _etl, _n, _ok, _setup, _wf


@pytest.fixture
def factory(tmp_path: Path) -> Iterator[sessionmaker[Session]]:
    engine = create_db_engine(f"sqlite:///{tmp_path / 'replay.db'}")
    init_db(engine)
    yield make_session_factory(engine)
    engine.dispose()


@pytest.fixture
def collector() -> Iterator[LogCollector]:
    with collecting_logs() as c:
        yield c


def _handlers(calls: list[str], state: dict[str, bool]) -> dict[str, object]:
    def make(name: str, *, flaky: bool = False):  # type: ignore[no-untyped-def]
        def fn() -> str:
            calls.append(name)
            if flaky and state["fail"]:
                raise RuntimeError("boom")
            return name

        return fn

    return {
        "extract": make("extract"),
        "clean": make("clean", flaky=True),
        "stats": make("stats"),
        "report": make("report"),
        "audit": make("audit"),
    }


def test_retry_persists_child_run_with_reused_nodes(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    state = {"fail": True}
    engine = _setup(factory, _handlers([], state))  # type: ignore[arg-type]
    first = run_workflow(engine, _etl(), factory, collector)
    state["fail"] = False

    second = retry_workflow(engine, first.run_id, factory, collector)

    with session_scope(factory) as s:
        repo = WorkflowRunRepository(s)
        loaded_first = repo.get(first.run_id)
        loaded_second = repo.get(second.run_id)
        history = repo.list_paginated(workflow="wf")
    assert loaded_first is not None and loaded_second is not None
    assert loaded_first.status is WorkflowStatus.PARTIAL_FAILED  # 来源运行不变
    assert loaded_second.status is WorkflowStatus.SUCCESS
    assert loaded_second.parent_run_id == first.run_id
    assert {n for n, r in loaded_second.nodes.items() if r.reused} == {"extract", "stats"}
    assert loaded_second.nodes["extract"].attempts == loaded_first.nodes["extract"].attempts
    assert not loaded_second.nodes["clean"].reused
    assert [r.parent_run_id for r in history.items] == [first.run_id, None]


def test_replay_uses_definition_snapshot_and_keeps_current_definition(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    state = {"fail": True}
    engine = _setup(factory, _handlers([], state))  # type: ignore[arg-type]
    first = run_workflow(engine, _etl(), factory, collector)
    changed = _wf(_n("extract"))
    with session_scope(factory) as s:
        WorkflowRepository(s).save(changed)
    state["fail"] = False

    second = retry_workflow(engine, first.run_id, factory, collector)

    assert second.status is WorkflowStatus.SUCCESS
    assert list(second.nodes) == [n.name for n in _etl().nodes]  # 用的是旧快照
    with session_scope(factory) as s:
        assert WorkflowRepository(s).get("wf") == changed  # 当前定义没有被覆盖


def test_rerun_node_only_executes_node_and_downstream(
    factory: sessionmaker[Session], collector: LogCollector
) -> None:
    calls: list[str] = []
    engine = _setup(factory, _handlers(calls, {"fail": False}))  # type: ignore[arg-type]
    first = run_workflow(engine, _etl(), factory, collector)
    calls.clear()

    second = rerun_node(engine, first.run_id, "clean", factory, collector)

    assert sorted(calls) == ["audit", "clean", "report"]
    with session_scope(factory) as s:
        loaded = WorkflowRunRepository(s).get(second.run_id)
    assert loaded is not None
    assert loaded.parent_run_id == first.run_id
    assert {n for n, r in loaded.nodes.items() if r.reused} == {"extract", "stats"}


def test_replay_errors(factory: sessionmaker[Session], collector: LogCollector) -> None:
    engine = _setup(factory, {"a": _ok})
    run = run_workflow(engine, _wf(_n("a")), factory, collector)

    with pytest.raises(RunNotFoundError):
        retry_workflow(engine, "wfrun_nope", factory, collector)
    with pytest.raises(RunNotFoundError):
        rerun_node(engine, "wfrun_nope", "a", factory, collector)
    with pytest.raises(ReplayError):  # 已全部成功
        retry_workflow(engine, run.run_id, factory, collector)
    with pytest.raises(ReplayError):  # 节点不存在
        rerun_node(engine, run.run_id, "ghost", factory, collector)

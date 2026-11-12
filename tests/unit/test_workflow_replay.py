from __future__ import annotations

import pytest

from task_platform.domain.enums import NodeStatus, WorkflowStatus
from task_platform.domain.workflow import WorkflowValidationError
from task_platform.domain.workflow_replay import (
    ReplayError,
    carry_over_for_rerun,
    carry_over_for_retry,
)
from task_platform.domain.workflow_run import NodeRun, new_workflow_run
from tests.unit.test_workflow_engine import _engine, _etl, _rec

N = NodeStatus


def _handlers(calls: list[str], state: dict[str, bool]) -> dict[str, object]:
    def clean() -> str:
        calls.append("clean")
        if state["fail"]:
            raise RuntimeError("clean failed")
        return "clean"

    return {
        "extract": _rec(calls, "extract"),
        "clean": clean,
        "stats": _rec(calls, "stats"),
        "report": _rec(calls, "report"),
        "audit": _rec(calls, "audit"),
    }


def _reused(run) -> set[str]:  # type: ignore[no-untyped-def]
    return {name for name, node in run.nodes.items() if node.reused}


# ---------------- new_workflow_run ----------------


def test_carry_over_copies_finished_nodes_and_marks_them_reused() -> None:
    previous = NodeRun("extract", status=N.SUCCESS, error_message=None)
    run = new_workflow_run(_etl(), parent_run_id="wfrun_x", carry_over={"extract": previous})

    assert run.parent_run_id == "wfrun_x"
    assert run.nodes["extract"].status is N.SUCCESS and run.nodes["extract"].reused
    assert run.nodes["clean"].status is N.PENDING and not run.nodes["clean"].reused


def test_carry_over_rejects_unknown_or_unfinished_nodes() -> None:
    with pytest.raises(WorkflowValidationError):
        new_workflow_run(_etl(), carry_over={"ghost": NodeRun("ghost", status=N.SUCCESS)})
    with pytest.raises(WorkflowValidationError):
        new_workflow_run(_etl(), carry_over={"extract": NodeRun("extract", status=N.RUNNING)})


# ---------------- 重试 ----------------


def test_retry_reuses_successful_nodes_and_reruns_the_rest() -> None:
    calls: list[str] = []
    state = {"fail": True}
    engine = _engine(_handlers(calls, state))  # type: ignore[arg-type]
    first = engine.run(_etl())
    assert first.status is WorkflowStatus.PARTIAL_FAILED
    calls.clear()
    state["fail"] = False

    second = engine.run(_etl(), carry_over=carry_over_for_retry(first), parent_run_id=first.run_id)

    assert second.status is WorkflowStatus.SUCCESS
    assert second.run_id != first.run_id and second.parent_run_id == first.run_id
    assert sorted(calls) == ["audit", "clean", "report"]  # extract / stats 没有再执行
    assert _reused(second) == {"extract", "stats"}
    assert second.nodes["extract"].run_id == first.nodes["extract"].run_id
    assert first.status is WorkflowStatus.PARTIAL_FAILED  # 来源运行不被改动


def test_retry_rejects_succeeded_or_unfinished_runs() -> None:
    engine = _engine(_handlers([], {"fail": False}))  # type: ignore[arg-type]
    succeeded = engine.run(_etl())
    with pytest.raises(ReplayError, match="全部成功"):
        carry_over_for_retry(succeeded)
    with pytest.raises(ReplayError, match="尚未结束"):
        carry_over_for_retry(new_workflow_run(_etl()))


# ---------------- 单节点重跑 ----------------


def test_rerun_resets_node_and_all_downstream_only() -> None:
    calls: list[str] = []
    engine = _engine(_handlers(calls, {"fail": False}))  # type: ignore[arg-type]
    first = engine.run(_etl())
    calls.clear()

    second = engine.run(
        _etl(),
        carry_over=carry_over_for_rerun(_etl(), first, "clean"),
        parent_run_id=first.run_id,
    )

    assert second.status is WorkflowStatus.SUCCESS
    assert sorted(calls) == ["audit", "clean", "report"]
    assert _reused(second) == {"extract", "stats"}


def test_rerun_keeps_failed_sibling_branch_untouched() -> None:
    calls: list[str] = []
    engine = _engine(_handlers(calls, {"fail": True}))  # type: ignore[arg-type]
    first = engine.run(_etl())  # clean 失败，report / audit 被跳过，stats 成功
    calls.clear()

    second = engine.run(
        _etl(),
        carry_over=carry_over_for_rerun(_etl(), first, "stats"),
        parent_run_id=first.run_id,
    )

    assert calls == ["stats"]  # clean 沿用了 FAILED，没有再执行
    assert second.statuses() == {
        "extract": N.SUCCESS,
        "clean": N.FAILED,
        "stats": N.SUCCESS,
        "report": N.SKIPPED,  # 上游 clean 仍是 FAILED
        "audit": N.SKIPPED,
    }
    assert second.status is WorkflowStatus.PARTIAL_FAILED


def test_rerun_requires_upstream_success_and_known_node() -> None:
    root_failed = _engine(
        {n.name: _rec([], n.name, fail=(n.name == "extract")) for n in _etl().nodes}
    ).run(_etl())

    with pytest.raises(ReplayError, match="上游"):
        carry_over_for_rerun(_etl(), root_failed, "clean")
    with pytest.raises(ReplayError, match="不存在"):
        carry_over_for_rerun(_etl(), root_failed, "ghost")
    with pytest.raises(ReplayError, match="尚未结束"):
        carry_over_for_rerun(_etl(), new_workflow_run(_etl()), "extract")

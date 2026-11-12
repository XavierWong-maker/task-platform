from __future__ import annotations

import pytest

from task_platform.domain.enums import NodeStatus as N
from task_platform.domain.enums import WorkflowStatus as W
from task_platform.domain.workflow import TaskNode, WorkflowDefinition
from task_platform.domain.workflow_state import (
    InvalidNodeTransitionError,
    can_transition_node,
    check_node_transition,
    derive_workflow_status,
    initial_statuses,
    nodes_to_skip,
    ready_nodes,
)


def _n(name: str, *deps: str) -> TaskNode:
    return TaskNode(name=name, job=f"job-{name}", depends_on=tuple(deps))


def _etl() -> WorkflowDefinition:
    """extract -> clean -> (stats, audit) ；report 依赖 clean 与 stats"""
    return WorkflowDefinition(
        "etl",
        (
            _n("extract"),
            _n("clean", "extract"),
            _n("stats", "extract"),
            _n("report", "clean", "stats"),
            _n("audit", "clean"),
        ),
    )


def _with(**overrides: N) -> dict[str, N]:
    statuses = initial_statuses(_etl())
    statuses.update(overrides)
    return statuses


# ---------------- 节点状态转换 ----------------


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (N.PENDING, N.RUNNING),
        (N.PENDING, N.SKIPPED),
        (N.PENDING, N.CANCELED),
        (N.RUNNING, N.SUCCESS),
        (N.RUNNING, N.FAILED),
        (N.RUNNING, N.CANCELED),
    ],
)
def test_allowed_transitions(current: N, target: N) -> None:
    assert can_transition_node(current, target) is True
    check_node_transition(current, target)


def test_terminal_nodes_cannot_transition() -> None:
    for terminal in (N.SUCCESS, N.FAILED, N.SKIPPED, N.CANCELED):
        assert terminal.is_terminal()
        for target in N:
            assert can_transition_node(terminal, target) is False


@pytest.mark.parametrize(
    ("current", "target"),
    [(N.PENDING, N.SUCCESS), (N.PENDING, N.FAILED), (N.RUNNING, N.SKIPPED), (N.RUNNING, N.PENDING)],
)
def test_illegal_transitions_raise(current: N, target: N) -> None:
    with pytest.raises(InvalidNodeTransitionError):
        check_node_transition(current, target)


# ---------------- ready_nodes ----------------


def test_initially_only_roots_are_ready() -> None:
    assert ready_nodes(_etl(), initial_statuses(_etl())) == ["extract"]


def test_children_become_ready_after_upstream_success() -> None:
    assert ready_nodes(_etl(), _with(extract=N.SUCCESS)) == ["clean", "stats"]


def test_node_waits_for_all_upstreams() -> None:
    statuses = _with(extract=N.SUCCESS, clean=N.SUCCESS, stats=N.RUNNING)
    assert ready_nodes(_etl(), statuses) == ["audit"]  # report 还在等 stats


def test_running_and_finished_nodes_are_not_ready() -> None:
    statuses = _with(extract=N.SUCCESS, clean=N.RUNNING, stats=N.SUCCESS)
    assert ready_nodes(_etl(), statuses) == []


def test_node_with_failed_upstream_is_not_ready() -> None:
    assert ready_nodes(_etl(), _with(extract=N.FAILED)) == []


# ---------------- 失败传播 ----------------


def test_failure_skips_all_transitive_downstream() -> None:
    assert nodes_to_skip(_etl(), _with(extract=N.FAILED)) == [
        "clean",
        "stats",
        "report",
        "audit",
    ]


def test_independent_branch_is_not_skipped() -> None:
    statuses = _with(extract=N.SUCCESS, clean=N.FAILED, stats=N.RUNNING)
    assert nodes_to_skip(_etl(), statuses) == ["report", "audit"]  # stats 不受影响


def test_canceled_upstream_also_skips_downstream() -> None:
    assert nodes_to_skip(_etl(), _with(extract=N.SUCCESS, clean=N.CANCELED)) == ["report", "audit"]


def test_skip_ignores_nodes_already_started_or_finished() -> None:
    statuses = _with(extract=N.SUCCESS, clean=N.FAILED, report=N.RUNNING, audit=N.SUCCESS)
    assert nodes_to_skip(_etl(), statuses) == []


def test_nothing_to_skip_when_all_healthy() -> None:
    assert nodes_to_skip(_etl(), _with(extract=N.SUCCESS, clean=N.RUNNING)) == []


def test_skipped_upstream_propagates_even_if_already_marked() -> None:
    statuses = _with(extract=N.FAILED, clean=N.SKIPPED, stats=N.SKIPPED)
    assert nodes_to_skip(_etl(), statuses) == ["report", "audit"]


def test_mismatched_status_keys_rejected() -> None:
    with pytest.raises(ValueError):
        ready_nodes(_etl(), {"extract": N.PENDING})
    with pytest.raises(ValueError):
        nodes_to_skip(_etl(), {"extract": N.PENDING})


# ---------------- 工作流状态推导 ----------------


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ([N.PENDING, N.PENDING], W.PENDING),
        ([N.SUCCESS, N.PENDING], W.RUNNING),
        ([N.RUNNING, N.PENDING], W.RUNNING),
        ([N.SUCCESS, N.RUNNING], W.RUNNING),
        ([N.SUCCESS, N.SUCCESS], W.SUCCESS),
        ([N.SUCCESS, N.FAILED], W.PARTIAL_FAILED),
        ([N.SUCCESS, N.FAILED, N.SKIPPED], W.PARTIAL_FAILED),
        ([N.FAILED, N.SKIPPED], W.FAILED),
        ([N.FAILED, N.FAILED], W.FAILED),
        ([N.SUCCESS, N.CANCELED], W.CANCELED),
        ([N.FAILED, N.CANCELED], W.CANCELED),
    ],
)
def test_derive_workflow_status(statuses: list[N], expected: W) -> None:
    assert derive_workflow_status(statuses) is expected


def test_derive_rejects_empty() -> None:
    with pytest.raises(ValueError):
        derive_workflow_status([])


def test_workflow_terminal_flags() -> None:
    assert not W.PENDING.is_terminal() and not W.RUNNING.is_terminal()
    for status in (W.SUCCESS, W.PARTIAL_FAILED, W.FAILED, W.CANCELED):
        assert status.is_terminal()


def test_end_to_end_failure_scenario_yields_partial_failed() -> None:
    """extract 成功，clean 失败 -> report/audit 被跳过，stats 成功 -> PARTIAL_FAILED"""
    statuses = _with(extract=N.SUCCESS, clean=N.FAILED, stats=N.SUCCESS)
    for name in nodes_to_skip(_etl(), statuses):
        statuses[name] = N.SKIPPED
    assert derive_workflow_status(statuses.values()) is W.PARTIAL_FAILED

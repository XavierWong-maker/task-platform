from __future__ import annotations

import pytest

from task_platform.domain.workflow import (
    CyclicDependencyError,
    Dependency,
    TaskNode,
    WorkflowDefinition,
    WorkflowValidationError,
)


def _n(name: str, *deps: str) -> TaskNode:
    return TaskNode(name=name, job=f"job-{name}", depends_on=tuple(deps))


def _wf(*nodes: TaskNode) -> WorkflowDefinition:
    return WorkflowDefinition(name="wf", nodes=tuple(nodes))


def _diamond() -> WorkflowDefinition:
    return _wf(_n("a"), _n("b", "a"), _n("c", "a"), _n("d", "b", "c"))


# ---------------- 拓扑排序 ----------------


def test_layers_group_independent_nodes_for_parallel_execution() -> None:
    assert _diamond().layers() == [["a"], ["b", "c"], ["d"]]


def test_topological_order_respects_every_dependency() -> None:
    wf = _diamond()
    order = wf.topological_order()
    position = {name: i for i, name in enumerate(order)}
    assert sorted(order) == ["a", "b", "c", "d"]
    for dep in wf.dependencies():
        assert position[dep.upstream] < position[dep.downstream]


def test_layers_follow_declaration_order_not_alphabet() -> None:
    wf = _wf(_n("z"), _n("m", "z"), _n("a", "z"))
    assert wf.layers() == [["z"], ["m", "a"]]


def test_node_declared_before_its_dependency_is_still_ordered_correctly() -> None:
    wf = _wf(_n("report", "agg"), _n("agg", "extract"), _n("extract"))
    assert wf.topological_order() == ["extract", "agg", "report"]


def test_disconnected_graph_puts_all_roots_in_first_layer() -> None:
    wf = _wf(_n("a"), _n("b"), _n("c", "a"))
    assert wf.layers() == [["a", "b"], ["c"]]
    assert wf.roots() == ["a", "b"]


def test_linear_etl_chain_has_one_node_per_layer() -> None:
    wf = _wf(
        _n("extract"),
        _n("clean", "extract"),
        _n("validate", "clean"),
        _n("aggregate", "validate"),
        _n("report", "aggregate"),
    )
    assert wf.layers() == [["extract"], ["clean"], ["validate"], ["aggregate"], ["report"]]


# ---------------- 查询 ----------------


def test_dependencies_lists_edges_in_stable_order() -> None:
    assert _diamond().dependencies() == [
        Dependency("a", "b"),
        Dependency("a", "c"),
        Dependency("b", "d"),
        Dependency("c", "d"),
    ]


def test_downstream_of_is_transitive_and_topologically_ordered() -> None:
    wf = _diamond()
    assert wf.downstream_of("a") == ["b", "c", "d"]
    assert wf.downstream_of("b") == ["d"]
    assert wf.downstream_of("d") == []


def test_downstream_of_unknown_node_raises_key_error() -> None:
    with pytest.raises(KeyError):
        _diamond().downstream_of("nope")


def test_node_lookup() -> None:
    assert _diamond().node("b").depends_on == ("a",)
    with pytest.raises(KeyError):
        _diamond().node("nope")


# ---------------- 环检测 ----------------


def test_cycle_is_rejected_with_the_cycle_path() -> None:
    with pytest.raises(CyclicDependencyError) as exc:
        _wf(_n("a", "c"), _n("b", "a"), _n("c", "b"))

    cycle = exc.value.cycle
    assert cycle[0] == cycle[-1]
    assert set(cycle[:-1]) == {"a", "b", "c"}
    assert "->" in str(exc.value)


def test_cycle_path_excludes_nodes_outside_the_cycle() -> None:
    with pytest.raises(CyclicDependencyError) as exc:
        _wf(_n("root"), _n("b", "root", "c"), _n("c", "b"), _n("tail", "c"))

    assert set(exc.value.cycle[:-1]) == {"b", "c"}


def test_two_node_cycle() -> None:
    with pytest.raises(CyclicDependencyError) as exc:
        _wf(_n("a", "b"), _n("b", "a"))
    assert exc.value.cycle == ["a", "b", "a"]


# ---------------- 其他校验 ----------------


def test_cyclic_error_is_a_validation_error_and_value_error() -> None:
    assert issubclass(CyclicDependencyError, WorkflowValidationError)
    assert issubclass(WorkflowValidationError, ValueError)


def test_self_dependency_rejected() -> None:
    with pytest.raises(WorkflowValidationError, match="不能依赖自己"):
        _n("a", "a")


def test_duplicate_dependency_rejected() -> None:
    with pytest.raises(WorkflowValidationError, match="重复"):
        _n("b", "a", "a")


def test_unknown_dependency_rejected() -> None:
    with pytest.raises(WorkflowValidationError, match="不存在"):
        _wf(_n("a", "ghost"))


def test_duplicate_node_names_rejected() -> None:
    with pytest.raises(WorkflowValidationError, match="重复"):
        _wf(_n("a"), _n("a"))


def test_empty_workflow_rejected() -> None:
    with pytest.raises(WorkflowValidationError):
        WorkflowDefinition(name="wf", nodes=())


@pytest.mark.parametrize(
    "build",
    [
        lambda: TaskNode(name=" ", job="j"),
        lambda: TaskNode(name="a", job=""),
        lambda: WorkflowDefinition(name=" ", nodes=(TaskNode("a", "j"),)),
    ],
)
def test_blank_names_rejected(build) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(WorkflowValidationError):
        build()

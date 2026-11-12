"""工作流的状态规则：节点状态转换、可运行/需跳过的节点、工作流整体状态。

全部是纯函数，输入是"节点名 -> NodeStatus"的映射，不依赖数据库和执行器。

规则：
- 节点可运行：自身 PENDING，且所有上游都是 SUCCESS
- 失败传播：PENDING 节点只要有上游处于 FAILED / SKIPPED / CANCELED，就会被 SKIPPED
  （按拓扑顺序扫一遍即得传递闭包；没有依赖关系的并行分支不受影响）
- 工作流状态由节点状态推导，不单独存储
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from task_platform.domain.enums import NodeStatus, WorkflowStatus
from task_platform.domain.workflow import WorkflowDefinition


class InvalidNodeTransitionError(Exception):
    def __init__(self, current: NodeStatus, target: NodeStatus) -> None:
        super().__init__(f"不允许从 {current.value} 转换到 {target.value}")
        self.current = current
        self.target = target


_ALLOWED: dict[NodeStatus, frozenset[NodeStatus]] = {
    NodeStatus.PENDING: frozenset({NodeStatus.RUNNING, NodeStatus.SKIPPED, NodeStatus.CANCELED}),
    NodeStatus.RUNNING: frozenset({NodeStatus.SUCCESS, NodeStatus.FAILED, NodeStatus.CANCELED}),
    NodeStatus.SUCCESS: frozenset(),
    NodeStatus.FAILED: frozenset(),
    NodeStatus.SKIPPED: frozenset(),
    NodeStatus.CANCELED: frozenset(),
}

_BLOCKING = frozenset({NodeStatus.FAILED, NodeStatus.SKIPPED, NodeStatus.CANCELED})


def can_transition_node(current: NodeStatus, target: NodeStatus) -> bool:
    return target in _ALLOWED[current]


def check_node_transition(current: NodeStatus, target: NodeStatus) -> None:
    """非法转换抛 InvalidNodeTransitionError"""
    if not can_transition_node(current, target):
        raise InvalidNodeTransitionError(current, target)


def initial_statuses(definition: WorkflowDefinition) -> dict[str, NodeStatus]:
    return {n.name: NodeStatus.PENDING for n in definition.nodes}


def _check_keys(definition: WorkflowDefinition, statuses: Mapping[str, NodeStatus]) -> None:
    if set(statuses) != {n.name for n in definition.nodes}:
        raise ValueError("statuses 的节点必须与工作流定义的节点完全一致")


def ready_nodes(definition: WorkflowDefinition, statuses: Mapping[str, NodeStatus]) -> list[str]:
    """现在可以开始运行的节点（按声明顺序）：自身 PENDING 且所有上游 SUCCESS"""
    _check_keys(definition, statuses)
    return [
        n.name
        for n in definition.nodes
        if statuses[n.name] is NodeStatus.PENDING
        and all(statuses[d] is NodeStatus.SUCCESS for d in n.depends_on)
    ]


def nodes_to_skip(definition: WorkflowDefinition, statuses: Mapping[str, NodeStatus]) -> list[str]:
    """必须被跳过的 PENDING 节点（按拓扑顺序）：有上游处于 FAILED / SKIPPED / CANCELED。
    已经开始或已结束的节点不会被改动。"""
    _check_keys(definition, statuses)
    local = dict(statuses)
    skipped: list[str] = []
    for name in definition.topological_order():
        if local[name] is not NodeStatus.PENDING:
            continue
        if any(local[d] in _BLOCKING for d in definition.node(name).depends_on):
            local[name] = NodeStatus.SKIPPED  # 让更下游的节点也能看到
            skipped.append(name)
    return skipped


def derive_workflow_status(statuses: Iterable[NodeStatus]) -> WorkflowStatus:
    """由节点状态推导工作流状态。

    - 全部 PENDING                    -> PENDING
    - 还有 PENDING / RUNNING 的节点    -> RUNNING
    - 全部结束：有 CANCELED            -> CANCELED
                全部 SUCCESS           -> SUCCESS
                有 SUCCESS 也有失败/跳过 -> PARTIAL_FAILED
                没有任何 SUCCESS        -> FAILED
    """
    values = list(statuses)
    if not values:
        raise ValueError("至少需要一个节点状态")
    if all(s is NodeStatus.PENDING for s in values):
        return WorkflowStatus.PENDING
    if any(not s.is_terminal() for s in values):
        return WorkflowStatus.RUNNING
    if any(s is NodeStatus.CANCELED for s in values):
        return WorkflowStatus.CANCELED
    if all(s is NodeStatus.SUCCESS for s in values):
        return WorkflowStatus.SUCCESS
    if any(s is NodeStatus.SUCCESS for s in values):
        return WorkflowStatus.PARTIAL_FAILED
    return WorkflowStatus.FAILED

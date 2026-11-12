"""重放规则：决定"重试"和"重跑某个节点"时，来源运行里哪些节点沿用、哪些重新执行。

返回值是 carry_over（节点名 -> 来源节点运行），直接交给引擎；其余节点会重新执行。
纯函数，只依赖来源运行的状态和定义。
"""

from __future__ import annotations

from task_platform.domain.enums import NodeStatus, WorkflowStatus
from task_platform.domain.workflow import WorkflowDefinition
from task_platform.domain.workflow_run import NodeRun, WorkflowRun


class ReplayError(Exception):
    """来源运行的状态不允许这次重试 / 重跑"""


def _require_finished(source: WorkflowRun) -> None:
    status = source.status
    if not status.is_terminal():
        raise ReplayError(f"工作流运行 {source.run_id} 尚未结束（{status.value}），不能重放")


def carry_over_for_retry(source: WorkflowRun) -> dict[str, NodeRun]:
    """工作流级重试：沿用所有 SUCCESS 的节点，重新执行其余节点（FAILED / SKIPPED / CANCELED）"""
    _require_finished(source)
    if source.status is WorkflowStatus.SUCCESS:
        raise ReplayError("该运行已全部成功，没有需要重试的节点；如需重跑请指定节点")
    return {name: n for name, n in source.nodes.items() if n.status is NodeStatus.SUCCESS}


def carry_over_for_rerun(
    definition: WorkflowDefinition, source: WorkflowRun, node: str
) -> dict[str, NodeRun]:
    """单节点重跑：重置该节点及其全部下游，其余节点原样沿用（包括 FAILED 的兄弟分支）。
    该节点的上游必须全部 SUCCESS，否则它根本无法运行——应先用 retry。"""
    _require_finished(source)
    if node not in source.nodes:
        raise ReplayError(f"节点不存在: {node}")
    blocked = [
        up
        for up in definition.node(node).depends_on
        if source.nodes[up].status is not NodeStatus.SUCCESS
    ]
    if blocked:
        raise ReplayError(f"节点 {node} 的上游没有成功: {blocked}，请先重试整个工作流")
    reset = {node, *definition.downstream_of(node)}
    return {name: n for name, n in source.nodes.items() if name not in reset}

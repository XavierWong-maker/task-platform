"""工作流的运行记录：一次工作流运行 = 若干节点运行。

NodeRun 持有该节点的所有尝试（JobExecution，同一节点的各次尝试共享 run_id）。
WorkflowRun 的整体状态由节点状态推导，不单独存储，不会出现不一致。

重试 / 重跑会创建新的 WorkflowRun（parent_run_id 指向来源运行）；
沿用来源结果、没有重新执行的节点标记 reused=True。
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from task_platform.domain.enums import NodeStatus, WorkflowStatus
from task_platform.domain.models import JobExecution
from task_platform.domain.workflow import WorkflowDefinition, WorkflowValidationError
from task_platform.domain.workflow_state import check_node_transition, derive_workflow_status


def new_workflow_run_id() -> str:
    return f"wfrun_{uuid.uuid4().hex[:12]}"


@dataclass(slots=True)
class NodeRun:
    name: str
    status: NodeStatus = NodeStatus.PENDING
    attempts: list[JobExecution] = field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    reused: bool = False  # 沿用来源运行的结果，本次运行没有执行它

    @property
    def run_id(self) -> str | None:
        """节点对应的任务 run_id；没有真正执行过（被跳过/取消）时为 None"""
        return self.attempts[-1].run_id if self.attempts else None

    def mark(self, target: NodeStatus) -> None:
        """修改节点状态的唯一入口：非法转换抛 InvalidNodeTransitionError"""
        check_node_transition(self.status, target)
        self.status = target


@dataclass(slots=True)
class WorkflowRun:
    workflow: str
    nodes: dict[str, NodeRun]
    run_id: str = field(default_factory=new_workflow_run_id)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    parent_run_id: str | None = None

    @property
    def status(self) -> WorkflowStatus:
        return derive_workflow_status(n.status for n in self.nodes.values())

    def statuses(self) -> dict[str, NodeStatus]:
        return {name: node.status for name, node in self.nodes.items()}


def new_workflow_run(
    definition: WorkflowDefinition,
    *,
    parent_run_id: str | None = None,
    carry_over: Mapping[str, NodeRun] | None = None,
) -> WorkflowRun:
    """创建一次新运行。carry_over 里的节点沿用来源结果（必须已结束），其余节点从 PENDING 开始"""
    nodes = {n.name: NodeRun(n.name) for n in definition.nodes}
    for name, previous in (carry_over or {}).items():
        if name not in nodes:
            raise WorkflowValidationError(f"carry_over 包含工作流中不存在的节点: {name}")
        if not previous.status.is_terminal():
            raise WorkflowValidationError(f"节点 {name} 尚未结束，不能沿用它的结果")
        nodes[name] = NodeRun(
            name=name,
            status=previous.status,
            attempts=list(previous.attempts),
            started_at=previous.started_at,
            finished_at=previous.finished_at,
            error_message=previous.error_message,
            reused=True,
        )
    return WorkflowRun(workflow=definition.name, nodes=nodes, parent_run_id=parent_run_id)

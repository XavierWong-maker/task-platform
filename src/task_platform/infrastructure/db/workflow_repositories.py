"""工作流的 Repository：定义与运行历史。

约定与 repositories.py 一致：不提交事务，写操作后 flush()。
- 定义以 JSON 整体读写；读出时由 WorkflowDefinition 重新校验
- 运行记录带定义快照，之后修改定义不影响历史
- 节点每次尝试的 JobExecution 在 job_executions 里，get() 时按 job_run_id 加载
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from task_platform.domain.enums import WorkflowStatus
from task_platform.domain.models import utc_now
from task_platform.domain.workflow import TaskNode, WorkflowDefinition
from task_platform.domain.workflow_run import NodeRun, WorkflowRun
from task_platform.infrastructure.db.models import (
    WorkflowNodeRunRecord,
    WorkflowRecord,
    WorkflowRunRecord,
)
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    Page,
    _validate_pagination,
)


def definition_to_json(definition: WorkflowDefinition) -> list[dict[str, Any]]:
    return [
        {"name": n.name, "job": n.job, "depends_on": list(n.depends_on)} for n in definition.nodes
    ]


def definition_from_json(name: str, data: list[dict[str, Any]]) -> WorkflowDefinition:
    nodes = tuple(TaskNode(d["name"], d["job"], tuple(d["depends_on"])) for d in data)
    return WorkflowDefinition(name, nodes)


class WorkflowRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def save(self, definition: WorkflowDefinition) -> None:
        """按名称 upsert：不存在则创建，存在则整体替换定义"""
        now = utc_now()
        data = definition_to_json(definition)
        record = self._session.get(WorkflowRecord, definition.name)
        if record is None:
            self._session.add(
                WorkflowRecord(
                    name=definition.name, definition=data, created_at=now, updated_at=now
                )
            )
        else:
            record.definition = data
            record.updated_at = now
        self._session.flush()

    def get(self, name: str) -> WorkflowDefinition | None:
        record = self._session.get(WorkflowRecord, name)
        return definition_from_json(record.name, record.definition) if record else None

    def list_names(self) -> list[str]:
        return list(
            self._session.scalars(select(WorkflowRecord.name).order_by(WorkflowRecord.name))
        )

    def delete(self, name: str) -> bool:
        """删除定义；外键 ON DELETE CASCADE 会一并清理它的运行历史"""
        record = self._session.get(WorkflowRecord, name)
        if record is None:
            return False
        self._session.delete(record)
        self._session.flush()
        return True


@dataclass(frozen=True, slots=True)
class WorkflowRunSummary:
    run_id: str
    workflow: str
    status: WorkflowStatus
    started_at: datetime
    finished_at: datetime | None
    parent_run_id: str | None = None


class WorkflowRunRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def save(self, run: WorkflowRun, definition: WorkflowDefinition) -> None:
        """upsert 一次运行及其全部节点。definition 只在首次写入时记为快照。
        对应的 workflows 行必须已存在（外键）。"""
        if run.started_at is None:
            raise ValueError("工作流运行尚未开始，不能保存")

        record = self._session.get(WorkflowRunRecord, run.run_id)
        if record is None:
            self._session.add(
                WorkflowRunRecord(
                    run_id=run.run_id,
                    workflow_name=run.workflow,
                    status=run.status,
                    definition=definition_to_json(definition),
                    started_at=run.started_at,
                    finished_at=run.finished_at,
                    parent_run_id=run.parent_run_id,
                )
            )
        else:
            record.status = run.status
            record.finished_at = run.finished_at
        self._session.flush()  # 先有父行，再写节点行

        for position, node in enumerate(run.nodes.values()):
            node_record = self._session.get(WorkflowNodeRunRecord, (run.run_id, node.name))
            if node_record is None:
                node_record = WorkflowNodeRunRecord(workflow_run_id=run.run_id, node_name=node.name)
                self._session.add(node_record)
            node_record.position = position
            node_record.status = node.status
            node_record.job_run_id = node.run_id
            node_record.attempts = len(node.attempts)
            node_record.started_at = node.started_at
            node_record.finished_at = node.finished_at
            node_record.error_message = node.error_message
            node_record.reused = node.reused
        self._session.flush()

    def get(self, run_id: str) -> WorkflowRun | None:
        record = self._session.get(WorkflowRunRecord, run_id)
        if record is None:
            return None
        stmt = (
            select(WorkflowNodeRunRecord)
            .where(WorkflowNodeRunRecord.workflow_run_id == run_id)
            .order_by(WorkflowNodeRunRecord.position)
        )
        executions = ExecutionRepository(self._session)
        nodes: dict[str, NodeRun] = {}
        for r in self._session.scalars(stmt):
            attempts = executions.list_by_run(r.job_run_id) if r.job_run_id else []
            nodes[r.node_name] = NodeRun(
                name=r.node_name,
                status=r.status,
                attempts=attempts,
                started_at=r.started_at,
                finished_at=r.finished_at,
                error_message=r.error_message,
                reused=r.reused,
            )
        return WorkflowRun(
            workflow=record.workflow_name,
            nodes=nodes,
            run_id=record.run_id,
            started_at=record.started_at,
            finished_at=record.finished_at,
            parent_run_id=record.parent_run_id,
        )

    def get_definition(self, run_id: str) -> WorkflowDefinition | None:
        """运行时刻的定义快照（不受之后修改定义的影响）"""
        record = self._session.get(WorkflowRunRecord, run_id)
        return definition_from_json(record.workflow_name, record.definition) if record else None

    def list_paginated(
        self,
        *,
        workflow: str | None = None,
        status: WorkflowStatus | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[WorkflowRunSummary]:
        """按工作流名/状态过滤，最近开始的在前"""
        _validate_pagination(page, page_size)

        filters = []
        if workflow is not None:
            filters.append(WorkflowRunRecord.workflow_name == workflow)
        if status is not None:
            filters.append(WorkflowRunRecord.status == status)

        count_stmt = select(func.count()).select_from(WorkflowRunRecord)
        stmt = select(WorkflowRunRecord).order_by(
            WorkflowRunRecord.started_at.desc(), WorkflowRunRecord.run_id.desc()
        )
        for condition in filters:
            count_stmt = count_stmt.where(condition)
            stmt = stmt.where(condition)

        total = self._session.scalar(count_stmt) or 0
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        items = [
            WorkflowRunSummary(
                r.run_id, r.workflow_name, r.status, r.started_at, r.finished_at, r.parent_run_id
            )
            for r in self._session.scalars(stmt)
        ]
        return Page(items=items, total=total, page=page, page_size=page_size)

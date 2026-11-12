"""工作流的服务层：把引擎的回调接到数据库，并提供运行 / 重试 / 重跑三个入口。

- on_update（协调线程）：写 workflow_runs / workflow_node_runs
- on_attempt（runner 线程）：每次尝试结束后，同一事务内写 job_executions 和该次尝试的日志。
  必须在每次尝试之后立刻取走日志：同一节点重试多次时 run_id 相同，
  拖到节点结束再取，所有日志会被算到最后一次尝试上。
两个回调用各自的 session，互不共享。回调失败只记日志（引擎保证不受影响）。

重试 / 重跑都会创建一次新的工作流运行（parent_run_id 指向来源运行），来源运行保持不变。
它们使用来源运行的定义快照，既不读取、也不覆盖 workflows 表里的当前定义。
"""

from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.cancel import CancelToken
from task_platform.domain.models import JobExecution
from task_platform.domain.workflow import WorkflowDefinition
from task_platform.domain.workflow_replay import carry_over_for_rerun, carry_over_for_retry
from task_platform.domain.workflow_run import NodeRun, WorkflowRun
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobLogRepository
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.db.workflow_repositories import (
    WorkflowRepository,
    WorkflowRunRepository,
)
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.workflow_engine import WorkflowEngine


class RunNotFoundError(Exception):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"工作流运行不存在: {run_id}")
        self.run_id = run_id


class WorkflowRecorder:
    def __init__(
        self,
        factory: sessionmaker[Session],
        collector: LogCollector,
        definition: WorkflowDefinition,
    ) -> None:
        self._factory = factory
        self._collector = collector
        self._definition = definition

    def on_update(self, run: WorkflowRun) -> None:
        with session_scope(self._factory) as s:
            WorkflowRunRepository(s).save(run, self._definition)

    def on_attempt(self, workflow_run_id: str, node: str, execution: JobExecution) -> None:
        logs = self._collector.drain(execution.run_id, attempt=execution.attempt)
        with session_scope(self._factory) as s:
            ExecutionRepository(s).upsert(execution)  # 先 execution 后 logs（复合外键）
            if logs:
                JobLogRepository(s).add_many(logs)


def _execute(
    engine: WorkflowEngine,
    definition: WorkflowDefinition,
    factory: sessionmaker[Session],
    collector: LogCollector,
    *,
    cancel_token: CancelToken | None,
    carry_over: Mapping[str, NodeRun] | None = None,
    parent_run_id: str | None = None,
) -> WorkflowRun:
    recorder = WorkflowRecorder(factory, collector, definition)
    return engine.run(
        definition,
        cancel_token=cancel_token,
        on_update=recorder.on_update,
        on_attempt=recorder.on_attempt,
        carry_over=carry_over,
        parent_run_id=parent_run_id,
    )


def run_workflow(
    engine: WorkflowEngine,
    definition: WorkflowDefinition,
    factory: sessionmaker[Session],
    collector: LogCollector,
    *,
    cancel_token: CancelToken | None = None,
) -> WorkflowRun:
    """保存（或更新）定义，运行，并把运行历史写入数据库"""
    with session_scope(factory) as s:
        WorkflowRepository(s).save(definition)
    return _execute(engine, definition, factory, collector, cancel_token=cancel_token)


def _load_source(
    factory: sessionmaker[Session], run_id: str
) -> tuple[WorkflowRun, WorkflowDefinition]:
    with session_scope(factory) as s:
        repo = WorkflowRunRepository(s)
        source = repo.get(run_id)
        definition = repo.get_definition(run_id)
    if source is None or definition is None:
        raise RunNotFoundError(run_id)
    return source, definition


def retry_workflow(
    engine: WorkflowEngine,
    run_id: str,
    factory: sessionmaker[Session],
    collector: LogCollector,
    *,
    cancel_token: CancelToken | None = None,
) -> WorkflowRun:
    """工作流级重试：新建一次运行，沿用来源运行里成功的节点，重新执行其余节点。
    RunNotFoundError / ReplayError 表示来源不存在或状态不允许。"""
    source, definition = _load_source(factory, run_id)
    return _execute(
        engine,
        definition,
        factory,
        collector,
        cancel_token=cancel_token,
        carry_over=carry_over_for_retry(source),
        parent_run_id=run_id,
    )


def rerun_node(
    engine: WorkflowEngine,
    run_id: str,
    node: str,
    factory: sessionmaker[Session],
    collector: LogCollector,
    *,
    cancel_token: CancelToken | None = None,
) -> WorkflowRun:
    """单节点重跑：新建一次运行，重新执行该节点及其全部下游，其余节点沿用来源结果"""
    source, definition = _load_source(factory, run_id)
    return _execute(
        engine,
        definition,
        factory,
        collector,
        cancel_token=cancel_token,
        carry_over=carry_over_for_rerun(definition, source, node),
        parent_run_id=run_id,
    )

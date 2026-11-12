from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Response
from sqlalchemy.orm import Session, sessionmaker

from task_platform.api.deps import get_log_collector, get_registry, get_session, get_session_factory
from task_platform.api.errors import ApiError
from task_platform.api.schemas import (
    NodeRunResponse,
    PageResponse,
    RerunRequest,
    WorkflowNodeSchema,
    WorkflowPut,
    WorkflowResponse,
    WorkflowRunResponse,
    WorkflowRunSummaryResponse,
)
from task_platform.domain.enums import WorkflowStatus
from task_platform.domain.workflow import TaskNode, WorkflowDefinition, WorkflowValidationError
from task_platform.domain.workflow_replay import ReplayError
from task_platform.domain.workflow_run import WorkflowRun
from task_platform.infrastructure.db.repositories import JobRepository
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.db.workflow_repositories import (
    WorkflowRepository,
    WorkflowRunRepository,
)
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.registry import TaskRegistry
from task_platform.workflow_engine import WorkflowEngine
from task_platform.workflow_service import (
    RunNotFoundError,
    rerun_node,
    retry_workflow,
    run_workflow,
)

router = APIRouter(prefix="/api/v1", tags=["workflows"])

SessionDep = Annotated[Session, Depends(get_session)]
FactoryDep = Annotated[sessionmaker[Session], Depends(get_session_factory)]
RegistryDep = Annotated[TaskRegistry, Depends(get_registry)]
CollectorDep = Annotated[LogCollector, Depends(get_log_collector)]


def _invalid(exc: Exception) -> ApiError:
    return ApiError(422, "workflow_invalid", str(exc))


def _workflow_not_found(name: str) -> ApiError:
    return ApiError(404, "workflow_not_found", f"工作流不存在: {name}")


def _run_not_found(run_id: str) -> ApiError:
    return ApiError(404, "workflow_run_not_found", f"工作流运行不存在: {run_id}")


def _definition_response(definition: WorkflowDefinition) -> WorkflowResponse:
    return WorkflowResponse(
        name=definition.name,
        nodes=[
            WorkflowNodeSchema(name=n.name, job=n.job, depends_on=list(n.depends_on))
            for n in definition.nodes
        ],
    )


def _run_response(run: WorkflowRun) -> WorkflowRunResponse:
    return WorkflowRunResponse(
        run_id=run.run_id,
        workflow=run.workflow,
        status=run.status,
        parent_run_id=run.parent_run_id,
        started_at=run.started_at,
        finished_at=run.finished_at,
        nodes=[
            NodeRunResponse(
                name=n.name,
                status=n.status,
                attempts=len(n.attempts),
                job_run_id=n.run_id,
                reused=n.reused,
                started_at=n.started_at,
                finished_at=n.finished_at,
                error_message=n.error_message,
            )
            for n in run.nodes.values()
        ],
    )


def _build_engine(factory: sessionmaker[Session], registry: TaskRegistry) -> WorkflowEngine:
    with session_scope(factory) as s:
        jobs = {j.name: j for j in JobRepository(s).list_all()}
    return WorkflowEngine(registry, jobs)


# ---------------- 定义 ----------------


@router.put("/workflows/{name}", response_model=WorkflowResponse)
def put_workflow(
    name: Annotated[str, Path(min_length=1, max_length=128)],
    payload: WorkflowPut,
    session: SessionDep,
) -> WorkflowResponse:
    """创建或整体替换工作流定义（不影响已有的运行历史，历史保存的是运行时的定义快照）"""
    try:
        definition = WorkflowDefinition(
            name, tuple(TaskNode(n.name, n.job, tuple(n.depends_on)) for n in payload.nodes)
        )
    except WorkflowValidationError as exc:
        raise _invalid(exc) from None
    WorkflowRepository(session).save(definition)
    return _definition_response(definition)


@router.get("/workflows", response_model=list[str])
def list_workflows(session: SessionDep) -> list[str]:
    return WorkflowRepository(session).list_names()


@router.get("/workflows/{name}", response_model=WorkflowResponse)
def get_workflow(name: str, session: SessionDep) -> WorkflowResponse:
    definition = WorkflowRepository(session).get(name)
    if definition is None:
        raise _workflow_not_found(name)
    return _definition_response(definition)


@router.delete("/workflows/{name}", status_code=204)
def delete_workflow(name: str, session: SessionDep) -> Response:
    """删除定义，级联删除它的全部运行历史"""
    if not WorkflowRepository(session).delete(name):
        raise _workflow_not_found(name)
    return Response(status_code=204)


# ---------------- 运行（同步执行，与 ADR-0003 的手动触发一致） ----------------


@router.post("/workflows/{name}/run", response_model=WorkflowRunResponse, status_code=201)
def run_workflow_endpoint(
    name: str, factory: FactoryDep, registry: RegistryDep, collector: CollectorDep
) -> WorkflowRunResponse:
    with session_scope(factory) as s:
        definition = WorkflowRepository(s).get(name)
    if definition is None:
        raise _workflow_not_found(name)
    try:
        run = run_workflow(_build_engine(factory, registry), definition, factory, collector)
    except WorkflowValidationError as exc:
        raise _invalid(exc) from None
    return _run_response(run)


@router.get("/workflow-runs", response_model=PageResponse[WorkflowRunSummaryResponse])
def list_workflow_runs(
    session: SessionDep,
    workflow: str | None = None,
    status: WorkflowStatus | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> PageResponse[WorkflowRunSummaryResponse]:
    result = WorkflowRunRepository(session).list_paginated(
        workflow=workflow, status=status, page=page, page_size=page_size
    )
    return PageResponse[WorkflowRunSummaryResponse](
        items=[
            WorkflowRunSummaryResponse(
                run_id=r.run_id,
                workflow=r.workflow,
                status=r.status,
                parent_run_id=r.parent_run_id,
                started_at=r.started_at,
                finished_at=r.finished_at,
            )
            for r in result.items
        ],
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        total_pages=result.total_pages,
    )


@router.get("/workflow-runs/{run_id}", response_model=WorkflowRunResponse)
def get_workflow_run(run_id: str, session: SessionDep) -> WorkflowRunResponse:
    run = WorkflowRunRepository(session).get(run_id)
    if run is None:
        raise _run_not_found(run_id)
    return _run_response(run)


@router.post("/workflow-runs/{run_id}/retry", response_model=WorkflowRunResponse, status_code=201)
def retry_workflow_run(
    run_id: str, factory: FactoryDep, registry: RegistryDep, collector: CollectorDep
) -> WorkflowRunResponse:
    """重试：新建一次运行，沿用来源运行里成功的节点，重新执行其余节点"""
    try:
        run = retry_workflow(_build_engine(factory, registry), run_id, factory, collector)
    except RunNotFoundError:
        raise _run_not_found(run_id) from None
    except ReplayError as exc:
        raise ApiError(409, "run_not_replayable", str(exc)) from None
    except WorkflowValidationError as exc:
        raise _invalid(exc) from None
    return _run_response(run)


@router.post("/workflow-runs/{run_id}/rerun", response_model=WorkflowRunResponse, status_code=201)
def rerun_workflow_node(
    run_id: str,
    payload: RerunRequest,
    factory: FactoryDep,
    registry: RegistryDep,
    collector: CollectorDep,
) -> WorkflowRunResponse:
    """单节点重跑：新建一次运行，重新执行该节点及其全部下游，其余节点沿用来源结果"""
    try:
        run = rerun_node(_build_engine(factory, registry), run_id, payload.node, factory, collector)
    except RunNotFoundError:
        raise _run_not_found(run_id) from None
    except ReplayError as exc:
        raise ApiError(409, "run_not_replayable", str(exc)) from None
    except WorkflowValidationError as exc:
        raise _invalid(exc) from None
    return _run_response(run)

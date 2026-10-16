from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from task_platform.api.deps import get_executor_pool, get_log_collector, get_registry, get_session
from task_platform.api.errors import ApiError
from task_platform.api.schemas import (
    ExecutionResponse,
    JobCreate,
    JobResponse,
    JobUpdate,
    PageResponse,
)
from task_platform.domain.models import Job, RetryPolicy
from task_platform.executor import execute_attempt
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    JobLogRepository,
    JobRepository,
)
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.registry import JobNotRegisteredError, TaskRegistry

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])

SessionDep = Annotated[Session, Depends(get_session)]


def _name_conflict(name: str) -> ApiError:
    return ApiError(409, "job_name_conflict", f"任务名称已存在: {name}")


def _get_or_404(repo: JobRepository, job_id: str) -> Job:
    job = repo.get(job_id)
    if job is None:
        raise ApiError(404, "job_not_found", f"任务不存在: {job_id}")
    return job


@router.get("", response_model=PageResponse[JobResponse])
def list_jobs(
    session: SessionDep,
    enabled: bool | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> PageResponse[JobResponse]:
    result = JobRepository(session).list_paginated(enabled=enabled, page=page, page_size=page_size)
    return PageResponse[JobResponse](
        items=[JobResponse.model_validate(j) for j in result.items],
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        total_pages=result.total_pages,
    )


@router.post("", response_model=JobResponse, status_code=201)
def create_job(
    payload: JobCreate,
    session: SessionDep,
    registry: Annotated[TaskRegistry, Depends(get_registry)],
) -> JobResponse:
    if not registry.is_registered(payload.handler):
        raise ApiError(422, "handler_not_registered", f"handler 未注册: {payload.handler}")
    repo = JobRepository(session)
    if repo.get_by_name(payload.name) is not None:
        raise _name_conflict(payload.name)

    job = Job(
        name=payload.name,
        job_type=payload.job_type,
        handler=payload.handler,
        timeout_seconds=payload.timeout_seconds,
        retry_policy=RetryPolicy(**payload.retry_policy.model_dump()),
        enabled=payload.enabled,
    )
    try:
        repo.add(job)
    except IntegrityError:  # 并发下的兜底：两个请求同时通过了上面的查重
        raise _name_conflict(payload.name) from None
    return JobResponse.model_validate(job)


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: str, session: SessionDep) -> JobResponse:
    return JobResponse.model_validate(_get_or_404(JobRepository(session), job_id))


@router.patch("/{job_id}", response_model=JobResponse)
def update_job(job_id: str, payload: JobUpdate, session: SessionDep) -> JobResponse:
    repo = JobRepository(session)
    job = _get_or_404(repo, job_id)
    changes = payload.model_dump(exclude_unset=True)

    if "name" in changes and changes["name"] != job.name:
        if repo.get_by_name(changes["name"]) is not None:
            raise _name_conflict(changes["name"])
        job.name = changes["name"]
    if "timeout_seconds" in changes:
        job.timeout_seconds = changes["timeout_seconds"]
    if "retry_policy" in changes:
        job.retry_policy = RetryPolicy(**changes["retry_policy"])  # 整体替换
    if "enabled" in changes:
        job.enabled = changes["enabled"]

    try:
        repo.update(job)
    except IntegrityError:
        raise _name_conflict(job.name) from None
    return JobResponse.model_validate(job)


@router.delete("/{job_id}", status_code=204)
def delete_job(job_id: str, session: SessionDep) -> Response:
    if not JobRepository(session).delete(job_id):
        raise ApiError(404, "job_not_found", f"任务不存在: {job_id}")
    return Response(status_code=204)


def _set_enabled(job_id: str, session: Session, enabled: bool) -> JobResponse:
    repo = JobRepository(session)
    job = _get_or_404(repo, job_id)
    job.enabled = enabled
    repo.update(job)
    return JobResponse.model_validate(job)


@router.post("/{job_id}/pause", response_model=JobResponse)
def pause_job(job_id: str, session: SessionDep) -> JobResponse:
    return _set_enabled(job_id, session, False)


@router.post("/{job_id}/resume", response_model=JobResponse)
def resume_job(job_id: str, session: SessionDep) -> JobResponse:
    return _set_enabled(job_id, session, True)


@router.post("/{job_id}/run", response_model=ExecutionResponse, status_code=201)
def run_job(
    job_id: str,
    session: SessionDep,
    registry: Annotated[TaskRegistry, Depends(get_registry)],
    pool: Annotated[ThreadPoolExecutor, Depends(get_executor_pool)],
    collector: Annotated[LogCollector, Depends(get_log_collector)],
) -> ExecutionResponse:
    """立即同步运行一次（单次尝试，与 CLI `run` 语义一致）并持久化结果和日志"""
    job = _get_or_404(JobRepository(session), job_id)
    try:
        result = execute_attempt(job, registry, pool, attempt_number=1)
    except JobNotRegisteredError:
        raise ApiError(409, "handler_not_registered", f"handler 未注册: {job.handler}")

    execution = result.execution
    logs = collector.drain(execution.run_id, attempt=execution.attempt)
    ExecutionRepository(session).add(execution)  # 先 execution 后 logs (复合外键)
    if logs:
        JobLogRepository(session).add_many(logs)
    return ExecutionResponse.model_validate(execution)

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from task_platform.api.deps import get_session
from task_platform.api.errors import ApiError
from task_platform.api.schemas import (
    ExecutionResponse,
    JobLogResponse,
    PageResponse,
    RunDetailResponse,
)
from task_platform.domain.enums import JobStatus
from task_platform.infrastructure.db.repositories import ExecutionRepository, JobLogRepository

router = APIRouter(prefix="/api/v1/executions", tags=["executions"])


def _require_aware(name: str, value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        raise ApiError(422, "validation_error", f"{name} 必须带时区，例如 2026-10-01T00:00:00Z")
    return value


@router.get("", response_model=PageResponse[ExecutionResponse])
def list_executions(
    session: Annotated[Session, Depends(get_session)],
    job_id: str | None = None,
    status: JobStatus | None = None,
    started_after: datetime | None = None,
    started_before: datetime | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> PageResponse[ExecutionResponse]:
    result = ExecutionRepository(session).list_paginated(
        job_id=job_id,
        status=status,
        started_after=_require_aware("started_after", started_after),
        started_before=_require_aware("started_before", started_before),
        page=page,
        page_size=page_size,
    )
    return PageResponse[ExecutionResponse](
        items=[ExecutionResponse.model_validate(e) for e in result.items],
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        total_pages=result.total_pages,
    )


def _run_not_found(run_id: str) -> ApiError:
    return ApiError(404, "run_not_found", f"运行记录不存在: {run_id}")


@router.get("/{run_id}", response_model=RunDetailResponse)
def get_run(run_id: str, session: Annotated[Session, Depends(get_session)]) -> RunDetailResponse:
    attempt = ExecutionRepository(session).list_by_run(run_id)  # 按 attempt 升序
    if not attempt:
        raise _run_not_found(run_id)
    last = attempt[-1]
    return RunDetailResponse(
        run_id=run_id,
        job_id=last.job_id,
        status=last.status,
        attempts=[ExecutionResponse.model_validate(e) for e in attempt],
    )


@router.get("/{run_id}/logs", response_model=PageResponse[JobLogResponse])
def get_run_logs(
    run_id: str,
    session: Annotated[Session, Depends(get_session)],
    attempt: Annotated[int | None, Query(ge=1)] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PageResponse[JobLogResponse]:
    if not ExecutionRepository(session).list_by_run(run_id):
        raise _run_not_found(run_id)
    result = JobLogRepository(session).list_by_run(
        run_id, attempt=attempt, page=page, page_size=page_size
    )
    return PageResponse[JobLogResponse](
        items=[JobLogResponse.model_validate(log) for log in result.items],
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        total_pages=result.total_pages,
    )

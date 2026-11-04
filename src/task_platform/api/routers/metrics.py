from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from task_platform.api.deps import get_queue, get_session
from task_platform.api.schemas import JobMetricsResponse, MetricsResponse, QueueMetricsResponse
from task_platform.infrastructure.queue.base import TaskQueue
from task_platform.infrastructure.queue.factory import get_queue_stream
from task_platform.metrics import collect_metrics
from task_platform.prometheus import CONTENT_TYPE, render_prometheus

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])
prometheus_router = APIRouter(tags=["metrics"])

SessionDep = Annotated[Session, Depends(get_session)]
QueueDep = Annotated[TaskQueue | None, Depends(get_queue)]


@router.get("", response_model=MetricsResponse)
def get_metrics(
    session: SessionDep,
    queue: QueueDep,
    window_seconds: Annotated[float | None, Query(gt=0, le=315_360_000)] = None,
) -> MetricsResponse:
    """任务指标快照；window_seconds 不传表示全量"""
    window = timedelta(seconds=window_seconds) if window_seconds is not None else None
    snapshot = collect_metrics(session, queue, window=window)
    return MetricsResponse(
        generated_at=snapshot.generated_at,
        window_seconds=window_seconds,
        jobs=[
            JobMetricsResponse(
                job=j.job,
                status_counts=j.status_counts,
                retry_attempts=j.retry_attempts,
                success_rate=j.success_rate,
                failure_rate=j.failure_rate,
                avg_duration_seconds=j.avg_duration_seconds,
                max_duration_seconds=j.duration_max,
            )
            for j in snapshot.jobs
        ],
        queue=QueueMetricsResponse(depth=snapshot.queue_depth, pending=snapshot.queue_pending),
    )


@prometheus_router.get("/metrics", response_class=PlainTextResponse)
def prometheus_metrics(session: SessionDep, queue: QueueDep) -> PlainTextResponse:
    """Prometheus 抓取端点：总是全量，窗口计算交给 Prometheus 的 rate()"""
    snapshot = collect_metrics(session, queue)
    body = render_prometheus(snapshot, queue_name=get_queue_stream())
    return PlainTextResponse(body, media_type=CONTENT_TYPE)

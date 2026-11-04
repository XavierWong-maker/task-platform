from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from task_platform.alerts import AlertRules, evaluate_alerts
from task_platform.api.deps import get_alert_rules, get_queue, get_session
from task_platform.api.schemas import AlertResponse, AlertsResponse
from task_platform.domain.models import utc_now
from task_platform.infrastructure.queue.base import TaskQueue
from task_platform.infrastructure.queue.factory import get_queue_stream

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("", response_model=AlertsResponse)
def list_alerts(
    session: Annotated[Session, Depends(get_session)],
    queue: Annotated[TaskQueue | None, Depends(get_queue)],
    rules: Annotated[AlertRules, Depends(get_alert_rules)],
) -> AlertsResponse:
    """当前正在触发的告警（实时评估，不保存历史）"""
    alerts = evaluate_alerts(session, queue, rules=rules, queue_name=get_queue_stream())
    return AlertsResponse(
        generated_at=utc_now(),
        alerts=[
            AlertResponse(
                rule=a.rule,
                subject=a.subject,
                severity=a.severity.value,
                message=a.message,
                value=a.value,
                threshold=a.threshold,
            )
            for a in alerts
        ],
    )

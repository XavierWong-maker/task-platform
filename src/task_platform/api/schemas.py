"""API 请求/响应模型（与 domain / ORM 分离）"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from task_platform.domain.enums import (
    BackoffStrategy,
    JobStatus,
    JobType,
    NodeStatus,
    WorkflowStatus,
)


class RetryPolicySchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    max_attempts: int
    backoff: BackoffStrategy
    base_delay_seconds: float
    max_delay_seconds: float


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    job_type: JobType
    handler: str
    timeout_seconds: float
    retry_policy: RetryPolicySchema
    enabled: bool
    created_at: datetime


class PageResponse[T](BaseModel):
    items: list[T]
    total: int
    page: int
    page_size: int
    total_pages: int


class ErrorDetail(BaseModel):
    field: str
    message: str


class ErrorBody(BaseModel):
    code: str
    message: str
    details: list[ErrorDetail] = []


class ErrorResponse(BaseModel):
    error: ErrorBody


JobName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]


class RetryPolicyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=3, ge=1, le=20)
    backoff: BackoffStrategy = BackoffStrategy.FIXED
    base_delay_seconds: float = Field(default=1.0, ge=0)
    max_delay_seconds: float = Field(default=60.0, ge=0)

    @model_validator(mode="after")
    def _check_delays(self) -> Self:
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds 不能小于 base_delay_seconds")
        return self


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: JobName
    handler: str = Field(min_length=1, max_length=128)
    job_type: JobType = JobType.CUSTOM
    timeout_seconds: float = Field(default=30.0, gt=0)
    retry_policy: RetryPolicyIn = Field(default_factory=lambda: RetryPolicyIn())
    enabled: bool = True


class JobUpdate(BaseModel):
    """局部更新：只处理请求里出现的字段；handler / job_type 不可修改"""

    model_config = ConfigDict(extra="forbid")

    name: JobName | None = None
    timeout_seconds: float | None = Field(default=None, gt=0)
    retry_policy: RetryPolicyIn | None = None
    enabled: bool | None = None

    @model_validator(mode="after")
    def _check_fields(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("至少提供一个要修改的字段")
        for name in self.model_fields_set:
            if getattr(self, name) is None:
                raise ValueError(f"{name} 不能为 null")
        return self


class ExecutionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    job_id: str
    attempt: int
    status: JobStatus
    started_at: datetime | None
    finished_at: datetime | None
    duration_seconds: float
    error_message: str | None
    timeout_seconds: float | None
    max_attempts: int | None
    cancel_requested: bool


class RunDetailResponse(BaseModel):
    """一次运行（同一 run_id）的汇总：status 为最后一次尝试的状态"""

    run_id: str
    job_id: str
    status: JobStatus
    attempts: list[ExecutionResponse]


class JobLogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    attempt: int
    level: str
    message: str
    created_at: datetime


class CancelResponse(BaseModel):
    run_id: str
    outcome: str


class JobMetricsResponse(BaseModel):
    job: str
    status_counts: dict[str, int]
    retry_attempts: int
    success_rate: float | None
    failure_rate: float | None
    avg_duration_seconds: float | None
    max_duration_seconds: float


class QueueMetricsResponse(BaseModel):
    depth: int | None
    pending: int | None


class MetricsResponse(BaseModel):
    generated_at: datetime
    window_seconds: float | None
    jobs: list[JobMetricsResponse]
    queue: QueueMetricsResponse


class AlertResponse(BaseModel):
    rule: str
    subject: str
    severity: str
    message: str
    value: float
    threshold: float


class AlertsResponse(BaseModel):
    generated_at: datetime
    alerts: list[AlertResponse]


class WorkflowNodeSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    job: str = Field(min_length=1, max_length=128)
    depends_on: list[str] = Field(default_factory=list)


class WorkflowPut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nodes: list[WorkflowNodeSchema] = Field(min_length=1)


class WorkflowResponse(BaseModel):
    name: str
    nodes: list[WorkflowNodeSchema]


class NodeRunResponse(BaseModel):
    name: str
    status: NodeStatus
    attempts: int
    job_run_id: str | None
    reused: bool
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None


class WorkflowRunResponse(BaseModel):
    run_id: str
    workflow: str
    status: WorkflowStatus
    parent_run_id: str | None
    started_at: datetime | None
    finished_at: datetime | None
    nodes: list[NodeRunResponse]


class WorkflowRunSummaryResponse(BaseModel):
    run_id: str
    workflow: str
    status: WorkflowStatus
    parent_run_id: str | None
    started_at: datetime
    finished_at: datetime | None


class RerunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node: str = Field(min_length=1)

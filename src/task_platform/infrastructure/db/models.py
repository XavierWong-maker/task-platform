"""ORM 模型（持久化表结构）。与 domain 层的 dataclass 分离，映射在 Repository 中完成"""

from __future__ import annotations

from datetime import datetime, time
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Enum,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from task_platform.domain.enums import (
    BackoffStrategy,
    JobStatus,
    JobType,
    NodeStatus,
    ScheduleType,
    WorkflowStatus,
)
from task_platform.infrastructure.db.base import Base, UTCDateTime


class JobRecord(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    job_type: Mapped[JobType] = mapped_column(Enum(JobType, native_enum=False, length=32))
    handler: Mapped[str] = mapped_column(String(128))
    timeout_seconds: Mapped[float] = mapped_column(Float)
    # RetryPolicy 值对象打平成列
    retry_max_attempts: Mapped[int] = mapped_column(Integer)
    retry_backoff: Mapped[BackoffStrategy] = mapped_column(
        Enum(BackoffStrategy, native_enum=False, length=16)
    )
    retry_base_delay_seconds: Mapped[float] = mapped_column(Float)
    retry_max_delay_seconds: Mapped[float] = mapped_column(Float)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class ScheduleRecord(Base):
    __tablename__ = "schedules"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    schedule_type: Mapped[ScheduleType] = mapped_column(
        Enum(ScheduleType, native_enum=False, length=16)
    )
    interval_seconds: Mapped[float | None] = mapped_column(Float)
    time_of_day: Mapped[time | None] = mapped_column(Time)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class ExecutionRecord(Base):
    """一次"尝试"对应一行；同一次运行的多次重试共享 run_id，attempt 递增"""

    __tablename__ = "job_executions"
    __table_args__ = (
        UniqueConstraint("run_id", "attempt", name="uq_job_executions_run_attempt"),
        Index("ix_job_executions_job_id_status", "job_id", "status"),
        Index("ix_job_executions_next_retry_at", "next_retry_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(32))
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    attempt: Mapped[int] = mapped_column(Integer)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, native_enum=False, length=16))
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    error_message: Mapped[str | None] = mapped_column(Text)
    timeout_seconds: Mapped[float | None] = mapped_column(Float)
    max_attempts: Mapped[int | None] = mapped_column(Integer)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class JobLogRecord(Base):
    """运行日志；通过 (run_id, attempt) 复合外键归属到某次尝试，随 execution 级联删除"""

    __tablename__ = "job_logs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "attempt"],
            ["job_executions.run_id", "job_executions.attempt"],
            ondelete="CASCADE",
        ),
        Index("ix_job_logs_run_id_attempt", "run_id", "attempt"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(32))
    attempt: Mapped[int] = mapped_column(Integer)
    level: Mapped[str] = mapped_column(String(16))
    message: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class IdempotencyRecord(Base):
    """幂等键认领记录：key 唯一，记录是哪次 run 认领了它"""

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    run_id: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class WorkflowRecord(Base):
    """工作流定义：节点与依赖作为一个整体（JSON）读写，不拆成多张表"""

    __tablename__ = "workflows"

    name: Mapped[str] = mapped_column(String(128), primary_key=True)
    definition: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)


class WorkflowRunRecord(Base):
    """一次工作流运行。definition 是运行时刻的定义快照；status 由节点状态派生后写入"""

    __tablename__ = "workflow_runs"
    __table_args__ = (
        Index("ix_workflow_runs_workflow_name_started_at", "workflow_name", "started_at"),
    )

    run_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    workflow_name: Mapped[str] = mapped_column(ForeignKey("workflows.name", ondelete="CASCADE"))
    status: Mapped[WorkflowStatus] = mapped_column(
        Enum(WorkflowStatus, native_enum=False, length=16)
    )
    definition: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    parent_run_id: Mapped[str | None] = mapped_column(String(32))


class WorkflowNodeRunRecord(Base):
    """一次工作流运行中某个节点的状态。job_run_id 指向该节点任务的 run_id
    （对应 job_executions 里的多次尝试），不是外键：run_id 单列不唯一"""

    __tablename__ = "workflow_node_runs"

    workflow_run_id: Mapped[str] = mapped_column(
        ForeignKey("workflow_runs.run_id", ondelete="CASCADE"), primary_key=True
    )
    node_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    position: Mapped[int] = mapped_column(Integer)
    status: Mapped[NodeStatus] = mapped_column(Enum(NodeStatus, native_enum=False, length=16))
    job_run_id: Mapped[str | None] = mapped_column(String(32))
    attempts: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    error_message: Mapped[str | None] = mapped_column(Text)
    reused: Mapped[bool] = mapped_column(Boolean, default=False)

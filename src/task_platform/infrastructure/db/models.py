"""ORM 模型（持久化表结构）。与 domain 层的 dataclass 分离，映射在 Repository 中完成"""

from __future__ import annotations

from datetime import datetime, time

from sqlalchemy import (
    Boolean,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from task_platform.domain.enums import BackoffStrategy, JobStatus, JobType, ScheduleType
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

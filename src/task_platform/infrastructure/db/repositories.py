"""Repository：隔离数据库访问，对外只收发领域对象。

约定：
- Repository 不提交事务，事务边界由调用方（session_scope）控制
- 写操作后立即 flush()，让唯一约束/外键错误在调用点暴露，而不是拖到 commit
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.mappers import (
    execution_to_record,
    job_to_record,
    record_to_execution,
    record_to_job,
    record_to_schedule,
    schedule_to_record,
    update_execution_record,
    update_job_record,
    update_schedule_record,
)
from task_platform.infrastructure.db.models import ExecutionRecord, JobRecord, ScheduleRecord

T = TypeVar("T")


@dataclass(slots=True)
class Page(Generic[T]):
    """一页查询结果; total 是过滤条件下的总数, 不是当前页的行数"""

    items: list[T]
    total: int
    page: int
    page_size: int

    @property
    def total_pages(self) -> int:
        if self.page_size <= 0:
            return 0
        return -(-self.total // self.page_size)  # 向上取整


def _validate_pagination(page: int, page_size: int) -> None:
    if page < 1:
        raise ValueError("page must be >= 1")
    if page_size < 1:
        raise ValueError("page_size must be >= 1")


class EntityNotFoundError(Exception):
    """更新一个不存在的实时抛出"""

    def __init__(self, kind: str, key: str) -> None:
        super().__init__(f"{kind} 不存在: {key}")
        self.kind = kind
        self.key = key


class JobRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, job: Job) -> None:
        self._session.add(job_to_record(job))
        self._session.flush()

    def get(self, job_id: str) -> Job | None:
        record = self._session.get(JobRecord, job_id)
        return record_to_job(record) if record is not None else None

    def get_by_name(self, name: str) -> Job | None:
        record = self._session.scalar(select(JobRecord).where(JobRecord.name == name))
        return record_to_job(record) if record is not None else None

    def list_all(self) -> list[Job]:
        stmt = select(JobRecord).order_by(JobRecord.created_at, JobRecord.name)
        return [record_to_job(r) for r in self._session.scalars(stmt)]

    def update(self, job: Job) -> None:
        record = self._session.get(JobRecord, job.id)
        if record is None:
            raise EntityNotFoundError("job", job.id)
        update_job_record(record, job)
        self._session.flush()

    def delete(self, job_id: str) -> bool:
        """删除 job；数据库外键 ON DELETE CASCADE 会一并清理其 schedules / executions。
        返回是否真的删除了一行。
        """
        record = self._session.get(JobRecord, job_id)
        if record is None:
            return False
        self._session.delete(record)
        self._session.flush()
        return True

    def list_paginated(
        self, *, enabled: bool | None = None, page: int = 1, page_size: int = 20
    ) -> Page[Job]:
        _validate_pagination(page, page_size)

        filters = [] if enabled is None else [JobRecord.enabled == enabled]

        count_stmt = select(func.count()).select_from(JobRecord)
        stmt = select(JobRecord).order_by(JobRecord.created_at, JobRecord.name)
        for condition in filters:
            count_stmt = count_stmt.where(condition)
            stmt = stmt.where(condition)

        total = self._session.scalar(count_stmt) or 0
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        items = [record_to_job(r) for r in self._session.scalars(stmt)]
        return Page(items=items, total=total, page=page, page_size=page_size)


class ScheduleRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, schedule: Schedule) -> None:
        self._session.add(schedule_to_record(schedule))
        self._session.flush()

    def get(self, schedule_id: str) -> Schedule | None:
        record = self._session.get(ScheduleRecord, schedule_id)
        return record_to_schedule(record) if record is not None else None

    def list_by_job(self, job_id: str) -> list[Schedule]:
        stmt = select(ScheduleRecord).where(ScheduleRecord.job_id == job_id)
        return [record_to_schedule(r) for r in self._session.scalars(stmt)]

    def update(self, schedule: Schedule) -> None:
        record = self._session.get(ScheduleRecord, schedule.id)
        if record is None:
            raise EntityNotFoundError("Schedule", schedule.id)
        update_schedule_record(record, schedule)
        self._session.flush()

    def delete(self, schedule_id: str) -> bool:
        record = self._session.get(ScheduleRecord, schedule_id)
        if record is None:
            return False
        self._session.delete(record)
        self._session.flush()
        return True


class ExecutionRepository:
    """一次"尝试"一行，自然键是 (run_id, attempt)"""

    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, execution: JobExecution) -> None:
        self._session.add(execution_to_record(execution))
        self._session.flush()

    def get_attempt(self, run_id: str, attempt: int) -> JobExecution | None:
        record = self._find(run_id, attempt)
        return record_to_execution(record) if record is not None else None

    def list_by_run(self, run_id: str) -> list[JobExecution]:
        stmt = (
            select(ExecutionRecord)
            .where(ExecutionRecord.run_id == run_id)
            .order_by(ExecutionRecord.attempt)
        )
        return [record_to_execution(r) for r in self._session.scalars(stmt)]

    def update(self, execution: JobExecution) -> None:
        record = self._find(execution.run_id, execution.attempt)
        if record is None:
            raise EntityNotFoundError("JobExecution", f"{execution.run_id}#{execution.attempt}")
        update_execution_record(record, execution)
        self._session.flush()

    def _find(self, run_id: str, attempt: int) -> ExecutionRecord | None:
        stmt = select(ExecutionRecord).where(
            ExecutionRecord.run_id == run_id,
            ExecutionRecord.attempt == attempt,
        )
        return self._session.scalar(stmt)

    def list_paginated(
        self,
        *,
        job_id: str | None = None,
        status: JobStatus | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[JobExecution]:
        """按 job_id/status/started_at 区间过滤，id DESC 排序（近似"最近的在前"）"""
        _validate_pagination(page, page_size)

        filters = []
        if job_id is not None:
            filters.append(ExecutionRecord.job_id == job_id)
        if status is not None:
            filters.append(ExecutionRecord.status == status)
        if started_after is not None:
            filters.append(ExecutionRecord.started_at >= started_after)
        if started_before is not None:
            filters.append(ExecutionRecord.started_at <= started_before)

        count_stmt = select(func.count()).select_from(ExecutionRecord)
        stmt = select(ExecutionRecord).order_by(ExecutionRecord.id.desc())
        for condition in filters:
            count_stmt = count_stmt.where(condition)
            stmt = stmt.where(condition)

        total = self._session.scalar(count_stmt) or 0
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        items = [record_to_execution(r) for r in self._session.scalars(stmt)]
        return Page(items=items, total=total, page=page, page_size=page_size)

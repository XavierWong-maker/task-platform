"""Repository：隔离数据库访问，对外只收发领域对象。

约定：
- Repository 不提交事务，事务边界由调用方（session_scope）控制
- 写操作后立即 flush()，让唯一约束/外键错误在调用点暴露，而不是拖到 commit
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar, cast

from sqlalchemy import CursorResult, delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, aliased

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import Job, JobExecution, JobLog, utc_now
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.mappers import (
    execution_to_record,
    job_to_record,
    log_to_record,
    record_to_execution,
    record_to_job,
    record_to_log,
    record_to_schedule,
    schedule_to_record,
    update_execution_record,
    update_job_record,
    update_schedule_record,
)
from task_platform.infrastructure.db.models import (
    ExecutionRecord,
    IdempotencyRecord,
    JobLogRecord,
    JobRecord,
    ScheduleRecord,
)

T = TypeVar("T")


@dataclass(slots=True)
class Page[T]:
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

    def upsert(self, execution: JobExecution) -> None:
        """已存在 (run_id, attempt) 则更新，否则插入：提前落库后，结束时走更新分支"""
        if self._find(execution.run_id, execution.attempt) is None:
            self.add(execution)
        else:
            self.update(execution)

    def list_by_status(self, status: JobStatus) -> list[JobExecution]:
        stmt = (
            select(ExecutionRecord)
            .where(ExecutionRecord.status == status)
            .order_by(ExecutionRecord.id)
        )
        return [record_to_execution(r) for r in self._session.scalars(stmt)]

    def list_dangling_retries(self) -> list[JobExecution]:
        """状态为 RETRYING 且同一 run 没有更高 attempt 的记录：重试链在内存排期丢失后留下的残骸"""
        later = aliased(ExecutionRecord)
        superseded = (
            select(later.id)
            .where(later.run_id == ExecutionRecord.run_id, later.attempt > ExecutionRecord.attempt)
            .exists()
        )
        stmt = (
            select(ExecutionRecord)
            .where(ExecutionRecord.status == JobStatus.RETRYING, ~superseded)
            .order_by(ExecutionRecord.id)
        )
        return [record_to_execution(r) for r in self._session.scalars(stmt)]

    def mark_cancel_requested(self, run_id: str, attempt: int) -> bool:
        record = self._find(run_id, attempt)
        if record is None:
            return False
        record.cancel_requested = True
        self._session.flush()
        return True

    def list_cancel_requested(self, run_ids: Collection[str]) -> list[str]:
        """给定 run_id 中：当前状态 RUNNING 且被请求取消"""
        if not run_ids:
            return []
        stmt = (
            select(ExecutionRecord.run_id)
            .where(
                ExecutionRecord.run_id.in_(list(run_ids)),
                ExecutionRecord.status == JobStatus.RUNNING,
                ExecutionRecord.cancel_requested.is_(True),
            )
            .distinct()
        )
        return list(self._session.scalars(stmt))

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


class JobLogRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_many(self, logs: list[JobLog]) -> None:
        """批量写入：对应的 execution 必须已在同一事务内 add 过(复合外键约束)"""
        self._session.add_all([log_to_record(log) for log in logs])
        self._session.flush()

    def list_by_run(
        self,
        run_id: str,
        *,
        attempt: int | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> Page[JobLog]:
        _validate_pagination(page, page_size)

        filters = [JobLogRecord.run_id == run_id]
        if attempt is not None:
            filters.append(JobLogRecord.attempt == attempt)

        count_stmt = select(func.count()).select_from(JobLogRecord)
        stmt = select(JobLogRecord).order_by(JobLogRecord.id)
        for condition in filters:
            count_stmt = count_stmt.where(condition)
            stmt = stmt.where(condition)

        total = self._session.scalar(count_stmt) or 0
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)
        items = [record_to_log(r) for r in self._session.scalars(stmt)]
        return Page(items=items, total=total, page=page, page_size=page_size)


class IdempotencyRepository:
    """幂等键认领。原子性来自数据库主键冲突，而不是"先查再写"。"""

    def __init__(self, session: Session) -> None:
        self._session = session

    def try_claim(self, key: str, job_id: str, run_id: str) -> bool:
        """认领成功返回 True；键已被占用返回 False（不抛异常）"""
        dialect_insert = (
            pg_insert if self._session.get_bind().dialect.name == "postgresql" else sqlite_insert
        )

        stmt = (
            dialect_insert(IdempotencyRecord)
            .values(key=key, job_id=job_id, run_id=run_id, created_at=utc_now())
            .on_conflict_do_nothing(index_elements=["key"])
        )
        result = cast("CursorResult[Any]", self._session.execute(stmt))
        return result.rowcount == 1

    def get_run_id(self, key: str) -> str | None:
        return self._session.scalar(
            select(IdempotencyRecord.run_id).where(IdempotencyRecord.key == key)
        )

    def release(self, key: str) -> bool:
        """释放键（例如运行最终失败、允许人工重跑）；返回是否真的删除了一行"""
        result = cast(
            "CursorResult[Any]",
            self._session.execute(delete(IdempotencyRecord).where(IdempotencyRecord.key == key)),
        )
        return result.rowcount == 1

    def list_orphan_keys(self) -> list[str]:
        """已认领、但数据库里没有任何对应 run_id 的 execution：崩溃发生在认领与开始执行之间"""
        known_runs = select(ExecutionRecord.run_id)
        stmt = select(IdempotencyRecord.key).where(IdempotencyRecord.run_id.not_in(known_runs))
        return list(self._session.scalars(stmt))

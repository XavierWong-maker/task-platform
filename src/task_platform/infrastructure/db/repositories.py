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

from sqlalchemy import CursorResult, delete, func, select, update
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
        """状态为 RETRYING、同一 run 没有更高 attempt、且没有持久化重试排期的记录：
        排期丢失后留下的残骸。next_retry_at 不为空的行仍在等待 sweeper 投递，不算悬空。"""
        later = aliased(ExecutionRecord)
        superseded = (
            select(later.id)
            .where(later.run_id == ExecutionRecord.run_id, later.attempt > ExecutionRecord.attempt)
            .exists()
        )
        stmt = (
            select(ExecutionRecord)
            .where(
                ExecutionRecord.status == JobStatus.RETRYING,
                ExecutionRecord.next_retry_at.is_(None),
                ~superseded,
            )
            .order_by(ExecutionRecord.id)
        )
        return [record_to_execution(r) for r in self._session.scalars(stmt)]

    def list_due_retries(self, now: datetime, *, limit: int = 100) -> list[JobExecution]:
        """RETRYING 且重试时刻已到、尚未被认领的记录，最早到期的在前"""
        stmt = (
            select(ExecutionRecord)
            .where(
                ExecutionRecord.status == JobStatus.RETRYING,
                ExecutionRecord.next_retry_at.is_not(None),
                ExecutionRecord.next_retry_at <= now,
            )
            .order_by(ExecutionRecord.next_retry_at, ExecutionRecord.id)
            .limit(limit)
        )
        return [record_to_execution(r) for r in self._session.scalars(stmt)]

    def claim_retry(self, run_id: str, attempt: int) -> bool:
        """原子认领一条重试排期（把 next_retry_at 清空）；多个 sweeper 并发时只有一个返回 True"""
        stmt = (
            update(ExecutionRecord)
            .where(
                ExecutionRecord.run_id == run_id,
                ExecutionRecord.attempt == attempt,
                ExecutionRecord.next_retry_at.is_not(None),
            )
            .values(next_retry_at=None)
            .execution_options(synchronize_session=False)
        )
        result = cast("CursorResult[Any]", self._session.execute(stmt))
        return result.rowcount == 1

    def set_next_retry_at(self, run_id: str, attempt: int, value: datetime | None) -> bool:
        """直接改写重试排期（投递失败时用来把认领撤回）"""
        stmt = (
            update(ExecutionRecord)
            .where(ExecutionRecord.run_id == run_id, ExecutionRecord.attempt == attempt)
            .values(next_retry_at=value)
            .execution_options(synchronize_session=False)
        )
        result = cast("CursorResult[Any]", self._session.execute(stmt))
        return result.rowcount == 1

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

    def count_by_job_and_status(
        self, *, since: datetime | None = None
    ) -> list[tuple[str, JobStatus, int]]:
        """按 (任务名, 状态) 统计行数；since 不为空时只统计 started_at >= since 的记录"""
        stmt = (
            select(JobRecord.name, ExecutionRecord.status, func.count())
            .select_from(ExecutionRecord)
            .join(JobRecord, JobRecord.id == ExecutionRecord.job_id)
            .group_by(JobRecord.name, ExecutionRecord.status)
            .order_by(JobRecord.name, ExecutionRecord.status)
        )
        if since is not None:
            stmt = stmt.where(ExecutionRecord.started_at >= since)
        return [(name, status, int(n)) for name, status, n in self._session.execute(stmt)]

    def count_retry_attempts_by_job(self, *, since: datetime | None = None) -> dict[str, int]:
        """每个任务实际执行过的重试次数（attempt > 1 的行数）"""
        stmt = (
            select(JobRecord.name, func.count())
            .select_from(ExecutionRecord)
            .join(JobRecord, JobRecord.id == ExecutionRecord.job_id)
            .where(ExecutionRecord.attempt > 1)
            .group_by(JobRecord.name)
        )
        if since is not None:
            stmt = stmt.where(ExecutionRecord.started_at >= since)
        return {name: int(n) for name, n in self._session.execute(stmt)}

    def list_durations(self, *, since: datetime | None = None) -> list[tuple[str, float]]:
        """已结束（SUCCESS / FAILED）且有起止时间的尝试：(任务名, 耗时秒)"""
        stmt = (
            select(JobRecord.name, ExecutionRecord.started_at, ExecutionRecord.finished_at)
            .select_from(ExecutionRecord)
            .join(JobRecord, JobRecord.id == ExecutionRecord.job_id)
            .where(
                ExecutionRecord.status.in_([JobStatus.SUCCESS, JobStatus.FAILED]),
                ExecutionRecord.started_at.is_not(None),
                ExecutionRecord.finished_at.is_not(None),
            )
        )
        if since is not None:
            stmt = stmt.where(ExecutionRecord.started_at >= since)
        durations: list[tuple[str, float]] = []

        for name, started, finished in self._session.execute(stmt):
            if started is None or finished is None:
                continue
            durations.append((name, (finished - started).total_seconds()))
        return durations

    def recent_final_statuses(self, job_id: str, *, limit: int) -> list[JobStatus]:
        """某任务最近 limit 条"最终结果"（SUCCESS / FAILED），最新的在前（按 id DESC）"""
        stmt = (
            select(ExecutionRecord.status)
            .where(
                ExecutionRecord.job_id == job_id,
                ExecutionRecord.status.in_([JobStatus.SUCCESS, JobStatus.FAILED]),
            )
            .order_by(ExecutionRecord.id.desc())
            .limit(limit)
        )
        return list(self._session.scalars(stmt))

    def count_timeouts_by_job(self, prefix: str, *, since: datetime) -> dict[str, int]:
        """started_at >= since 且 error_message 以 prefix 开头的尝试数（按任务名）"""
        stmt = (
            select(JobRecord.name, func.count())
            .select_from(ExecutionRecord)
            .join(JobRecord, JobRecord.id == ExecutionRecord.job_id)
            .where(
                ExecutionRecord.error_message.startswith(prefix, autoescape=True),
                ExecutionRecord.started_at >= since,
            )
            .group_by(JobRecord.name)
        )
        return {name: int(n) for name, n in self._session.execute(stmt)}

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
        """认领成功返回 True；键已被占用返回 False（不抛异常）。
        用 RETURNING 判断输赢：ON CONFLICT DO NOTHING 只有真正插入了才会返回行，
        不依赖 rowcount（ORM 方式执行的 INSERT 在 PostgreSQL 上 rowcount 不可靠）。"""
        dialect_insert = (
            pg_insert if self._session.get_bind().dialect.name == "postgresql" else sqlite_insert
        )

        stmt = (
            dialect_insert(IdempotencyRecord)
            .values(key=key, job_id=job_id, run_id=run_id, created_at=utc_now())
            .on_conflict_do_nothing(index_elements=["key"])
            .returning(IdempotencyRecord.key)
        )
        return self._session.execute(stmt).first() is not None

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

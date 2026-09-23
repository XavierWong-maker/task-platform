"""领域对象 <-> ORM Record 的转换。纯函数，不接触 Session。"""

from __future__ import annotations

from task_platform.domain.models import Job, JobExecution, RetryPolicy
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.models import ExecutionRecord, JobRecord, ScheduleRecord

# ---------- Job ----------


def job_to_record(job: Job) -> JobRecord:
    return JobRecord(
        id=job.id,
        name=job.name,
        job_type=job.job_type,
        handler=job.handler,
        timeout_seconds=job.timeout_seconds,
        retry_max_attempts=job.retry_policy.max_attempts,
        retry_backoff=job.retry_policy.backoff,
        retry_base_delay_seconds=job.retry_policy.base_delay_seconds,
        retry_max_delay_seconds=job.retry_policy.max_delay_seconds,
        enabled=job.enabled,
        created_at=job.created_at,
    )


def update_job_record(record: JobRecord, job: Job) -> None:
    """把 job 的可变字段写回 record（id / created_at 不可变，不更新）"""
    record.name = job.name
    record.job_type = job.job_type
    record.handler = job.handler
    record.timeout_seconds = job.timeout_seconds
    record.retry_max_attempts = job.retry_policy.max_attempts
    record.retry_backoff = job.retry_policy.backoff
    record.retry_base_delay_seconds = job.retry_policy.base_delay_seconds
    record.retry_max_delay_seconds = job.retry_policy.max_delay_seconds
    record.enabled = job.enabled


def record_to_job(record: JobRecord) -> Job:
    return Job(
        name=record.name,
        job_type=record.job_type,
        handler=record.handler,
        id=record.id,
        timeout_seconds=record.timeout_seconds,
        retry_policy=RetryPolicy(
            max_attempts=record.retry_max_attempts,
            backoff=record.retry_backoff,
            base_delay_seconds=record.retry_base_delay_seconds,
            max_delay_seconds=record.retry_max_delay_seconds,
        ),
        enabled=record.enabled,
        created_at=record.created_at,
    )


# ---------- Schedule ----------


def schedule_to_record(schedule: Schedule) -> ScheduleRecord:
    return ScheduleRecord(
        id=schedule.id,
        job_id=schedule.job_id,
        schedule_type=schedule.schedule_type,
        interval_seconds=schedule.interval_seconds,
        time_of_day=schedule.time_of_day,
        enabled=schedule.enabled,
    )


def update_schedule_record(record: ScheduleRecord, schedule: Schedule) -> None:
    record.schedule_type = schedule.schedule_type
    record.interval_seconds = schedule.interval_seconds
    record.time_of_day = schedule.time_of_day
    record.enabled = schedule.enabled


def record_to_schedule(record: ScheduleRecord) -> Schedule:
    return Schedule(
        job_id=record.job_id,
        schedule_type=record.schedule_type,
        id=record.id,
        interval_seconds=record.interval_seconds,
        time_of_day=record.time_of_day,
        enabled=record.enabled,
    )


# ---------- JobExecution ----------


def execution_to_record(execution: JobExecution) -> ExecutionRecord:
    return ExecutionRecord(
        run_id=execution.run_id,
        job_id=execution.job_id,
        attempt=execution.attempt,
        status=execution.status,
        started_at=execution.started_at,
        finished_at=execution.finished_at,
        error_message=execution.error_message,
        timeout_seconds=execution.timeout_seconds,
        max_attempts=execution.max_attempts,
    )


def update_execution_record(record: ExecutionRecord, execution: JobExecution) -> None:
    """(run_id, attempt) 是自然键，不更新；其余字段写回"""
    record.status = execution.status
    record.started_at = execution.started_at
    record.finished_at = execution.finished_at
    record.error_message = execution.error_message
    record.timeout_seconds = execution.timeout_seconds
    record.max_attempts = execution.max_attempts


def record_to_execution(record: ExecutionRecord) -> JobExecution:
    return JobExecution(
        job_id=record.job_id,
        run_id=record.run_id,
        status=record.status,
        attempt=record.attempt,
        started_at=record.started_at,
        finished_at=record.finished_at,
        error_message=record.error_message,
        timeout_seconds=record.timeout_seconds,
        max_attempts=record.max_attempts,
    )

"""
应用启动装配：发现并注册所有 jobs/ 目录下的任务，构建 Job 列表；
现阶段新增与数据库的对账逻辑（sync_jobs_to_db）和加载调度组合（load_active_schedules）

build_registry() 改为 discover_jobs() 自动发现；
build_jobs() 拆成两部分——
    1) 显式定义的演示任务
    2) registry 中其余"被发现但没有显式定义"的任务，自动生成 Job
        （这正是 example_new_job 这类新增任务无需改这个文件就能被CLI 看到的关键）

sync_jobs_to_db()：build_jobs() 每次调用都会生成全新的 Job（新 id、新 created_at），
如果直接持久化会导致每次进程启动 Job 都"变成新的一行"——重启数据不丢的前提是
按 name 对账：
    - 数据库里没有的：按代码定义插入
    - 数据库里已有的：保留其 id / enabled / created_at（这些是"运行时状态"，
      不该被代码重新定义覆盖），但 handler / job_type / timeout / retry_policy
      始终以代码为准写回——保证代码里的任务定义变更能够生效
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from task_platform import jobs as jobs_package
from task_platform.domain.enums import JobType
from task_platform.domain.models import Job
from task_platform.domain.schedule import Schedule
from task_platform.infrastructure.db.repositories import JobRepository, ScheduleRepository
from task_platform.jobs.discovery import discover_jobs
from task_platform.registry import RegisteredHandler, TaskRegistry, default_registry

# 阶段 1 遗留的三个演示任务，显式建立 CLI 展示名到 handler 的映射
# 只是为了保留原有的 CLI 名称（demo-success 等）；
# 不在这里出现的任务（比如 demo.example_new）会走下面的自动生成逻辑
_EXPLICIT_JOB_DEFINITIONS: dict[str, JobType] = {
    "demo.success": JobType.DEMO_SUCCESS,
    "demo.failure": JobType.DEMO_FAILURE,
    "demo.slow": JobType.DEMO_SLOW,
}


def build_registry() -> TaskRegistry:
    """发现 jobs/ 目录下所有任务并注册进 default_registry，然后返回它"""
    discover_jobs(jobs_package)
    return default_registry


def _auto_job_name(handler_name: str) -> str:
    """由 handler 名称推导一个 CLI 展示用的 Job.name（把点替换为短横线）"""
    return handler_name.replace(".", "-")


def _build_job_from_handler(handler: RegisteredHandler) -> Job:
    """为一个已注册但没有显式 Job 定义的 handler，自动生成一个 Job 对象

    timeout_seconds / retry_policy 若 handler 上有元数据（来自 @job 装饰器）
    则直接采用；否则退回 Job 的类默认值
    """
    job = Job(
        name=_auto_job_name(handler.name),
        job_type=JobType.CUSTOM,
        handler=handler.name,
    )
    if handler.timeout_seconds is not None:
        job.timeout_seconds = handler.timeout_seconds
    if handler.retry_policy is not None:
        job.retry_policy = handler.retry_policy
    return job


def build_jobs(registry: TaskRegistry) -> dict[str, Job]:
    """构建 Job 列表：显式定义的演示任务 + registry 中其余任务自动生成的 Job
    注意：每次调用都会生成全新的 Job 对象（新 id、新 created_at）——
    这是"代码里期望的任务定义"，不代表数据库里的真实状态，
    需要经过 sync_jobs_to_db() 对账后才能安全持久化
    Args:
        registry: 已完成 discover_jobs() 的 TaskRegistry（通常是 build_registry() 的返回值）
    """
    jobs: dict[str, Job] = {}

    for handler_name, job_type in _EXPLICIT_JOB_DEFINITIONS.items():
        if not registry.is_registered(handler_name):
            continue
        name = _auto_job_name(handler_name)
        jobs[name] = Job(name=name, job_type=job_type, handler=handler_name)

    for handler_name in registry.list_names():
        if handler_name in _EXPLICIT_JOB_DEFINITIONS:
            continue
        handler = registry.get(handler_name)
        auto_job = _build_job_from_handler(handler)
        jobs[auto_job.name] = auto_job

    return jobs


def sync_jobs_to_db(registry: TaskRegistry, session: Session) -> dict[str, Job]:
    """把代码里发现的 Job 定义与数据库对账，返回数据库里的真实状态

    - 数据库里没有的（新任务）：按代码定义插入
    - 数据库里已有的（按 name 匹配）：保留其 id / enabled / created_at
      （这些是运行时状态，例如用户手工 pause 过某个任务），但
      handler / job_type / timeout_seconds / retry_policy 始终以代码为准覆盖写回

    调用方应在一个 session_scope 内调用本函数，保证对账过程本身是一次原子事务。
    """
    desired = build_jobs(registry)
    repo = JobRepository(session)
    synced: dict[str, Job] = {}

    for name, code_job in desired.items():
        existing = repo.get_by_name(name)
        if existing is None:
            repo.add(code_job)
            synced[name] = code_job
        else:
            existing.job_type = code_job.job_type
            existing.handler = code_job.handler
            existing.timeout_seconds = code_job.timeout_seconds
            existing.retry_policy = code_job.retry_policy
            repo.update(existing)
            synced[name] = existing

    return synced


def load_active_schedules(session: Session) -> list[tuple[Job, Schedule]]:
    """加载所有"任务已启用 且 调度规则已启用"的 (Job, Schedule) 组合，
    供 Scheduler.add() 逐一注册;
    """
    job_repo = JobRepository(session)
    schedule_repo = ScheduleRepository(session)
    pairs: list[tuple[Job, Schedule]] = []
    for job in job_repo.list_all():
        if not job.enabled:
            continue
        for schedule in schedule_repo.list_by_job(job.id):
            if schedule.enabled:
                pairs.append((job, schedule))
    return pairs

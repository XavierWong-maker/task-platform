"""
任务平台 CLI（阶段 4 版本）

用法：
    python -m task_platform.cli.main list
    python -m task_platform.cli.main show demo-success
    python -m task_platform.cli.main run demo-success
    python -m task_platform.cli.main run demo-failure
    python -m task_platform.cli.main cancel demo-slow
    python -m task_platform.cli.main schedule demo-success --interval 5
    python -m task_platform.cli.main schedule demo-success --daily 01:00
    python -m task_platform.cli.main serve

变化：
- Job 定义通过 bootstrap.sync_jobs_to_db() 与数据库对账：数据库里没有的任务
  按代码定义插入；已存在的任务保留其 id/enabled/created_at（用户可控状态与历史），
  但 handler/timeout/retry_policy 始终以代码为准写回数据库。
- run/cancel/serve 触发的每一次 JobExecution 都会持久化，服务重启后执行历史不丢。
- serve 不再硬编码调度演示任务，而是加载数据库中所有"任务已启用且调度已启用"的
  (Job, Schedule) 组合；需要先用 `schedule` 子命令为任务添加调度规则
  （因此原来的 --interval 演示参数已从 serve 中移除）。
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import time as time_cls

from sqlalchemy.orm import Session, sessionmaker

from task_platform.bootstrap import build_registry, load_active_schedules, sync_jobs_to_db
from task_platform.domain.enums import JobStatus, ScheduleType
from task_platform.domain.models import Job, JobExecution
from task_platform.domain.schedule import Schedule
from task_platform.executor import (
    cancel_before_start,
    execute_attempt,
    resolve_retry_policy,
    resolve_timeout_seconds,
)
from task_platform.infrastructure.db.migrate import upgrade_db
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    JobLogRepository,
    ScheduleRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    get_database_url,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.logging.collector import LogCollector, collecting_logs
from task_platform.registry import TaskRegistry
from task_platform.scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="task-platform", description="任务调度平台 CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="列出所有已注册的任务")

    show_parser = subparsers.add_parser("show", help="查看任务详情")
    show_parser.add_argument("name", help="任务名称")

    run_parser = subparsers.add_parser("run", help="立即运行一个任务")
    run_parser.add_argument("name", help="任务名称")

    cancel_parser = subparsers.add_parser("cancel", help="取消一个尚未开始运行的任务")
    cancel_parser.add_argument("name", help="任务名称")

    history_parser = subparsers.add_parser("history", help="查看执行历史（分页）")
    history_parser.add_argument("name", nargs="?", default=None, help="只看某个任务")
    history_parser.add_argument(
        "--status", choices=[s.value for s in JobStatus], default=None, help="按状态过滤"
    )
    history_parser.add_argument("--page", type=int, default=1)
    history_parser.add_argument("--page-size", type=int, default=20)

    logs_parser = subparsers.add_parser("logs", help="查看某次运行的日志")
    logs_parser.add_argument("run_id", help="run_id")
    logs_parser.add_argument("--attempt", type=int, default=None, help="只看某一次尝试")

    schedule_parser = subparsers.add_parser("schedule", help="为任务添加一条调度规则")
    schedule_parser.add_argument("name", help="任务名称")
    schedule_group = schedule_parser.add_mutually_exclusive_group(required=True)
    schedule_group.add_argument("--interval", type=float, help="固定间隔秒数")
    schedule_group.add_argument("--daily", type=str, help="每天固定时刻，格式 HH:MM")

    serve_parser = subparsers.add_parser(
        "serve", help="启动 Scheduler 主循环（阻塞运行，Ctrl+C 优雅退出）"
    )
    serve_parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Scheduler 主循环的轮询间隔",
    )

    return parser


def _setup_db() -> sessionmaker[Session]:
    url = get_database_url()
    upgrade_db(url)  # 通过 Alembic 迁移建表/升级，而不是 create_all
    return make_session_factory(create_db_engine(url))


def _persist_execution(
    factory: sessionmaker[Session], execution: JobExecution, collector: LogCollector
) -> None:
    """同一事务内先写 execution, 再写它的日志"""
    logs = collector.drain(execution.run_id, attempt=execution.attempt)
    with session_scope(factory) as s:
        ExecutionRepository(s).add(execution)
        if logs:
            JobLogRepository(s).add_many(logs)


def _parse_daily(value: str) -> time_cls:
    hour_str, _, minute_str = value.partition(":")
    return time_cls(int(hour_str), int(minute_str))


def main(argv: list[str] | None = None) -> int:
    with collecting_logs() as collector:
        return _main(argv, collector)


def _main(argv: list[str] | None, collector: LogCollector) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    factory = _setup_db()
    registry: TaskRegistry = build_registry()
    with session_scope(factory) as s:
        jobs: dict[str, Job] = sync_jobs_to_db(registry, s)

    if args.command == "list":
        if not jobs:
            print("暂无已注册任务")
            return 0
        for registered_job in jobs.values():
            print(
                f"{registered_job.name}\t{registered_job.job_type.value}\t"
                f"handler={registered_job.handler}\tid={registered_job.id}\t"
                f"enabled={registered_job.enabled}"
            )
        return 0

    if args.command == "history":
        job_id: str | None = None
        if args.name is not None:
            target = jobs.get(args.name)
            if target is None:
                print(f"未找到任务：{args.name}", file=sys.stderr)
                return 1
            job_id = target.id
        status = JobStatus(args.status) if args.status else None
        with session_scope(factory) as s:
            page = ExecutionRepository(s).list_paginated(
                job_id=job_id, status=status, page=args.page, page_size=args.page_size
            )
        for ex in page.items:
            started = ex.started_at.isoformat() if ex.started_at else "-"
            print(
                f"{ex.run_id}\tattempt={ex.attempt}\t{ex.status.value}\t"
                f"started={started}\tduration={ex.duration_seconds:.3f}s"
            )
        print(f"page {page.page}/{page.total_pages}  total={page.total}")
        return 0

    if args.command == "logs":
        with session_scope(factory) as s:
            log_page = JobLogRepository(s).list_by_run(
                args.run_id, attempt=args.attempt, page_size=1000
            )
        if not log_page.items:
            print(f"没有找到 run_id={args.run_id} 的日志", file=sys.stderr)
            return 1
        for entry in log_page.items:
            print(
                f"{entry.created_at.isoformat()} attempt={entry.attempt} "
                f"{entry.level} {entry.message}"
            )
        return 0

    if args.command == "schedule":
        job = jobs.get(args.name)
        if job is None:
            print(f"未找到任务：{args.name}", file=sys.stderr)
            return 1

        new_schedule: Schedule
        if args.interval is not None:
            new_schedule = Schedule(
                job_id=job.id, schedule_type=ScheduleType.INTERVAL, interval_seconds=args.interval
            )
        else:
            new_schedule = Schedule(
                job_id=job.id,
                schedule_type=ScheduleType.DAILY,
                time_of_day=_parse_daily(args.daily),
            )

        with session_scope(factory) as s:
            ScheduleRepository(s).add(new_schedule)
        print(
            f"schedule_id={new_schedule.id} job={job.name} type={new_schedule.schedule_type.value}"
        )
        return 0

    if args.command == "serve":
        with session_scope(factory) as s:
            pairs = load_active_schedules(s)

        if not pairs:
            print(
                "数据库中没有任何已启用的 (Job, Schedule)，无法启动调度。"
                "请先用 `schedule` 子命令为任务添加调度规则。",
                file=sys.stderr,
            )
            return 1

        scheduler = Scheduler(registry)
        for job, sched in pairs:
            scheduler.add(job, sched)

        def _on_tick(executions: list[JobExecution]) -> None:
            for execution in executions:
                _persist_execution(factory, execution, collector)

        print(f"Scheduler 已启动：{len(pairs)} 条 (job, schedule) 组合。按 Ctrl+C 优雅退出。")
        scheduler.run_forever(poll_interval_seconds=args.poll_interval, on_tick=_on_tick)
        print("Scheduler 已优雅退出。")
        return 0

    if args.command in ("show", "run", "cancel"):
        job = jobs.get(args.name)
        if job is None:
            print(f"未找到任务：{args.name}", file=sys.stderr)
            return 1

        if args.command == "show":
            handler = registry.get(job.handler)
            effective_timeout = resolve_timeout_seconds(job, handler)
            effective_retry = resolve_retry_policy(job, handler)
            print(f"name         : {job.name}")
            print(f"id           : {job.id}")
            print(f"job_type     : {job.job_type.value}")
            print(f"handler      : {job.handler}")
            print(f"description  : {handler.description or '(无)'}")
            print(f"timeout(s)   : {effective_timeout}")
            print(f"retry_policy : {effective_retry}")
            print(f"enabled      : {job.enabled}")
            return 0

        if args.command == "run":
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="cli-run") as pool:
                result = execute_attempt(job, registry, pool, attempt_number=1)
            execution = result.execution
            _persist_execution(factory, execution, collector)
            print(
                f"run_id={execution.run_id} status={execution.status.value} "
                f"attempt={execution.attempt} duration={execution.duration_seconds:.3f}s "
                f"timeout={execution.timeout_seconds}s max_attempts={execution.max_attempts}"
            )
            if execution.error_message:
                print(f"error={execution.error_message}", file=sys.stderr)
            return 0 if execution.status is JobStatus.SUCCESS else 1

        if args.command == "cancel":
            execution = cancel_before_start(job)
            _persist_execution(factory, execution, collector)
            print(f"run_id={execution.run_id} status={execution.status.value}")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

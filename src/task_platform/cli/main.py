"""
任务平台 CLI（阶段 2 版本）

用法：
    python -m task_platform.cli.main list
    python -m task_platform.cli.main show demo-success
    python -m task_platform.cli.main run demo-success
    python -m task_platform.cli.main run demo-failure
    python -m task_platform.cli.main cancel demo-slow
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

from task_platform.bootstrap import build_jobs, build_registry
from task_platform.domain.enums import JobStatus, ScheduleType
from task_platform.domain.schedule import Schedule
from task_platform.executor import (
    cancel_before_start,
    execute_attempt,
    resolve_retry_policy,
    resolve_timeout_seconds,
)
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

    serve_parser = subparsers.add_parser(
        "serve", help="启动 Scheduler 主循环（阻塞运行，Ctrl+C 优雅退出）"
    )
    serve_parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="演示任务的触发间隔",
    )
    serve_parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Scheduler 主循环的轮询间隔",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    registry = build_registry()
    jobs = build_jobs(registry)

    if args.command == "serve":
        # 占位实现：先阶段还没有持久化，暂时硬编码一个 demo-success 的 INTERVAL 调度
        # 下一阶段引入 SQLite/Repository 后，这里应该改为从数据库加载所有已启用的 (Job, Schedule)
        demo_job = jobs.get("demo-success")
        if demo_job is None:
            print("未找到 demo-success 任务，无法启动演示调度", file=sys.stderr)
            return 1

        scheduler = Scheduler(registry)
        schedule = Schedule(
            job_id=demo_job.id,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=args.interval,
        )
        scheduler.add(demo_job, schedule)
        print(
            f"Scheduler 已启动：demo-success 每 {args.interval}s 触发一次，"
            f"轮询间隔 {args.poll_interval}s。按 Ctrl+C 优雅退出。"
        )
        scheduler.run_forever(poll_interval_seconds=args.poll_interval)
        print("Scheduler 已优雅退出。")
        return 0

    if args.command == "list":
        if not jobs:
            print("暂无已注册任务")
            return 0
        for registered_job in jobs.values():
            print(
                f"{registered_job.name}\t{registered_job.job_type.value}\t"
                f"handler={registered_job.handler}\tid={registered_job.id}"
            )
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
            print(f"run_id={execution.run_id} status={execution.status.value}")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

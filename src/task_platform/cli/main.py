"""
任务平台 CLI（阶段 1 版本）

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

from task_platform.bootstrap import build_jobs, build_registry
from task_platform.executor import cancel_before_start, execute

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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    jobs = build_jobs()
    registry = build_registry()

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
            print(f"name         : {job.name}")
            print(f"id           : {job.id}")
            print(f"job_type     : {job.job_type.value}")
            print(f"handler      : {job.handler}")
            print(f"timeout(s)   : {job.timeout_seconds}")
            print(f"retry_policy : {job.retry_policy}")
            print(f"enabled      : {job.enabled}")
            return 0

        if args.command == "run":
            execution = execute(job, registry)
            print(
                f"run_id={execution.run_id} status={execution.status.value} "
                f"attempt={execution.attempt} duration={execution.duration_seconds:.3f}s"
            )

            if execution.error_message:
                print(f"error={execution.error_message}", file=sys.stderr)
            return 0 if execution.status.value else 1

        if args.command == "cancel":
            execution = cancel_before_start(job)
            print(f"run_id={execution.run_id} status={execution.status.value}")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""workflow 子命令：register / list / run / retry / rerun / history / show。
独立成模块，避免 cli/main.py 的 _main 继续膨胀。"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.cancel import CancelToken
from task_platform.domain.enums import NodeStatus, WorkflowStatus
from task_platform.domain.models import Job
from task_platform.domain.workflow import TaskNode, WorkflowDefinition, WorkflowValidationError
from task_platform.domain.workflow_replay import ReplayError
from task_platform.domain.workflow_run import WorkflowRun
from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.db.workflow_repositories import (
    WorkflowRepository,
    WorkflowRunRepository,
)
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.registry import TaskRegistry
from task_platform.runtime import stop_on_signals
from task_platform.workflow_engine import WorkflowEngine
from task_platform.workflow_service import (
    RunNotFoundError,
    rerun_node,
    retry_workflow,
    run_workflow,
)


def add_workflow_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("workflow", help="DAG 工作流：登记、运行、重试、重跑、历史")
    sub = parser.add_subparsers(dest="workflow_command", required=True)

    register = sub.add_parser("register", help="从 JSON 文件登记（或更新）工作流定义")
    register.add_argument(
        "file", help='JSON：{"name": "...", "nodes": [{"name", "job", "depends_on": [...]}]}'
    )

    sub.add_parser("list", help="列出已登记的工作流")

    run = sub.add_parser("run", help="运行一个已登记的工作流")
    run.add_argument("name")
    retry = sub.add_parser("retry", help="重试：沿用成功的节点，重新执行其余节点（新建一次运行）")
    retry.add_argument("run_id")
    rerun = sub.add_parser("rerun", help="重跑某个节点及其全部下游（新建一次运行）")
    rerun.add_argument("run_id")
    rerun.add_argument("node")
    for p in (run, retry, rerun):
        p.add_argument("--max-parallel", type=int, default=4, help="同时运行的节点数")

    history = sub.add_parser("history", help="工作流运行历史（分页）")
    history.add_argument("name", nargs="?", default=None, help="只看某个工作流")
    history.add_argument("--status", choices=[s.value for s in WorkflowStatus], default=None)
    history.add_argument("--page", type=int, default=1)
    history.add_argument("--page-size", type=int, default=20)

    show = sub.add_parser("show", help="查看一次运行的各节点状态")
    show.add_argument("run_id")


@contextmanager
def _cancel_token_on_signals() -> Iterator[CancelToken]:
    """Ctrl+C / SIGTERM -> 取消令牌：节点协作式取消，未开始的节点直接取消"""
    token = CancelToken()
    done = threading.Event()
    with stop_on_signals() as stop:

        def watch() -> None:
            while not done.is_set():
                if stop.wait(0.2):
                    token.cancel()
                    return

        thread = threading.Thread(target=watch, name="workflow-cancel-watch", daemon=True)
        thread.start()
        try:
            yield token
        finally:
            done.set()
            thread.join(timeout=1.0)


def _print_run(run: WorkflowRun) -> None:
    parent = f" parent={run.parent_run_id}" if run.parent_run_id else ""
    print(f"workflow_run_id={run.run_id} workflow={run.workflow} status={run.status.value}{parent}")
    for node in run.nodes.values():
        reused = " (reused)" if node.reused else ""
        print(
            f"  {node.name}\t{node.status.value}\tattempts={len(node.attempts)}\t"
            f"run_id={node.run_id or '-'}{reused}"
        )
        if node.error_message and node.status is not NodeStatus.SUCCESS:
            print(f"    {node.error_message}")


def _parse_definition(data: Any) -> WorkflowDefinition:
    if not isinstance(data, dict):
        raise ValueError("顶层必须是 JSON 对象")
    nodes = tuple(
        TaskNode(d["name"], d["job"], tuple(d.get("depends_on", ()))) for d in data["nodes"]
    )
    return WorkflowDefinition(data["name"], nodes)


def _register(factory: sessionmaker[Session], jobs: Mapping[str, Job], path: str) -> int:
    try:
        definition = _parse_definition(json.loads(Path(path).read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        print(f"工作流定义不合法：{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    missing = sorted({n.job for n in definition.nodes if n.job not in jobs})
    if missing:
        print(f"工作流引用了不存在的 Job：{missing}", file=sys.stderr)
        return 1
    with session_scope(factory) as s:
        WorkflowRepository(s).save(definition)
    print(f"workflow={definition.name} nodes={len(definition.nodes)}")
    return 0


def _execute(
    action: Callable[[WorkflowEngine, CancelToken], WorkflowRun],
    registry: TaskRegistry,
    jobs: Mapping[str, Job],
    max_parallel: int,
) -> int:
    try:
        engine = WorkflowEngine(registry, jobs, max_parallel=max_parallel)
        with _cancel_token_on_signals() as token:
            run = action(engine, token)
    except (RunNotFoundError, ReplayError, WorkflowValidationError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    _print_run(run)
    return 0 if run.status is WorkflowStatus.SUCCESS else 1


def run_workflow_command(
    args: argparse.Namespace,
    factory: sessionmaker[Session],
    registry: TaskRegistry,
    jobs: Mapping[str, Job],
    collector: LogCollector,
) -> int:
    command: str = args.workflow_command

    if command == "register":
        return _register(factory, jobs, args.file)

    if command == "list":
        with session_scope(factory) as s:
            names = WorkflowRepository(s).list_names()
        for name in names:
            print(name)
        return 0

    if command == "run":
        with session_scope(factory) as s:
            definition = WorkflowRepository(s).get(args.name)
        if definition is None:
            print(f"未找到工作流：{args.name}", file=sys.stderr)
            return 1
        return _execute(
            lambda engine, token: run_workflow(
                engine, definition, factory, collector, cancel_token=token
            ),
            registry,
            jobs,
            args.max_parallel,
        )

    if command == "retry":
        return _execute(
            lambda engine, token: retry_workflow(
                engine, args.run_id, factory, collector, cancel_token=token
            ),
            registry,
            jobs,
            args.max_parallel,
        )

    if command == "rerun":
        return _execute(
            lambda engine, token: rerun_node(
                engine, args.run_id, args.node, factory, collector, cancel_token=token
            ),
            registry,
            jobs,
            args.max_parallel,
        )

    if command == "history":
        status = WorkflowStatus(args.status) if args.status else None
        with session_scope(factory) as s:
            page = WorkflowRunRepository(s).list_paginated(
                workflow=args.name, status=status, page=args.page, page_size=args.page_size
            )
        for r in page.items:
            parent = f"\tparent={r.parent_run_id}" if r.parent_run_id else ""
            print(
                f"{r.run_id}\t{r.workflow}\t{r.status.value}\t"
                f"started={r.started_at.isoformat()}{parent}"
            )
        print(f"page {page.page}/{page.total_pages}  total={page.total}")
        return 0

    if command == "show":
        with session_scope(factory) as s:
            run = WorkflowRunRepository(s).get(args.run_id)
        if run is None:
            print(f"未找到工作流运行：{args.run_id}", file=sys.stderr)
            return 1
        _print_run(run)
        return 0

    return 1

"""工作流引擎：按 DAG 依赖并行执行节点。

线程模型：
- 调用 run() 的线程是"协调线程"：只有它修改节点状态、触发 on_update，所以不需要加锁
- runner 池：每个运行中的节点占一个线程，负责该节点的重试循环
- handler 池：交给 execute_attempt 运行任务函数（超时/取消由它处理）
  两个池大小都是 max_parallel，避免任务在池里排队而被算进超时

规则（见 domain/workflow_state.py）：上游全部 SUCCESS 才运行；上游 FAILED / SKIPPED /
CANCELED 则下游 SKIPPED；没有依赖关系的分支不受影响。

每一次尝试使用全新的 CancelToken：execute_attempt 超时时会 cancel() 令牌，
复用同一个令牌会让下一次重试被误判为已取消。节点级取消由 _NodeControl 桥接。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Executor, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass

from task_platform.domain.cancel import CancelToken
from task_platform.domain.enums import JobStatus, NodeStatus
from task_platform.domain.models import Job, JobExecution, utc_now
from task_platform.domain.workflow import WorkflowDefinition, WorkflowValidationError
from task_platform.domain.workflow_run import NodeRun, WorkflowRun, new_workflow_run
from task_platform.domain.workflow_state import nodes_to_skip, ready_nodes
from task_platform.executor import execute_attempt
from task_platform.registry import TaskRegistry

logger = logging.getLogger("task_platform.workflow")
UpdateFn = Callable[[WorkflowRun], None]
AttemptFn = Callable[[str, str, JobExecution], None]  # (工作流 run_id, 节点名， 这次尝试)


@dataclass(frozen=True, slots=True)
class _NodeOutcome:
    status: NodeStatus
    attempts: list[JobExecution]
    error_message: str | None


class _NodeControl:
    """一个节点的取消控制：stop() 同时打断重试等待，并取消当前这次尝试的令牌"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._token: CancelToken | None = None

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def attempt_token(self) -> CancelToken:
        with self._lock:
            token = CancelToken()
            if self._stop.is_set():
                token.cancel()
            self._token = token
            return token

    def stop(self) -> None:
        with self._lock:
            self._stop.set()
            if self._token is not None:
                self._token.cancel()

    def sleep(self, seconds: float) -> bool:
        """可被 stop() 打断的等待；返回 True 表示期间被取消"""
        return self._stop.wait(seconds)


class WorkflowEngine:
    def __init__(
        self,
        registry: TaskRegistry,
        jobs: Mapping[str, Job],
        *,
        max_parallel: int = 4,
        poll_seconds: float = 0.05,
    ) -> None:
        """jobs: Job.name -> Job，节点的 job 字段按它解析"""
        if max_parallel < 1:
            raise ValueError("max_parallel must be >= 1")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be > 0")
        self._registry = registry
        self._jobs = jobs
        self._max_parallel = max_parallel
        self._poll = poll_seconds

    def _resolve_jobs(self, definition: WorkflowDefinition) -> dict[str, Job]:
        """节点名 -> Job；运行任何节点之前先整体检查，避免跑到一半才发现缺失"""
        missing = sorted({n.job for n in definition.nodes if n.job not in self._jobs})
        if missing:
            raise WorkflowValidationError(f"工作流引用了不存在的 Job: {missing}")
        resolved = {n.name: self._jobs[n.job] for n in definition.nodes}
        unregistered = sorted(
            {j.handler for j in resolved.values() if not self._registry.is_registered(j.handler)}
        )
        if unregistered:
            raise WorkflowValidationError(f"以下 handler 未注册：{unregistered}")
        return resolved

    def run(
        self,
        definition: WorkflowDefinition,
        *,
        cancel_token: CancelToken | None = None,
        on_update: UpdateFn | None = None,
        on_attempt: AttemptFn | None = None,
        carry_over: Mapping[str, NodeRun] | None = None,
        parent_run_id: str | None = None,
    ) -> WorkflowRun:
        """同步运行整个工作流，全部节点结束（或被取消）后返回。

        on_update：每次节点状态变化后在协调线程里调用，传入实时的 WorkflowRun；
        on_attempt：每次尝试结束后在 runner 线程里调用（多个节点可能同时回调，
        实现需要线程安全），用于逐次落库尝试和日志；
        两个回调抛异常都只记日志，不影响运行。
        carry_over：沿用来源运行的节点结果（这些节点不会再执行，标记 reused），
        配合 parent_run_id 用于重试 / 重跑；规则见 domain/workflow_replay.py。
        """
        jobs = self._resolve_jobs(definition)
        token = cancel_token if cancel_token is not None else CancelToken()
        run = new_workflow_run(definition, parent_run_id=parent_run_id, carry_over=carry_over)
        run.started_at = utc_now()
        controls: dict[str, _NodeControl] = {}
        running: dict[Future[_NodeOutcome], str] = {}
        extra = {"workflow": definition.name, "workflow_run_id": run.run_id}

        def notify() -> None:
            if on_update is None:
                return
            # noinspection PyBroadException
            try:
                on_update(run)
            except Exception:
                logger.exception("workflow on_update callback failed", extra=extra)

        logger.info("workflow started", extra=extra)
        with (
            ThreadPoolExecutor(self._max_parallel, thread_name_prefix="wf-node") as runners,
            ThreadPoolExecutor(self._max_parallel, thread_name_prefix="wf-handler") as handlers,
        ):
            notify()
            while True:
                changed = False
                if token.is_cancelled:
                    for control in controls.values():
                        control.stop()
                    for node in run.nodes.values():
                        if node.status is NodeStatus.PENDING:
                            self._settle(node, NodeStatus.CANCELED, "canceled before start")
                else:
                    for name in nodes_to_skip(definition, run.statuses()):
                        self._settle(
                            run.nodes[name],
                            NodeStatus.SKIPPED,
                            "skipped because an upstream node did not succeed",
                        )
                        changed = True
                    for name in ready_nodes(definition, run.statuses()):
                        if len(running) >= self._max_parallel:
                            break
                        node = run.nodes[name]
                        node.mark(NodeStatus.RUNNING)
                        node.started_at = utc_now()
                        control = controls[name] = _NodeControl()
                        future = runners.submit(
                            self._run_node,
                            name,
                            jobs[name],
                            control,
                            handlers,
                            run.run_id,
                            on_attempt,
                        )
                        running[future] = name
                        changed = True
                if changed:
                    notify()
                if not running:
                    break  # 没有运行中的节点，也没有可启动的节点；全部已到终态

                done, _ = wait(running, timeout=self._poll, return_when=FIRST_COMPLETED)
                for future in done:
                    name = running.pop(future)
                    self._finish(run.nodes[name], future.result())
                    logger.info(
                        "workflow node finished",
                        extra={**extra, "node": name, "status": run.nodes[name].status.value},
                    )
                if done:
                    notify()

        run.finished_at = utc_now()
        notify()
        logger.info("workflow finished", extra={**extra, "status": run.status.value})
        return run

    # ---------------- 内部 ----------------

    @staticmethod
    def _settle(node: NodeRun, status: NodeStatus, message: str) -> None:
        """没有真正执行过的节点直接结束（SKIPPED / CANCELED）"""
        node.mark(status)
        node.error_message = message
        node.finished_at = utc_now()

    @staticmethod
    def _finish(node: NodeRun, outcome: _NodeOutcome) -> None:
        node.attempts = outcome.attempts
        node.error_message = outcome.error_message
        node.finished_at = utc_now()
        node.mark(outcome.status)

    def _run_node(
        self,
        node: str,
        job: Job,
        control: _NodeControl,
        handlers: Executor,
        workflow_run_id: str,
        on_attempt: AttemptFn | None,
    ) -> _NodeOutcome:
        """在 runner 线程里驱动一个节点：执行 -> 失败则按重试策略等待后重试。
        同一节点的各次尝试共享 run_id、attempt 递增（与 Scheduler/Worker 一致）。"""
        attempts: list[JobExecution] = []
        run_id: str | None = None
        attempt = 1
        try:
            while True:
                if control.stopped:
                    return _NodeOutcome(NodeStatus.CANCELED, attempts, "canceled")
                result = execute_attempt(
                    job,
                    self._registry,
                    handlers,
                    attempt_number=attempt,
                    run_id=run_id,
                    cancel_token=control.attempt_token(),
                )
                execution = result.execution
                run_id = execution.run_id
                attempts.append(execution)
                if on_attempt is not None:
                    # noinspection PyBroadException
                    try:
                        on_attempt(workflow_run_id, node, execution)
                    except Exception:
                        logger.exception(
                            "workflow on_attempt callback failed",
                            extra={"workflow_run_id": workflow_run_id, "node": node},
                        )

                if execution.status is JobStatus.SUCCESS:
                    return _NodeOutcome(NodeStatus.SUCCESS, attempts, None)
                if execution.status is JobStatus.CANCELED:
                    return _NodeOutcome(NodeStatus.CANCELED, attempts, execution.error_message)
                if not result.should_retry:
                    return _NodeOutcome(NodeStatus.FAILED, attempts, execution.error_message)

                delay = result.retry_policy.delay_for_attempt(attempt)
                if control.sleep(delay):
                    return _NodeOutcome(NodeStatus.CANCELED, attempts, "canceled while retrying")
                attempt += 1
        except Exception as exc:
            logger.exception(
                "workflow node crashed",
                extra={"workflow_run_id": workflow_run_id, "node": node},
            )
            return _NodeOutcome(NodeStatus.FAILED, attempts, f"engine error: {exc}")

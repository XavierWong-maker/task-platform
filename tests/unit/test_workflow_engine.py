from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from task_platform.domain.cancel import CancelToken
from task_platform.domain.enums import JobStatus, JobType, NodeStatus, WorkflowStatus
from task_platform.domain.models import Job, JobExecution, RetryPolicy
from task_platform.domain.workflow import TaskNode, WorkflowDefinition, WorkflowValidationError
from task_platform.domain.workflow_run import WorkflowRun
from task_platform.executor import TIMEOUT_ERROR_PREFIX
from task_platform.faults import FlakyState
from task_platform.jobs.context import JobContext
from task_platform.registry import TaskRegistry
from task_platform.workflow_engine import WorkflowEngine

N = NodeStatus
FAST_RETRY = RetryPolicy(max_attempts=3, base_delay_seconds=0.01)


def _n(name: str, *deps: str) -> TaskNode:
    """节点名、job 名、handler 名都用同一个名字，测试里更直观"""
    return TaskNode(name=name, job=name, depends_on=tuple(deps))


def _wf(*nodes: TaskNode) -> WorkflowDefinition:
    return WorkflowDefinition("wf", tuple(nodes))


def _etl() -> WorkflowDefinition:
    return _wf(
        _n("extract"),
        _n("clean", "extract"),
        _n("stats", "extract"),
        _n("report", "clean", "stats"),
        _n("audit", "clean"),
    )


def _engine(
    handlers: dict[str, Callable[..., object]],
    *,
    retry: RetryPolicy | None = None,
    timeout: float = 30.0,
    max_parallel: int = 4,
) -> WorkflowEngine:
    registry = TaskRegistry()
    jobs: dict[str, Job] = {}
    for name, fn in handlers.items():
        registry.register(name, fn)
        jobs[name] = Job(
            name=name,
            job_type=JobType.CUSTOM,
            handler=name,
            timeout_seconds=timeout,
            retry_policy=retry or RetryPolicy(max_attempts=1),
        )
    return WorkflowEngine(registry, jobs, max_parallel=max_parallel)


def _rec(calls: list[str], name: str, *, fail: bool = False) -> Callable[[], str]:
    def fn() -> str:
        calls.append(name)
        if fail:
            raise RuntimeError(f"{name} failed")
        return name

    return fn


# ---------------- 正常执行 ----------------


def test_linear_workflow_runs_in_order_and_succeeds() -> None:
    calls: list[str] = []
    engine = _engine({n: _rec(calls, n) for n in ("extract", "clean", "report")})

    run = engine.run(_wf(_n("extract"), _n("clean", "extract"), _n("report", "clean")))

    assert calls == ["extract", "clean", "report"]
    assert run.status is WorkflowStatus.SUCCESS
    assert all(n.status is N.SUCCESS and len(n.attempts) == 1 for n in run.nodes.values())
    assert all(n.run_id is not None for n in run.nodes.values())
    assert run.run_id.startswith("wfrun_")
    assert run.started_at is not None and run.finished_at is not None


def test_downstream_starts_only_after_all_upstreams_finished() -> None:
    events: list[tuple[str, str]] = []
    lock = threading.Lock()

    def timed(name: str) -> Callable[[], None]:
        def fn() -> None:
            with lock:
                events.append(("start", name))
            time.sleep(0.03)
            with lock:
                events.append(("end", name))

        return fn

    engine = _engine({n: timed(n) for n in "abcd"})
    run = engine.run(_wf(_n("a"), _n("b", "a"), _n("c", "a"), _n("d", "b", "c")))

    assert run.status is WorkflowStatus.SUCCESS
    for up, down in [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]:
        assert events.index(("end", up)) < events.index(("start", down))


def test_independent_nodes_really_run_concurrently() -> None:
    barrier = threading.Barrier(2)

    def meet() -> None:
        barrier.wait(timeout=5)  # 串行执行时第二个永远等不到，会超时失败

    run = _engine({"x": meet, "y": meet}, max_parallel=2).run(_wf(_n("x"), _n("y")))

    assert run.status is WorkflowStatus.SUCCESS


def test_max_parallel_limits_concurrent_nodes() -> None:
    lock = threading.Lock()
    active = peak = 0

    def work() -> None:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1

    names = ("n1", "n2", "n3", "n4")
    run = _engine({n: work for n in names}, max_parallel=2).run(_wf(*(_n(n) for n in names)))

    assert run.status is WorkflowStatus.SUCCESS
    assert peak == 2


# ---------------- 失败传播 ----------------


def test_failure_skips_downstream_but_independent_branch_completes() -> None:
    calls: list[str] = []
    engine = _engine(
        {
            "extract": _rec(calls, "extract"),
            "clean": _rec(calls, "clean", fail=True),
            "stats": _rec(calls, "stats"),
            "report": _rec(calls, "report"),
            "audit": _rec(calls, "audit"),
        }
    )

    run = engine.run(_etl())

    assert run.statuses() == {
        "extract": N.SUCCESS,
        "clean": N.FAILED,
        "stats": N.SUCCESS,
        "report": N.SKIPPED,
        "audit": N.SKIPPED,
    }
    assert run.status is WorkflowStatus.PARTIAL_FAILED
    assert "report" not in calls and "audit" not in calls
    assert run.nodes["report"].attempts == []
    assert run.nodes["clean"].error_message == "clean failed"
    assert run.nodes["report"].error_message is not None


def test_root_failure_fails_whole_workflow() -> None:
    calls: list[str] = []
    engine = _engine({n: _rec(calls, n, fail=(n == "extract")) for n in _etl().topological_order()})

    run = engine.run(_etl())

    assert run.status is WorkflowStatus.FAILED
    assert calls == ["extract"]
    assert [n.status for n in run.nodes.values()].count(N.SKIPPED) == 4


# ---------------- 节点重试 ----------------


def test_flaky_node_retries_with_same_run_id_then_succeeds() -> None:
    flaky = FlakyState()

    def handler(ctx: JobContext) -> str:
        if flaky.should_fail(ctx.execution_id, fail_times=1):
            raise RuntimeError("injected")
        return "ok"

    run = _engine({"a": handler, "b": _rec([], "b")}, retry=FAST_RETRY).run(
        _wf(_n("a"), _n("b", "a"))
    )

    node = run.nodes["a"]
    assert run.status is WorkflowStatus.SUCCESS
    assert [(e.attempt, e.status) for e in node.attempts] == [
        (1, JobStatus.RETRYING),
        (2, JobStatus.SUCCESS),
    ]
    assert len({e.run_id for e in node.attempts}) == 1


def test_exhausted_retries_fail_node_and_skip_downstream() -> None:
    calls: list[str] = []
    engine = _engine({"a": _rec(calls, "a", fail=True), "b": _rec(calls, "b")}, retry=FAST_RETRY)

    run = engine.run(_wf(_n("a"), _n("b", "a")))

    assert [e.status for e in run.nodes["a"].attempts] == [
        JobStatus.RETRYING,
        JobStatus.RETRYING,
        JobStatus.FAILED,
    ]
    assert run.statuses() == {"a": N.FAILED, "b": N.SKIPPED}
    assert calls == ["a", "a", "a"]


def test_retry_after_timeout_gets_a_fresh_cancel_token() -> None:
    """超时会触发该次尝试的令牌；下一次尝试必须拿到新令牌，否则会被误判为已取消"""
    flaky = FlakyState()

    def handler(ctx: JobContext) -> str:
        if flaky.should_fail(ctx.execution_id, fail_times=1):
            ctx.cancel.wait(5)  # 超时后被令牌唤醒
            raise RuntimeError("late")
        ctx.check_cancelled()  # 共用旧令牌时，这里会抛 JobCanceledError
        return "ok"

    run = _engine(
        {"a": handler}, retry=RetryPolicy(max_attempts=2, base_delay_seconds=0.01), timeout=0.1
    ).run(_wf(_n("a")))

    node = run.nodes["a"]
    assert node.status is N.SUCCESS
    assert [e.status for e in node.attempts] == [JobStatus.RETRYING, JobStatus.SUCCESS]
    assert (node.attempts[0].error_message or "").startswith(TIMEOUT_ERROR_PREFIX)


# ---------------- 取消 ----------------


def test_cancel_stops_running_node_and_cancels_pending_ones() -> None:
    started = threading.Event()

    def long_task(ctx: JobContext) -> None:
        started.set()
        while True:
            ctx.check_cancelled()
            ctx.cancel.wait(0.01)

    token = CancelToken()
    threading.Thread(target=lambda: (started.wait(5), token.cancel()), daemon=True).start()

    run = _engine({"a": long_task, "b": _rec([], "b")}).run(
        _wf(_n("a"), _n("b", "a")), cancel_token=token
    )

    assert run.statuses() == {"a": N.CANCELED, "b": N.CANCELED}
    assert run.status is WorkflowStatus.CANCELED


def test_already_cancelled_token_runs_nothing() -> None:
    calls: list[str] = []
    token = CancelToken()
    token.cancel()

    run = _engine({"a": _rec(calls, "a")}).run(_wf(_n("a")), cancel_token=token)

    assert calls == []
    assert run.status is WorkflowStatus.CANCELED


# ---------------- 校验与回调 ----------------


def test_unknown_job_is_rejected_before_running_anything() -> None:
    calls: list[str] = []
    engine = _engine({"a": _rec(calls, "a")})

    with pytest.raises(WorkflowValidationError, match="不存在的 Job"):
        engine.run(_wf(_n("a"), TaskNode("b", job="ghost", depends_on=("a",))))
    assert calls == []


def test_unregistered_handler_is_rejected() -> None:
    jobs = {"a": Job(name="a", job_type=JobType.CUSTOM, handler="gone.handler")}
    engine = WorkflowEngine(TaskRegistry(), jobs)

    with pytest.raises(WorkflowValidationError, match="未注册"):
        engine.run(_wf(_n("a")))


@pytest.mark.parametrize("kwargs", [{"max_parallel": 0}, {"poll_seconds": 0}])
def test_invalid_engine_parameters_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        WorkflowEngine(TaskRegistry(), {}, **kwargs)  # type: ignore[arg-type]


def test_on_update_reports_progress_from_pending_to_final() -> None:
    snapshots: list[dict[str, NodeStatus]] = []

    def listener(run: WorkflowRun) -> None:
        snapshots.append(run.statuses())

    _engine({"a": _rec([], "a"), "b": _rec([], "b")}).run(
        _wf(_n("a"), _n("b", "a")), on_update=listener
    )

    assert snapshots[0] == {"a": N.PENDING, "b": N.PENDING}
    assert snapshots[-1] == {"a": N.SUCCESS, "b": N.SUCCESS}
    assert len(snapshots) >= 3


def test_failing_listener_does_not_break_the_run() -> None:
    def broken(run: WorkflowRun) -> None:
        raise RuntimeError("listener down")

    run = _engine({"a": _rec([], "a")}).run(_wf(_n("a")), on_update=broken)

    assert run.status is WorkflowStatus.SUCCESS


def test_on_attempt_reports_every_attempt_in_order() -> None:
    seen: list[tuple[str, str, int, JobStatus]] = []

    def hook(workflow_run_id: str, node: str, execution: JobExecution) -> None:
        seen.append((workflow_run_id, node, execution.attempt, execution.status))

    engine = _engine({"a": _rec([], "a", fail=True)}, retry=FAST_RETRY)
    run = engine.run(_wf(_n("a")), on_attempt=hook)

    assert [(n, a, s) for _, n, a, s in seen] == [
        ("a", 1, JobStatus.RETRYING),
        ("a", 2, JobStatus.RETRYING),
        ("a", 3, JobStatus.FAILED),
    ]
    assert {w for w, *_ in seen} == {run.run_id}


def test_failing_on_attempt_hook_does_not_break_the_run() -> None:
    def broken(*_: object) -> None:
        raise RuntimeError("db down")

    run = _engine({"a": _rec([], "a")}).run(_wf(_n("a")), on_attempt=broken)

    assert run.status is WorkflowStatus.SUCCESS

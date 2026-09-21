from __future__ import annotations

import logging

from task_platform.jobs.context import JobContext, accepts_context, build_context, call_handler


def test_build_context_creates_named_logger() -> None:
    ctx = build_context("run_abc123")
    assert ctx.execution_id == "run_abc123"
    assert isinstance(ctx.logger, logging.Logger)
    assert ctx.logger.name == "task_platform.job.run_abc123"
    assert ctx.config == {}


def test_build_context_accepts_custom_config() -> None:
    ctx = build_context("run_1", config={"batch_size": 100})
    assert ctx.config == {"batch_size": 100}


def test_accepts_context_true_when_ctx_param_present() -> None:
    def with_ctx(ctx: JobContext) -> None:
        return None

    assert accepts_context(with_ctx) is True


def test_accepts_context_false_when_ctx_param() -> None:
    def without_ctx(seconds: float = 2.0) -> None:
        return None

    assert accepts_context(without_ctx) is False


def test_call_handler_passes_context_when_declared() -> None:
    received: dict[str, JobContext] = {}

    def with_ctx(ctx: JobContext) -> str:
        received["ctx"] = ctx
        return "handled"

    job_ctx = build_context("run_x")
    result = call_handler(with_ctx, job_ctx)

    assert result == "handled"
    assert received["ctx"] is job_ctx


def test_call_handler_calls_without_args_when_no_ctx() -> None:
    def without_ctx() -> str:
        return "ok"

    ctx = build_context("run_y")
    assert call_handler(without_ctx, ctx) == "ok"

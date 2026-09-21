"""
JobContext：统一的任务执行上下文
Executor 在调用任务函数前构建一个 JobContext，
携带 execution_id、专属 logger 和（预留的）config

任务函数是否需要它是可选的——只有显式声明名为 `ctx` 的参数，才会收到这个对象；
不写 ctx 参数的任务（如三个演示任务）继续按无参数方式调用，行为不变
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from task_platform.registry import Handler


@dataclass(frozen=True, slots=True)
class JobContext:
    """一次任务执行的上下文，随 handler 调用一起传入（若 handler 声明接收它）"""

    execution_id: str
    logger: logging.Logger
    config: Mapping[str, Any] = field(default_factory=dict)


def build_context(execution_id: str, *, config: Mapping[str, Any] | None = None) -> JobContext:
    """构建一个 JobContext

    logger 命名为 task_platform.job.<execution_id>，方便按 run_id 串联日志
    （对照设计蓝图：日志字段需包含 job_id/run_id，这里先从 logger 命名维度打基础）
    """
    return JobContext(
        execution_id=execution_id,
        logger=logging.getLogger(f"task_platform.job.{execution_id}"),
        config=config or {},
    )


def accepts_context(func: Handler) -> bool:
    """判断 func 是否声明了名为 `ctx` 的参数（约定：这是接收 JobContext 的方式）"""
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        # 内置函数/C 扩展等拿不到签名时，保守地认为不需要 ctx
        return False
    return "ctx" in sig.parameters


def call_handler(func: Handler, context: JobContext) -> Any:
    """按 func 是否需要 ctx，调用它并返回结果"""
    if accepts_context(func):
        return func(ctx=context)
    return func()

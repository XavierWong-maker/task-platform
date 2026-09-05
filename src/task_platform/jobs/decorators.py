"""
@job 装饰器：把一个普通函数注册为任务

装饰器本身不改变函数行为（返回原函数），唯一的副作用是：
模块被 import 时，函数会被写入 registry（默认是 registry.py 里的default_registry 单例）

这是"新增任务无需修改核心调度器"的基础：
只要该任务所在的模块被 import 过一次，注册就自动完成了
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from task_platform.domain.enums import BackoffStrategy
from task_platform.domain.models import RetryPolicy
from task_platform.registry import Handler, TaskRegistry, default_registry

F = TypeVar("F", bound=Handler)


def job(
    name: str,
    *,
    description: str = "",
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
    backoff: BackoffStrategy = BackoffStrategy.FIXED,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 60.0,
    registry: TaskRegistry | None = None,
) -> Callable[[F], F]:
    """将被装饰的函数注册为一个任务

    Args:
        name: 任务唯一标识名（对应 Job.handler / TaskRegistry 的 key）
        description: 简短说明
        timeout_seconds: 默认超时时间
        max_attempts/backoff/base_delay_seconds/max_delay_seconds:
            用于构造该任务的默认 RetryPolicy
        registry: 显式指定写入哪个 TaskRegistry；默认为 default_registry。
            测试中用得上——避免每个用例互相污染全局单例。

    Raises:
        DuplicateJobNameError: name 在目标 registry 中已存在时
            （在模块 import 阶段、也就是装饰器执行时立即抛出）

    Example:
        @job("demo.success", description="总是成功的演示任务")
        def demo_success() -> str:
            return "ok"
    """
    target_registry = registry if registry is not None else default_registry
    retry_policy = RetryPolicy(
        max_attempts=max_attempts,
        backoff=backoff,
        base_delay_seconds=base_delay_seconds,
        max_delay_seconds=max_delay_seconds,
    )

    def decorator(func: F) -> F:
        target_registry.register(
            name,
            func,
            description=description,
            timeout_seconds=timeout_seconds,
            retry_policy=retry_policy,
        )
        return func

    return decorator

"""
TaskRegistry：任务名称 -> 可执行函数 的内存注册表
阶段 2：新增 @job 装饰器 + importlib 自动发现，默认写入本模块的default_registry 单例；
        TaskRegistry 本身的职责保持不变，只是元数据（timeout/retry_policy）从"无"变为"可选携带"
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from task_platform.domain.models import RetryPolicy

Handler = Callable[..., Any]


class JobNotRegisteredError(Exception):
    """尝试获取一个未注册的任务名称时抛出"""

    def __init__(self, name: str) -> None:
        self._name = name
        super().__init__(f"任务 '{name}' 未在 TaskRegistry 中注册")


class DuplicateJobNameError(Exception):
    """尝试用一个已存在的名称重复注册时抛出"""

    def __init__(self, name: str) -> None:
        self._name = name
        super().__init__(f"任务名称 '{name}' 已被注册，不能重复注册")


@dataclass(frozen=True, slots=True)
class RegisteredHandler:
    """一个已经注册任务的元信息
    阶段 2 的 @job 装饰器会显式传入 timeout_seconds / retry_policy 这两项，
    作为该 job 自身携带的默认执行参数。
    """

    name: str
    func: Handler
    description: str = ""
    timeout_seconds: float | None = None
    retry_policy: RetryPolicy | None = None


class TaskRegistry:
    """保存任务名称与其可执行函数（及元数据）的映射"""

    def __init__(self) -> None:
        self._handlers: dict[str, RegisteredHandler] = {}

    def register(
        self,
        name: str,
        func: Handler,
        *,
        description: str = "",
        timeout_seconds: float | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """注册一个任务处理函数

        Args:
            name: 任务的唯一标识名，供 Job.handler 引用
            func: 可执行函数
            description: 简短说明，供 `show`/`list` 展示
            timeout_seconds: 该任务的默认超时时间（由 @job 提供）
            retry_policy: 该任务的默认重试策略（由 @job 提供）
        Raises:
            DuplicateJobNameError: name 已存在时
        """
        if name in self._handlers:
            raise DuplicateJobNameError(name)
        self._handlers[name] = RegisteredHandler(
            name=name,
            func=func,
            description=description,
            timeout_seconds=timeout_seconds,
            retry_policy=retry_policy,
        )

    def get(self, name: str) -> RegisteredHandler:
        """根据名称获取已注册的handler
        Raises:
            JobNotRegisteredError: name 不存在时
        """
        try:
            return self._handlers[name]
        except KeyError:
            raise JobNotRegisteredError(name) from None

    def is_registered(self, name: str) -> bool:
        return name in self._handlers

    def list_names(self) -> list[str]:
        """按注册顺序返回所有已注册的任务名称"""
        return list(self._handlers.keys())

    def unregister(self, name: str) -> None:
        """移除一个已注册的任务（用于测试隔离）"""
        self._handlers.pop(name, None)

    def clear(self) -> None:
        """清空所有已注册任务（主要用于测试隔离/重复 import 场景）"""
        self._handlers.clear()


# 模块级单例：@job 装饰器在未显式指定 registry 时默认写入这里。
# 后续 Scheduler/Executor 也会以这个单例作为"当前进程已知任务"的来源。
default_registry = TaskRegistry()

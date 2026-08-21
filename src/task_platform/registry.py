"""
TaskRegistry：任务名称 -> 可执行函数 的内存注册表
阶段 1 手动调用 register() 完成注册
阶段 2 会在此基础上加 @job 装饰器 + importlib 自动发现，
但 TaskRegistry 本身的职责保持不变
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

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
    """一个已经注册任务的元信息"""

    name: str
    func: Handler
    description: str = ""


class TaskRegistry:
    """保存任务名称与其可执行函数的映射"""

    def __init__(self) -> None:
        self._handlers: dict[str, RegisteredHandler] = {}

    def register(self, name: str, func: Handler, *, description: str = "") -> None:
        """注册一个任务处理函数
        Args:
            name: 任务的唯一标识名，供 Job.handler 引用
            func: 可执行函数
            description: 简短说明，供 `show`/`list` 展示
        Raises:
            DuplicateJobNameError: name 已存在时
        """
        if name in self._handlers:
            raise DuplicateJobNameError(name)
        self._handlers[name] = RegisteredHandler(name=name, func=func, description=description)

    def get(self, name: str) -> RegisteredHandler:
        """根据名称获取已注册的handler

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

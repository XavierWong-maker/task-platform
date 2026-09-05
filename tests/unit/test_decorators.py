"""
@job 装饰器测试
关键点：
1. 装饰后函数本身不变，仍可直接调用
2. 元数据（description/timeout/retry_policy）被正确写入 registry
3. 重复注册同名任务在装饰（即 import）阶段就报错
4. 默认写入 default_registry；也支持传入独立 registry 做测试隔离
"""

from __future__ import annotations

import pytest

from task_platform.domain.enums import BackoffStrategy
from task_platform.jobs.decorators import job
from task_platform.registry import DuplicateJobNameError, TaskRegistry, default_registry


def test_decorator_registry_into_explicit_registry() -> None:
    registry = TaskRegistry()

    @job("demo.explicit", description="显式 registry 示例", registry=registry)
    def sample() -> str:
        return "ok"

    handler = registry.get("demo.explicit")
    assert handler.name == "demo.explicit"
    assert handler.description == "显式 registry 示例"
    assert handler.func is sample
    assert sample() == "ok"  # 装饰器不改变函数行为


def test_decorator_carries_timeout_and_retry_policy() -> None:
    registry = TaskRegistry()

    @job(
        "demo.with.meta",
        timeout_seconds=5.0,
        max_attempts=4,
        backoff=BackoffStrategy.EXPONENTIAL,
        registry=registry,
    )
    def sample() -> None:
        return None

    handler = registry.get("demo.with.meta")
    assert handler.timeout_seconds == 5.0
    assert handler.retry_policy is not None
    assert handler.retry_policy.max_attempts == 4
    assert handler.retry_policy.backoff is BackoffStrategy.EXPONENTIAL


def test_duplicate_name_raises_on_decorator() -> None:
    registry = TaskRegistry()

    @job("demo.dup", registry=registry)
    def first() -> None:
        return None

    with pytest.raises(DuplicateJobNameError):

        @job("demo.dup", registry=registry)
        def second() -> None:
            return None


def test_default_registry_receives_registration_when_unspecified() -> None:
    # 备份当前已有的注册状态（可能包含 discover_jobs 早已注册的 demo.* 等），
    # 避免用 clear() 整体清空——因为已 import 过的任务模块在 sys.modules 里有缓存，
    # clear() 之后 discover_jobs 不会重新 import，也就无法把它们找回来。
    backup = dict(default_registry._handlers)
    default_registry._handlers.clear()
    try:

        @job("demo.default_target")
        def sample() -> str:
            return "from-default"

        assert default_registry.is_registered("demo.default_target")
        assert default_registry.get("demo.default_target").func() == "from-default"
    finally:
        # 只恢复原状态，不残留本测试自己注册的 demo.default_target
        default_registry._handlers.clear()
        default_registry._handlers.update(backup)

"""
演示任务：用于验证 TaskRegistry / CLI / 发现机制的典型结果
阶段 2 起改用 @job 装饰器注册，写入 default_registry；
模块被 import（无论是手动 import 还是被 discover_jobs 发现）时即完成注册
"""

from __future__ import annotations

import time

from task_platform.jobs.decorators import job


@job("demo.success", description="成功演示任务")
def demo_success() -> str:
    """假设成功"""
    return "ok"


@job("demo.failure", description="失败演示任务")
def demo_failure() -> str:
    """总是失败的任务，用于验证 FAILED / 重试路径"""
    raise RuntimeError("demo_failure 演示任务：故意失败")


@job("demo.slow", description="耗时演示任务", timeout_seconds=5.0)
def demo_slow(seconds: float = 2.0) -> str:
    """耗时任务，用于后续验证 timeout 行为"""
    time.sleep(seconds)
    return f"slept {seconds}s"

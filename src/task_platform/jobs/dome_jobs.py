"""
演示任务：用于验证 TaskRegistry 与 CLI 的三种典型结果
后续会引入 @job 装饰器实现自动发现；现阶段先手动在 TaskRegistry 里注册这三个函数
"""

from __future__ import annotations

import time


def demo_success() -> str:
    """假设成功"""
    return "ok"


def demo_failure() -> str:
    """总是失败的任务，用于验证 FAILED / 重试路径"""
    raise RuntimeError("demo_failure 演示任务：故意失败")


def demo_slow(seconds: float = 2.0) -> str:
    """耗时任务，用于后续验证 timeout 行为"""
    time.sleep(seconds)
    return f"slept {seconds}s"

"""
示例：演示"新增一个任务，只需新增一个 Python 文件"
本文件本身不是核心代码的一部分——删除它不影响任何其他模块。
它的存在只是为了证明：只要用 @job 装饰、放进 jobs/ 目录，
discover_jobs() 就能找到它，不需要碰 bootstrap.py 或 registry.py
"""

from __future__ import annotations

from task_platform.jobs.decorators import job


@job("demo.example_new", description="演示新增任务无需修改核心代码")
def example_new_job() -> str:
    return "new job discovered without touching core code"

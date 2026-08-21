"""
应用启动装配：注册演示任务，构建内存版 Job 列表
现阶段还没有数据库，Job 定义先放在内存字典里，
CLI 通过 name 或 id 查找。这个模块后续会被 Repository 取代
"""

from __future__ import annotations

from task_platform.domain.enums import JobType
from task_platform.domain.models import Job
from task_platform.jobs.dome_jobs import demo_failure, demo_slow, demo_success
from task_platform.registry import TaskRegistry


def build_registry() -> TaskRegistry:
    """构建并返回已注册好 3 个演示任务的 TaskRegistry"""
    registry = TaskRegistry()
    registry.register("demo.success", demo_success, description="总是成功的演示任务")
    registry.register("demo.failure", demo_failure, description="总是失败的演示任务")
    registry.register("demo.slow", demo_slow, description="耗时较长的演示任务")
    return registry


def build_jobs() -> dict[str, Job]:
    """构建内存版 Job 列表，key 为 Job.name，方便 CLI 按名称查找"""
    jobs = [
        Job(name="demo-success", job_type=JobType.DEMO_SUCCESS, handler="demo.success"),
        Job(name="demo-failure", job_type=JobType.DEMO_FAILURE, handler="demo.failure"),
        Job(name="demo-slow", job_type=JobType.DEMO_SLOW, handler="demo.slow"),
    ]
    return {job.name: job for job in jobs}

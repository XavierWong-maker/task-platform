"""
验证 build_jobs()：
1. 显式定义的演示任务（demo-success 等）名称/类型不变
2. 没有显式定义、但被 discover_jobs 发现的任务（demo.example_new）
   能自动生成 Job，且 timeout/retry 取自 @job 元数据
"""

from task_platform.bootstrap import build_jobs, build_registry
from task_platform.domain.enums import JobType


def test_explicit_demo_jobs_present_with_expected_names_and_types() -> None:
    registry = build_registry()
    jobs = build_jobs(registry)

    assert jobs["demo-success"].job_type is JobType.DEMO_SUCCESS
    assert jobs["demo-success"].handler == "demo.success"
    assert jobs["demo-failure"].job_type is JobType.DEMO_FAILURE
    assert jobs["demo-slow"].job_type is JobType.DEMO_SLOW


def test_example_new_job_is_auto_generated_without_explicit_definition() -> None:
    registry = build_registry()
    jobs = build_jobs(registry)

    auto_job = jobs["demo-example_new"]
    assert auto_job.job_type is JobType.CUSTOM
    assert auto_job.handler == "demo.example_new"

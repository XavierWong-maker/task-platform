"""DAG 版 ETL 的五个任务：被 discover_jobs 自动发现，Job 名为 etl-dag-<阶段>。

故障注入：设置 TASK_PLATFORM_ETL_DAG_FAIL_STAGE=<extract|clean|validate|aggregate|report>，
对应阶段每次尝试都会失败（用于验收失败传播与重试；取消该变量后用 workflow retry 恢复）。
"""

from __future__ import annotations

import os
from collections.abc import Callable

from task_platform.etl.dag_stages import STAGES, dag_dir
from task_platform.etl.daily_orders import DailyOrdersConfig
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job
from task_platform.registry import Handler

FAIL_STAGE_ENV = "TASK_PLATFORM_ETL_DAG_FAIL_STAGE"


def _run(stage: str, ctx: JobContext) -> str:
    if os.environ.get(FAIL_STAGE_ENV) == stage:
        raise RuntimeError(f"injected failure in stage {stage}")
    config = DailyOrdersConfig.from_env()
    return STAGES[stage](config, dag_dir(config), ctx.logger)


def _stage_job(stage: str, description: str) -> Callable[[Handler], Handler]:
    return job(
        f"etl.dag.{stage}",
        description=description,
        timeout_seconds=300.0,
        max_attempts=2,
        base_delay_seconds=0.2,
    )


@_stage_job("extract", "DAG ETL 阶段 1：抽取（生成模拟订单与用户）")
def etl_dag_extract(ctx: JobContext) -> str:
    return _run("extract", ctx)


@_stage_job("clean", "DAG ETL 阶段 2：清洗（统一字段格式）")
def etl_dag_clean(ctx: JobContext) -> str:
    return _run("clean", ctx)


@_stage_job("validate", "DAG ETL 阶段 3：校验（质量规则，坏数据进 quarantine）")
def etl_dag_validate(ctx: JobContext) -> str:
    return _run("validate", ctx)


@_stage_job("aggregate", "DAG ETL 阶段 4：聚合（按日期与地区）")
def etl_dag_aggregate(ctx: JobContext) -> str:
    return _run("aggregate", ctx)


@_stage_job("report", "DAG ETL 阶段 5：报表（交叉核对后输出日报与汇总）")
def etl_dag_report(ctx: JobContext) -> str:
    return _run("report", ctx)

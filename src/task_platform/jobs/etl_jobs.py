"""ETL 任务：被 discover_jobs 自动发现，无需修改任何核心代码。"""

from __future__ import annotations

from task_platform.etl.daily_orders import DailyOrdersConfig, run_daily_orders_etl
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job


@job(
    "etl.daily_orders",
    description="每日订单 ETL：校验、转换、聚合，输出日报/质量报告/quarantine",
    timeout_seconds=300.0,
    max_attempts=3,
)
def daily_orders_etl(ctx: JobContext) -> str:
    config = DailyOrdersConfig.from_env()
    summary = run_daily_orders_etl(config, config.output_dir / ctx.execution_id, logger=ctx.logger)
    return f"total={summary.total_rows} quarantined={summary.quarantined_rows}"

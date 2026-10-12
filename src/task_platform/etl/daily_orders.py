"""每日订单 ETL 用例：生成模拟数据 -> 流水线 -> 写出日报 / 质量报告 / quarantine。

这里是普通函数，不依赖调度器；任务入口见 jobs/etl_jobs.py。
"""

from __future__ import annotations

import csv
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from task_platform.etl.factory import DEFAULT_NOW, TestDataFactory
from task_platform.etl.pipeline import PipelineResult, QuarantinedOrder, run_orders_pipeline

ENV_PREFIX = "TASK_PLATFORM_ETL_"


@dataclass(frozen=True, slots=True)
class DailyOrdersConfig:
    seed: int = 20260817
    order_count: int = 100_000
    dirty_ratio: float = 0.03
    output_dir: Path = Path("data/output")
    user_pool_size: int = 1000
    product_pool_size: int = 200
    now: datetime = DEFAULT_NOW  # 模拟世界的"当前时间"，保证结果可复现

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> DailyOrdersConfig:
        """配置优先级：环境变量 > 默认值"""
        source = os.environ if env is None else env
        d = cls()
        return cls(
            seed=int(source.get(f"{ENV_PREFIX}SEED", d.seed)),
            order_count=int(source.get(f"{ENV_PREFIX}ORDER_COUNT", d.order_count)),
            dirty_ratio=float(source.get(f"{ENV_PREFIX}DIRTY_RATIO", d.dirty_ratio)),
            output_dir=Path(source.get(f"{ENV_PREFIX}OUTPUT_DIR", d.output_dir)),
        )


@dataclass(frozen=True, slots=True)
class EtlSummary:
    run_dir: Path
    total_rows: int
    quarantined_rows: int
    warning_rows: int
    report_rows: int
    total_gmv: Decimal


def _quarantine_line(item: QuarantinedOrder) -> str:
    payload = {"order": asdict(item.order), "violations": [asdict(v) for v in item.violations]}
    return json.dumps(payload, ensure_ascii=False, default=str)  # Decimal/datetime -> str


def _write_daily_report(path: Path, result: PipelineResult) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["order_date", "region", "order_count", "paid_count", "gmv", "avg_order_value"]
        )
        for s in result.stats:
            writer.writerow(
                [
                    s.order_date.isoformat(),
                    s.region,
                    s.order_count,
                    s.paid_count,
                    s.gmv,
                    s.avg_order_value,
                ]
            )


def _write_quality_report(path: Path, config: DailyOrdersConfig, result: PipelineResult) -> None:
    q = result.quality
    payload = {
        "config": {
            "seed": config.seed,
            "order_count": config.order_count,
            "dirty_ratio": config.dirty_ratio,
        },
        "total_rows": q.total_rows,
        "quarantined_rows": q.quarantined_rows,
        "warning_rows": q.warning_rows,
        "results": [asdict(r) for r in q.results],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_daily_orders_etl(
    config: DailyOrdersConfig, run_dir: Path, *, logger: logging.Logger | None = None
) -> EtlSummary:
    log = logger or logging.getLogger("task_platform.etl")
    run_dir.mkdir(parents=True, exist_ok=True)
    factory = TestDataFactory(
        config.seed,
        user_pool_size=config.user_pool_size,
        product_pool_size=config.product_pool_size,
        now=config.now,
    )
    log.info(
        "daily orders ETL started: orders=%d dirty_ratio=%s output=%s",
        config.order_count,
        config.dirty_ratio,
        run_dir,
    )

    with (run_dir / "quarantine.jsonl").open("w", encoding="utf-8", newline="\n") as qf:

        def on_quarantine(item: QuarantinedOrder) -> None:
            qf.write(_quarantine_line(item) + "\n")

        result = run_orders_pipeline(
            factory.generate_orders(config.order_count, dirty_ratio=config.dirty_ratio),
            factory.generate_users(),
            now=config.now,
            on_quarantine=on_quarantine,
        )

    _write_daily_report(run_dir / "daily_report.csv", result)
    _write_quality_report(run_dir / "quality_report.json", config, result)

    q = result.quality
    summary = EtlSummary(
        run_dir=run_dir,
        total_rows=q.total_rows,
        quarantined_rows=q.quarantined_rows,
        warning_rows=q.warning_rows,
        report_rows=len(result.stats),
        total_gmv=sum((s.gmv for s in result.stats), Decimal("0")),
    )
    log.info(
        "daily orders ETL finished: total=%d quarantined=%d warnings=%d gmv=%s",
        summary.total_rows,
        summary.quarantined_rows,
        summary.warning_rows,
        summary.total_gmv,
    )
    return summary

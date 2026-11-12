"""DAG 版每日订单 ETL 的五个阶段：抽取 -> 清洗 -> 校验 -> 聚合 -> 报表。

工作流引擎不在节点之间传递数据，阶段之间通过工作目录里的文件交接：
    extract   -> raw_orders.csv, users.csv
    clean     -> clean_orders.csv
    validate  -> valid_orders.csv, quarantine.jsonl, quality_report.json
    aggregate -> daily_stats.csv
    report    -> daily_report.csv, summary.json

每个阶段只读上游文件、只写自己的文件，结果只取决于配置（固定 seed），
所以任何阶段都可以安全地重跑（工作流的重试 / 单节点重跑依赖这一点）。
同一个工作目录同时只应有一次运行；并发运行请使用不同的 TASK_PLATFORM_ETL_OUTPUT_DIR。
全部阶段成功时，daily_report.csv / quality_report.json / quarantine.jsonl
与单体任务 etl.daily_orders 的输出逐字节相同（测试保证）。
"""

from __future__ import annotations

import csv
import json
import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from task_platform.etl.daily_orders import DailyOrdersConfig
from task_platform.etl.factory import TestDataFactory
from task_platform.etl.pipeline import (
    DailyRegionStat,
    QuarantinedOrder,
    aggregate,
    transform,
    validate_stream,
)
from task_platform.etl.quality import QualityChecker, default_order_rules
from task_platform.etl.records import RawOrder, RawUser

StageFn = Callable[[DailyOrdersConfig, Path, logging.Logger], str]

_ORDER_FIELDS = (
    "order_id",
    "user_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount",
    "status",
    "created_at",
)
_USER_FIELDS = ("user_id", "region", "register_date", "member_level")
_STATS_FIELDS = ("order_date", "region", "order_count", "paid_count", "gmv")
_REPORT_FIELDS = ("order_date", "region", "order_count", "paid_count", "gmv", "avg_order_value")


def dag_dir(config: DailyOrdersConfig) -> Path:
    """DAG 各阶段共用的工作目录"""
    return config.output_dir / "dag"


# ---------------- 文件读写（缺失值用空串表示） ----------------


def _write_orders(path: Path, orders: Iterable[RawOrder]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_ORDER_FIELDS)
        for o in orders:
            writer.writerow(
                [
                    "" if o.order_id is None else o.order_id,
                    o.user_id,
                    o.product_id,
                    o.quantity,
                    o.unit_price,
                    o.discount,
                    o.status,
                    "" if o.created_at is None else o.created_at.isoformat(),
                ]
            )
            count += 1
    return count


def _read_orders(path: Path) -> Iterator[RawOrder]:
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            yield RawOrder(
                order_id=row["order_id"] or None,
                user_id=row["user_id"],
                product_id=row["product_id"],
                quantity=int(row["quantity"]),
                unit_price=Decimal(row["unit_price"]),
                discount=Decimal(row["discount"]),
                status=row["status"],
                created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
            )


def _write_users(path: Path, users: Iterable[RawUser]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_USER_FIELDS)
        for u in users:
            writer.writerow([u.user_id, u.region, u.register_date.isoformat(), u.member_level])
            count += 1
    return count


def _read_users(path: Path) -> dict[str, RawUser]:
    with path.open(encoding="utf-8", newline="") as f:
        return {
            row["user_id"]: RawUser(
                user_id=row["user_id"],
                region=row["region"],
                register_date=date.fromisoformat(row["register_date"]),
                member_level=row["member_level"],
            )
            for row in csv.DictReader(f)
        }


def _read_stats(path: Path) -> list[DailyRegionStat]:
    with path.open(encoding="utf-8", newline="") as f:
        return [
            DailyRegionStat(
                order_date=date.fromisoformat(row["order_date"]),
                region=row["region"],
                order_count=int(row["order_count"]),
                paid_count=int(row["paid_count"]),
                gmv=Decimal(row["gmv"]),
            )
            for row in csv.DictReader(f)
        ]


def _quarantine_line(item: QuarantinedOrder) -> str:
    payload = {"order": asdict(item.order), "violations": [asdict(v) for v in item.violations]}
    return json.dumps(payload, ensure_ascii=False, default=str)  # Decimal/datetime -> str


# ---------------- 五个阶段 ----------------


def run_extract(config: DailyOrdersConfig, work_dir: Path, logger: logging.Logger) -> str:
    """抽取：生成模拟订单与用户（固定 seed，可复现）"""
    work_dir.mkdir(parents=True, exist_ok=True)
    factory = TestDataFactory(
        config.seed,
        user_pool_size=config.user_pool_size,
        product_pool_size=config.product_pool_size,
        now=config.now,
    )
    orders = _write_orders(
        work_dir / "raw_orders.csv",
        factory.generate_orders(config.order_count, dirty_ratio=config.dirty_ratio),
    )
    users = _write_users(work_dir / "users.csv", factory.generate_users())
    logger.info("extract finished: orders=%d users=%d", orders, users)
    return f"orders={orders} users={users}"


def _normalize(order: RawOrder) -> RawOrder:
    order_id = order.order_id.strip() if order.order_id is not None else None
    return replace(
        order,
        order_id=order_id or None,
        user_id=order.user_id.strip(),
        product_id=order.product_id.strip(),
        status=order.status.strip(),
    )


def run_clean(config: DailyOrdersConfig, work_dir: Path, logger: logging.Logger) -> str:
    """清洗（Normalize）：统一字段格式（去掉首尾空白、空串视为缺失）。
    只做格式规整；业务规则（非空 / 唯一 / 范围 / 枚举 / 关联 / 时间）在下一阶段校验。"""
    rows = _write_orders(
        work_dir / "clean_orders.csv",
        (_normalize(o) for o in _read_orders(work_dir / "raw_orders.csv")),
    )
    logger.info("clean finished: rows=%d", rows)
    return f"rows={rows}"


def run_validate(config: DailyOrdersConfig, work_dir: Path, logger: logging.Logger) -> str:
    """校验：套用质量规则，ERROR 违规的订单写入 quarantine，其余写入 valid_orders.csv"""
    users = _read_users(work_dir / "users.csv")
    checker = QualityChecker(default_order_rules(users.keys(), config.now))

    with (work_dir / "quarantine.jsonl").open("w", encoding="utf-8", newline="\n") as qf:

        def on_quarantine(item: QuarantinedOrder) -> None:
            qf.write(_quarantine_line(item) + "\n")

        passed = _write_orders(
            work_dir / "valid_orders.csv",
            validate_stream(_read_orders(work_dir / "clean_orders.csv"), checker, on_quarantine),
        )

    q = checker.report()  # 必须在数据流消费完之后再取
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
    (work_dir / "quality_report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(
        "validate finished: total=%d quarantined=%d warnings=%d",
        q.total_rows,
        q.quarantined_rows,
        q.warning_rows,
    )
    return f"total={q.total_rows} quarantined={q.quarantined_rows} passed={passed}"


def run_aggregate(config: DailyOrdersConfig, work_dir: Path, logger: logging.Logger) -> str:
    """聚合：计算金额与地区，按（日期, 地区）聚合"""
    users = _read_users(work_dir / "users.csv")
    stats = aggregate(transform(_read_orders(work_dir / "valid_orders.csv"), users))
    with (work_dir / "daily_stats.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_STATS_FIELDS)
        for s in stats:
            writer.writerow(
                [s.order_date.isoformat(), s.region, s.order_count, s.paid_count, s.gmv]
            )
    logger.info("aggregate finished: groups=%d", len(stats))
    return f"groups={len(stats)}"


def run_report(config: DailyOrdersConfig, work_dir: Path, logger: logging.Logger) -> str:
    """报表：先与质量报告交叉核对订单数，再输出日报和汇总"""
    stats = _read_stats(work_dir / "daily_stats.csv")
    quality = json.loads((work_dir / "quality_report.json").read_text(encoding="utf-8"))
    expected = quality["total_rows"] - quality["quarantined_rows"]
    actual = sum(s.order_count for s in stats)
    if actual != expected:
        raise RuntimeError(f"报表与质量报告不一致：聚合订单数 {actual}，期望 {expected}")

    with (work_dir / "daily_report.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_REPORT_FIELDS)
        for s in stats:
            writer.writerow(
                [s.order_date.isoformat(), s.region, s.order_count, s.paid_count, s.gmv]
                + [s.avg_order_value]
            )

    total_gmv = sum((s.gmv for s in stats), Decimal("0"))
    summary = {
        "total_rows": quality["total_rows"],
        "quarantined_rows": quality["quarantined_rows"],
        "warning_rows": quality["warning_rows"],
        "report_rows": len(stats),
        "total_gmv": str(total_gmv),
    }
    (work_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("report finished: rows=%d gmv=%s", len(stats), total_gmv)
    return f"report_rows={len(stats)} gmv={total_gmv}"


# 顺序即依赖顺序
STAGES: dict[str, StageFn] = {
    "extract": run_extract,
    "clean": run_clean,
    "validate": run_validate,
    "aggregate": run_aggregate,
    "report": run_report,
}

from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

import pytest

from task_platform.bootstrap import build_registry
from task_platform.etl.dag_stages import STAGES, _read_orders, _write_orders, dag_dir
from task_platform.etl.daily_orders import DailyOrdersConfig, run_daily_orders_etl
from task_platform.etl.factory import TestDataFactory

LOG = logging.getLogger("test.etl_dag")
OUTPUTS = (
    "raw_orders.csv",
    "users.csv",
    "clean_orders.csv",
    "valid_orders.csv",
    "quarantine.jsonl",
    "quality_report.json",
    "daily_stats.csv",
    "daily_report.csv",
    "summary.json",
)


def _config(tmp_path: Path) -> DailyOrdersConfig:
    return DailyOrdersConfig(
        order_count=2000,
        dirty_ratio=0.05,
        user_pool_size=50,
        product_pool_size=20,
        output_dir=tmp_path,
    )


def _run_stages(config: DailyOrdersConfig, names: list[str]) -> Path:
    work = dag_dir(config)
    for name in names:
        STAGES[name](config, work, LOG)
    return work


def test_csv_roundtrip_preserves_dirty_orders(tmp_path: Path) -> None:
    factory = TestDataFactory(1, user_pool_size=50, product_pool_size=20)
    orders = list(factory.generate_orders(500, dirty_ratio=0.5))  # 含缺失 / 非法取值
    path = tmp_path / "orders.csv"

    assert _write_orders(path, orders) == 500
    assert list(_read_orders(path)) == orders


def test_dag_outputs_are_byte_identical_to_monolithic_etl(tmp_path: Path) -> None:
    config = _config(tmp_path)
    work = _run_stages(config, list(STAGES))
    mono = run_daily_orders_etl(config, tmp_path / "mono")

    for name in ("daily_report.csv", "quality_report.json", "quarantine.jsonl"):
        assert (work / name).read_bytes() == (tmp_path / "mono" / name).read_bytes(), name

    summary = json.loads((work / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_rows"] == mono.total_rows == 2000
    assert summary["quarantined_rows"] == mono.quarantined_rows > 0
    assert summary["report_rows"] == mono.report_rows
    assert Decimal(summary["total_gmv"]) == mono.total_gmv


def test_downstream_stages_are_rerunnable_and_deterministic(tmp_path: Path) -> None:
    """重试 / 重跑会重新执行下游阶段：重复执行必须得到完全相同的结果"""
    config = _config(tmp_path)
    work = _run_stages(config, list(STAGES))
    before = {name: (work / name).read_bytes() for name in OUTPUTS}

    _run_stages(config, ["validate", "aggregate", "report"])

    assert {name: (work / name).read_bytes() for name in OUTPUTS} == before


def test_report_rejects_inconsistent_stats(tmp_path: Path) -> None:
    config = _config(tmp_path)
    work = _run_stages(config, list(STAGES))
    stats = work / "daily_stats.csv"
    lines = stats.read_text(encoding="utf-8").splitlines(keepends=True)
    stats.write_text("".join(lines[:-1]), encoding="utf-8")  # 丢掉最后一行聚合结果

    with pytest.raises(RuntimeError, match="不一致"):
        STAGES["report"](config, work, LOG)


def test_stage_jobs_are_discovered_with_expected_metadata() -> None:
    registry = build_registry()
    for stage in STAGES:
        handler = registry.get(f"etl.dag.{stage}")
        assert handler.retry_policy is not None and handler.retry_policy.max_attempts == 2
        assert handler.timeout_seconds == 300.0

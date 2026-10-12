from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path

import pytest

from task_platform.cli.main import main
from task_platform.etl.daily_orders import DailyOrdersConfig, run_daily_orders_etl


def _config(tmp_path: Path) -> DailyOrdersConfig:
    return DailyOrdersConfig(
        order_count=2000,
        dirty_ratio=0.05,
        user_pool_size=50,
        product_pool_size=20,
        output_dir=tmp_path,
    )


def test_etl_writes_consistent_outputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    summary = run_daily_orders_etl(_config(tmp_path), run_dir)

    quarantine_lines = (run_dir / "quarantine.jsonl").read_text(encoding="utf-8").splitlines()
    quality = json.loads((run_dir / "quality_report.json").read_text(encoding="utf-8"))
    with (run_dir / "daily_report.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    assert summary.total_rows == quality["total_rows"] == 2000
    assert len(quarantine_lines) == summary.quarantined_rows == quality["quarantined_rows"]
    assert summary.quarantined_rows > 0
    assert len(rows) == summary.report_rows
    assert sum(Decimal(r["gmv"]) for r in rows) == summary.total_gmv
    assert sum(int(r["order_count"]) for r in rows) == 2000 - summary.quarantined_rows


def test_quarantine_line_contains_order_and_error_violation(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_daily_orders_etl(_config(tmp_path), run_dir)

    first = json.loads((run_dir / "quarantine.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert "order" in first and "order_id" in first["order"]
    assert first["violations"]
    assert any(v["severity"] == "ERROR" for v in first["violations"])


def test_same_config_produces_byte_identical_outputs(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    run_daily_orders_etl(cfg, tmp_path / "a")
    run_daily_orders_etl(cfg, tmp_path / "b")

    for name in ("daily_report.csv", "quality_report.json", "quarantine.jsonl"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()


def test_config_from_env_overrides_defaults() -> None:
    cfg = DailyOrdersConfig.from_env(
        {
            "TASK_PLATFORM_ETL_SEED": "7",
            "TASK_PLATFORM_ETL_ORDER_COUNT": "123",
            "TASK_PLATFORM_ETL_DIRTY_RATIO": "0.5",
            "TASK_PLATFORM_ETL_OUTPUT_DIR": "somewhere",
        }
    )
    assert (cfg.seed, cfg.order_count, cfg.dirty_ratio) == (7, 123, 0.5)
    assert cfg.output_dir == Path("somewhere")


def test_config_from_empty_env_uses_defaults() -> None:
    cfg = DailyOrdersConfig.from_env({})
    assert (cfg.order_count, cfg.dirty_ratio) == (100_000, 0.03)


def test_job_is_discovered_and_runs_via_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "out"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", f"sqlite:///{tmp_path / 'etl.db'}")
    monkeypatch.setenv("TASK_PLATFORM_ETL_ORDER_COUNT", "500")
    monkeypatch.setenv("TASK_PLATFORM_ETL_OUTPUT_DIR", str(out))

    assert main(["run", "etl-daily_orders"]) == 0

    [run_dir] = list(out.iterdir())
    assert run_dir.name.startswith("run_")  # 目录以 run_id 命名
    assert {p.name for p in run_dir.iterdir()} == {
        "daily_report.csv",
        "quality_report.json",
        "quarantine.jsonl",
    }

    capsys.readouterr()
    assert main(["logs", run_dir.name]) == 0  # ETL 日志已写入 job_logs
    assert "daily orders ETL finished" in capsys.readouterr().out

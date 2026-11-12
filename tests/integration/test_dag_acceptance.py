"""最终验收（蓝图第 20 节第 11 项）：5 节点 ETL DAG，节点失败时下游行为符合预期。
走真实 CLI 入口：
登记 configs/workflows/daily_orders_dag.json -> 运行 -> 失败 -> 修复 -> 重试 -> 重跑。
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from task_platform.cli.main import main
from task_platform.jobs.etl_dag_jobs import FAIL_STAGE_ENV

CONFIG = Path(__file__).resolve().parents[2] / "configs" / "workflows" / "daily_orders_dag.json"
NODES = ["extract", "clean", "validate", "aggregate", "report"]


@pytest.fixture(autouse=True)
def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", f"sqlite:///{tmp_path / 'dag.db'}")
    monkeypatch.setenv("TASK_PLATFORM_ETL_ORDER_COUNT", "1000")
    monkeypatch.setenv("TASK_PLATFORM_ETL_DIRTY_RATIO", "0.05")
    monkeypatch.setenv("TASK_PLATFORM_ETL_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.delenv(FAIL_STAGE_ENV, raising=False)
    assert main(["workflow", "register", str(CONFIG)]) == 0


def _statuses(out: str) -> dict[str, str]:
    return {m[1]: m[2] for m in re.finditer(r"^  (\w+)\t(\w+)\tattempts=", out, re.M)}


def _reused(out: str) -> set[str]:
    return {m[1] for m in re.finditer(r"^  (\w+)\t.*\(reused\)$", out, re.M)}


def _run_id(out: str) -> str:
    match = re.search(r"workflow_run_id=(\S+)", out)
    assert match is not None, out
    return match.group(1)


def test_dag_succeeds_and_produces_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    capsys.readouterr()

    assert main(["workflow", "run", "daily-orders-dag"]) == 0

    out = capsys.readouterr().out
    assert "status=SUCCESS" in out
    assert _statuses(out) == dict.fromkeys(NODES, "SUCCESS")
    dag = tmp_path / "out" / "dag"
    assert {"daily_report.csv", "summary.json", "quality_report.json", "quarantine.jsonl"} <= {
        p.name for p in dag.iterdir()
    }
    summary = json.loads((dag / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_rows"] == 1000 and summary["quarantined_rows"] > 0


@pytest.mark.parametrize("failing", NODES)
def test_node_failure_skips_exactly_the_downstream(
    failing: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(FAIL_STAGE_ENV, failing)
    capsys.readouterr()

    assert main(["workflow", "run", "daily-orders-dag"]) == 1

    out = capsys.readouterr().out
    index = NODES.index(failing)
    expected = {
        name: "SUCCESS" if i < index else "FAILED" if i == index else "SKIPPED"
        for i, name in enumerate(NODES)
    }
    assert _statuses(out) == expected
    assert ("status=FAILED" if index == 0 else "status=PARTIAL_FAILED") in out
    assert not (tmp_path / "out" / "dag" / "daily_report.csv").exists()


def test_fix_and_retry_then_rerun_a_single_node(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(FAIL_STAGE_ENV, "validate")
    capsys.readouterr()
    assert main(["workflow", "run", "daily-orders-dag"]) == 1
    first = _run_id(capsys.readouterr().out)

    # 修复故障后重试：抽取 / 清洗沿用，不再执行
    monkeypatch.delenv(FAIL_STAGE_ENV)
    assert main(["workflow", "retry", first]) == 0
    out = capsys.readouterr().out
    second = _run_id(out)
    assert f"parent={first}" in out
    assert _statuses(out) == dict.fromkeys(NODES, "SUCCESS")
    assert _reused(out) == {"extract", "clean"}
    assert (tmp_path / "out" / "dag" / "daily_report.csv").exists()

    # 单节点重跑：聚合及其下游重新执行，上游沿用
    assert main(["workflow", "rerun", second, "aggregate"]) == 0
    out = capsys.readouterr().out
    assert _reused(out) == {"extract", "clean", "validate"}

    assert main(["workflow", "history", "daily-orders-dag"]) == 0
    assert "total=3" in capsys.readouterr().out

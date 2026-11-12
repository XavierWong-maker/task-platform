from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from task_platform.cli.main import main


@pytest.fixture(autouse=True)
def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", f"sqlite:///{tmp_path / 'wf_cli.db'}")
    monkeypatch.delenv("TASK_PLATFORM_FAULT_FAIL_TIMES", raising=False)


def _write(tmp_path: Path, name: str, nodes: list[dict[str, Any]]) -> str:
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps({"name": name, "nodes": nodes}), encoding="utf-8")
    return str(path)


def _run_id(out: str) -> str:
    match = re.search(r"workflow_run_id=(\S+)", out)
    assert match is not None, out
    return match.group(1)


def test_register_run_show_history_and_rerun(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(
        tmp_path,
        "flow",
        [
            {"name": "ok", "job": "demo-success"},
            {"name": "flaky", "job": "fault-flaky", "depends_on": ["ok"]},
        ],
    )
    assert main(["workflow", "register", path]) == 0
    assert main(["workflow", "list"]) == 0
    assert "flow" in capsys.readouterr().out

    assert main(["workflow", "run", "flow"]) == 0
    out = capsys.readouterr().out
    run_id = _run_id(out)
    assert "status=SUCCESS" in out
    assert re.search(r"flaky\s+SUCCESS\s+attempts=2", out)  # 注入失败一次后重试成功

    assert main(["workflow", "show", run_id]) == 0
    assert re.search(r"flaky\s+SUCCESS\s+attempts=2", capsys.readouterr().out)

    assert main(["workflow", "rerun", run_id, "flaky"]) == 0
    rerun_out = capsys.readouterr().out
    assert f"parent={run_id}" in rerun_out
    assert re.search(r"ok\s+SUCCESS\s+attempts=1\s+run_id=\S+ \(reused\)", rerun_out)

    assert main(["workflow", "history", "flow"]) == 0
    assert "total=2" in capsys.readouterr().out


def test_failing_workflow_then_retry_creates_child_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(
        tmp_path,
        "flow",
        [
            {"name": "ok", "job": "demo-success"},
            {"name": "bad", "job": "demo-failure", "depends_on": ["ok"]},
            {"name": "after", "job": "demo-success", "depends_on": ["bad"]},
        ],
    )
    assert main(["workflow", "register", path]) == 0
    capsys.readouterr()

    assert main(["workflow", "run", "flow"]) == 1
    first = _run_id(capsys.readouterr().out)

    assert main(["workflow", "retry", first]) == 1  # bad 仍然失败
    out = capsys.readouterr().out
    assert f"parent={first}" in out
    assert re.search(r"ok\s+SUCCESS\s+attempts=1\s+run_id=\S+ \(reused\)", out)
    assert re.search(r"after\s+SKIPPED", out)


def test_error_paths_return_one(tmp_path: Path) -> None:
    assert main(["workflow", "run", "nope"]) == 1
    assert main(["workflow", "show", "wfrun_nope"]) == 1
    assert main(["workflow", "retry", "wfrun_nope"]) == 1
    assert main(["workflow", "register", str(tmp_path / "missing.json")]) == 1

    unknown_job = _write(tmp_path, "x", [{"name": "a", "job": "ghost"}])
    assert main(["workflow", "register", unknown_job]) == 1

    cyclic = _write(
        tmp_path,
        "c",
        [
            {"name": "a", "job": "demo-success", "depends_on": ["b"]},
            {"name": "b", "job": "demo-success", "depends_on": ["a"]},
        ],
    )
    assert main(["workflow", "register", cyclic]) == 1

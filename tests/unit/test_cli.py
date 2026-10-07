from __future__ import annotations

from pathlib import Path

import pytest

from task_platform.cli.main import main


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", f"sqlite:///{tmp_path / 'cli_unit_test.db'}")


def test_run_success_returns_zero() -> None:
    assert main(["run", "demo-success"]) == 0


def test_run_failure_returns_nonzero() -> None:
    # demo-failure 第一次尝试会进入 RETRYING，同样不是 SUCCESS
    assert main(["run", "demo-failure"]) == 1

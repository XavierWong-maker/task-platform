from __future__ import annotations

import threading
from pathlib import Path

import pytest

from task_platform.bootstrap import build_registry
from task_platform.faults import FlakyState
from task_platform.jobs.context import build_context


def test_flaky_state_fails_first_n_calls_per_run() -> None:
    state = FlakyState()
    assert [state.should_fail("run_a", 2) for _ in range(4)] == [True, True, False, False]
    assert state.should_fail("run_b", 2) is True  # 不同 run 互不影响


def test_flaky_state_is_thread_safe() -> None:
    state = FlakyState()
    results: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        for _ in range(5):
            r = state.should_fail("run_x", 3)
            with lock:
                results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 40
    assert results.count(True) == 3  # 并发下也恰好失败 3 次


def test_fault_jobs_are_discovered_with_expected_metadata() -> None:
    registry = build_registry()
    for name in ("fault.flaky", "fault.slow", "fault.hang", "fault.ledger"):
        assert registry.is_registered(name)
    hang = registry.get("fault.hang")
    assert hang.timeout_seconds == 2.0
    flaky = registry.get("fault.flaky")
    assert flaky.retry_policy is not None and flaky.retry_policy.max_attempts == 3


def test_ledger_job_appends_one_line_per_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from task_platform.jobs.fault_jobs import fault_ledger

    ledger = tmp_path / "nested" / "ledger.txt"
    monkeypatch.setenv("TASK_PLATFORM_FAULT_LEDGER", str(ledger))
    ctx = build_context("run_1")
    fault_ledger(ctx)
    fault_ledger(ctx)  # 没有幂等保护时，同一次运行重复调用就会重复写入

    assert ledger.read_text(encoding="utf-8").splitlines() == ["run_1", "run_1"]

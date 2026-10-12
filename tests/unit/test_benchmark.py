from __future__ import annotations

import pytest

from task_platform.etl.benchmark import MODES, benchmark, main


def test_all_modes_produce_identical_results() -> None:
    results = {m: benchmark(m, 1000, 0.05, measure_memory=False) for m in MODES}
    assert len({(r.total_rows, r.quarantined_rows) for r in results.values()}) == 1
    assert all(r.peak_mb is None for r in results.values())


def test_memory_measurement_reports_positive_peak() -> None:
    result = benchmark("streaming", 500, 0.0, measure_memory=True)
    assert result.peak_mb is not None and result.peak_mb > 0


def test_cli_main_runs(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--count", "300", "--mode", "streaming"]) == 0
    assert "total=300" in capsys.readouterr().out

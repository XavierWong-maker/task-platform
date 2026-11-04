from __future__ import annotations

from datetime import UTC, datetime

from task_platform.domain.enums import JobStatus
from task_platform.metrics import JobMetrics, MetricsSnapshot
from task_platform.prometheus import render_prometheus

NOW = datetime(2026, 11, 2, tzinfo=UTC)


def _counts(**overrides: int) -> dict[str, int]:
    counts = {s.value: 0 for s in JobStatus}
    counts.update(overrides)
    return counts


def _snapshot(
    job: str = "job-a", depth: int | None = 5, pending: int | None = 2
) -> MetricsSnapshot:
    metrics = JobMetrics(job, _counts(SUCCESS=3, FAILED=1, RUNNING=2), 1, 4, 112.0, 100.0)
    return MetricsSnapshot(NOW, None, [metrics], depth, pending)


def test_renders_counters_gauges_summary_and_queue() -> None:
    text = render_prometheus(_snapshot(), queue_name="q")

    assert text.endswith("\n")
    assert "# TYPE job_execution_total counter" in text
    assert 'job_execution_total{job="job-a",status="SUCCESS"} 3' in text
    assert 'job_execution_total{job="job-a",status="FAILED"} 1' in text
    assert 'job_executions_active{job="job-a",status="RUNNING"} 2' in text
    assert 'job_execution_total{job="job-a",status="RUNNING"}' not in text  # 进行中不是 counter
    assert 'job_retry_total{job="job-a"} 1' in text
    assert 'job_duration_seconds_sum{job="job-a"} 112.000000' in text
    assert 'job_duration_seconds_count{job="job-a"} 4' in text
    assert 'job_duration_seconds_max{job="job-a"} 100.000000' in text
    assert 'queue_depth{queue="q"} 5' in text
    assert 'queue_pending{queue="q"} 2' in text


def test_queue_metrics_are_omitted_when_unavailable() -> None:
    text = render_prometheus(_snapshot(depth=None, pending=None), queue_name="q")
    assert "queue_depth" not in text and "queue_pending" not in text


def test_label_values_are_escaped() -> None:
    text = render_prometheus(_snapshot(job='a"b\\c'), queue_name="q")
    assert 'job_retry_total{job="a\\"b\\\\c"} 1' in text


def test_empty_snapshot_still_renders_headers() -> None:
    text = render_prometheus(MetricsSnapshot(NOW, None, [], None, None), queue_name="q")
    assert "# TYPE job_execution_total counter" in text
    assert "job_retry_total{" not in text

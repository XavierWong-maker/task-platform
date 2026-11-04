"""把 MetricsSnapshot 渲染为 Prometheus 文本暴露格式（0.0.4）。

传入的快照应是全量（不带窗口）：窗口过滤后的值不再单调递增，不适合标成 counter。
- job_execution_total   counter  终态：SUCCESS / FAILED / CANCELED / RETRYING 的尝试数
- job_executions_active gauge    进行中：PENDING / QUEUED / RUNNING / RECOVERABLE（会下降）
"""

from __future__ import annotations

from task_platform.domain.enums import JobStatus
from task_platform.metrics import MetricsSnapshot

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

_COUNTER_STATUSES = (
    JobStatus.SUCCESS,
    JobStatus.FAILED,
    JobStatus.CANCELED,
    JobStatus.RETRYING,
)
_ACTIVE_STATUSES = tuple(s for s in JobStatus if s not in _COUNTER_STATUSES)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(**labels: str) -> str:
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in labels.items()) + "}"


def render_prometheus(snapshot: MetricsSnapshot, *, queue_name: str) -> str:
    lines: list[str] = []

    def header(name: str, help_text: str, metric_type: str) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")

    header("job_execution_total", "Execution attempts by terminal status.", "counter")
    for j in snapshot.jobs:
        for status in _COUNTER_STATUSES:
            lines.append(
                f"job_execution_total{_labels(job=j.job, status=status.value)} "
                f"{j.status_counts[status.value]}"
            )

    header("job_executions_active", "Execution attempts not yet finished.", "gauge")
    for j in snapshot.jobs:
        for status in _ACTIVE_STATUSES:
            lines.append(
                f"job_executions_active{_labels(job=j.job, status=status.value)} "
                f"{j.status_counts[status.value]}"
            )

    header("job_retry_total", "Retry attempts actually executed (attempt > 1).", "counter")
    for j in snapshot.jobs:
        lines.append(f"job_retry_total{_labels(job=j.job)} {j.retry_attempts}")

    header("job_duration_seconds", "Duration of finished (SUCCESS/FAILED) attempts.", "summary")
    for j in snapshot.jobs:
        lines.append(f"job_duration_seconds_sum{_labels(job=j.job)} {j.duration_sum:.6f}")
        lines.append(f"job_duration_seconds_count{_labels(job=j.job)} {j.duration_count}")

    header("job_duration_seconds_max", "Longest finished attempt.", "gauge")
    for j in snapshot.jobs:
        lines.append(f"job_duration_seconds_max{_labels(job=j.job)} {j.duration_max:.6f}")

    if snapshot.queue_depth is not None and snapshot.queue_pending is not None:
        header("queue_depth", "Messages not yet consumed.", "gauge")
        lines.append(f"queue_depth{_labels(queue=queue_name)} {snapshot.queue_depth}")
        header("queue_pending", "Messages consumed but not yet acked.", "gauge")
        lines.append(f"queue_pending{_labels(queue=queue_name)} {snapshot.queue_pending}")

    return "\n".join(lines) + "\n"

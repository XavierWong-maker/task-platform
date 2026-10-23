"""故障注入任务：被 discover_jobs 自动发现。

可选环境变量（环境变量 > 默认值）：
- TASK_PLATFORM_FAULT_FAIL_TIMES   fault.flaky 每次运行失败的次数，默认 1
- TASK_PLATFORM_FAULT_DELAY_SECONDS fault.slow 的延迟秒数，默认 3
- TASK_PLATFORM_FAULT_LEDGER       fault.ledger 的台账文件，默认 data/output/fault_ledger.txt
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from task_platform.faults import FlakyState
from task_platform.jobs.context import JobContext
from task_platform.jobs.decorators import job

_FLAKY = FlakyState()


@job(
    "fault.flaky",
    description="故障注入：每次运行前 N 次尝试失败，之后成功（验证重试链路）",
    timeout_seconds=30.0,
    max_attempts=3,
    base_delay_seconds=0.5,
)
def fault_flaky(ctx: JobContext) -> str:
    fail_times = int(os.environ.get("TASK_PLATFORM_FAULT_FAIL_TIMES", "1"))
    if _FLAKY.should_fail(ctx.execution_id, fail_times):
        raise RuntimeError("injected failure")
    return "ok after injected failures"


@job(
    "fault.slow",
    description="故障注入：配合取消的延迟任务（验证延迟与取消）",
    timeout_seconds=30.0,
    max_attempts=1,
)
def fault_slow(ctx: JobContext) -> str:
    delay = float(os.environ.get("TASK_PLATFORM_FAULT_DELAY_SECONDS", "3"))
    ctx.cancel.wait(delay)
    ctx.check_cancelled()
    return f"delayed {delay}s"


@job(
    "fault.hang",
    description="故障注入：不响应取消的挂起任务（验证超时与宽限期；线程会残留约 10 秒）",
    timeout_seconds=2.0,
    max_attempts=2,
    base_delay_seconds=0.5,
)
def fault_hang() -> str:
    time.sleep(10)  # 故意不检查取消令牌
    return "finally woke up"


@job(
    "fault.ledger",
    description="故障注入：非幂等副作用，每次执行追加一行台账（验证重复执行）",
    timeout_seconds=30.0,
    max_attempts=1,
)
def fault_ledger(ctx: JobContext) -> str:
    path = Path(os.environ.get("TASK_PLATFORM_FAULT_LEDGER", "data/output/fault_ledger.txt"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{ctx.execution_id}\n")
    return "appended"

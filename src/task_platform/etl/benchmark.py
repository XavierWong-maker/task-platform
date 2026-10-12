"""ETL 规模验证：测耗时与内存峰值，并对比"流式"和"一次性装入 list"。

用法：
    python -m task_platform.etl.benchmark --count 1000000 --mode streaming
    python -m task_platform.etl.benchmark --count 1000000 --mode streaming --memory
    python -m task_platform.etl.benchmark --count 1000000 --mode materialized --memory

注意：--memory 使用 tracemalloc，会让程序明显变慢，带它测出的耗时不具参考价值。
耗时和内存请分开各跑一次。
"""

from __future__ import annotations

import argparse
import tempfile
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from task_platform.etl.daily_orders import DailyOrdersConfig, run_daily_orders_etl
from task_platform.etl.factory import TestDataFactory
from task_platform.etl.pipeline import run_orders_pipeline

MODES = ("streaming", "materialized", "etl")


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    mode: str
    count: int
    total_rows: int
    quarantined_rows: int
    elapsed_seconds: float
    peak_mb: float | None  # 未开启 --memory 时为 None


def _factory(cfg: DailyOrdersConfig) -> TestDataFactory:
    return TestDataFactory(
        cfg.seed,
        user_pool_size=cfg.user_pool_size,
        product_pool_size=cfg.product_pool_size,
        now=cfg.now,
    )


def _streaming(cfg: DailyOrdersConfig) -> tuple[int, int]:
    f = _factory(cfg)
    result = run_orders_pipeline(
        f.generate_orders(cfg.order_count, dirty_ratio=cfg.dirty_ratio),
        f.generate_users(),
        now=cfg.now,
    )
    return result.quality.total_rows, result.quality.quarantined_rows


def _materialized(cfg: DailyOrdersConfig) -> tuple[int, int]:
    f = _factory(cfg)
    orders = list(f.generate_orders(cfg.order_count, dirty_ratio=cfg.dirty_ratio))  # 全部进内存
    result = run_orders_pipeline(orders, f.generate_users(), now=cfg.now)
    return result.quality.total_rows, result.quality.quarantined_rows


def _etl(cfg: DailyOrdersConfig) -> tuple[int, int]:
    with tempfile.TemporaryDirectory() as tmp:
        summary = run_daily_orders_etl(cfg, Path(tmp) / "run")
    return summary.total_rows, summary.quarantined_rows


_RUNNERS: dict[str, Callable[[DailyOrdersConfig], tuple[int, int]]] = {
    "streaming": _streaming,
    "materialized": _materialized,
    "etl": _etl,
}


def benchmark(
    mode: str, count: int, dirty_ratio: float, *, measure_memory: bool
) -> BenchmarkResult:
    cfg = DailyOrdersConfig(order_count=count, dirty_ratio=dirty_ratio)
    runner = _RUNNERS[mode]

    if measure_memory:
        tracemalloc.start()
    try:
        start = time.perf_counter()
        total, quarantined = runner(cfg)
        elapsed = time.perf_counter() - start
        peak_mb = tracemalloc.get_traced_memory()[1] / 1024 / 1024 if measure_memory else None
    finally:
        if measure_memory:
            tracemalloc.stop()

    return BenchmarkResult(mode, count, total, quarantined, elapsed, peak_mb)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="etl-benchmark", description="ETL 规模验证")
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--dirty-ratio", type=float, default=0.03)
    parser.add_argument("--mode", choices=MODES, default="streaming")
    parser.add_argument("--memory", action="store_true", help="测内存峰值（会显著变慢）")
    args = parser.parse_args(argv)

    r = benchmark(args.mode, args.count, args.dirty_ratio, measure_memory=args.memory)
    print(f"mode={r.mode} count={r.count} total={r.total_rows} quarantined={r.quarantined_rows}")
    if r.peak_mb is None:
        print(
            f"elapsed={r.elapsed_seconds:.2f}s throughput={r.count / r.elapsed_seconds:,.0f} rows/s"
        )
    else:
        print(f"peak_memory={r.peak_mb:.1f} MB (tracemalloc; elapsed 不具参考价值)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""FastAPI 应用工厂

启动：uvicorn task_platform.api.app:create_app --factory --reload
设置 TASK_PLATFORM_METRICS_QUEUE=1 时，指标接口会连接 Redis 队列并报告队列长度。
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI

from task_platform.alerts import AlertRules
from task_platform.api.errors import register_error_handlers
from task_platform.api.routers import alerts, executions, health, jobs, metrics, workflows
from task_platform.bootstrap import build_registry, sync_jobs_to_db
from task_platform.infrastructure.db.migrate import upgrade_db
from task_platform.infrastructure.db.session import (
    create_db_engine,
    get_database_url,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.logging.collector import collecting_logs
from task_platform.infrastructure.logging.structured import structured_logging
from task_platform.infrastructure.queue.base import TaskQueue
from task_platform.infrastructure.queue.factory import build_queue

logger = logging.getLogger("task_platform.api")


def _build_metrics_queue() -> TaskQueue | None:
    """仅在显式开启时连接队列；连不上只记日志，指标接口照常工作（队列指标为空）"""
    if os.environ.get("TASK_PLATFORM_METRICS_QUEUE", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        return None
    # noinspection PyBroadException
    try:
        return build_queue()
    except Exception:
        logger.exception("cannot connect to queue, queue metrics disabled")
        return None


def create_app(
    database_url: str | None = None,
    queue: TaskQueue | None = None,
    alert_rules: AlertRules | None = None,
) -> FastAPI:
    url = database_url or get_database_url()
    rules = (
        alert_rules if alert_rules is not None else AlertRules.from_env()
    )  # 阈值非法则启动即报错

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        upgrade_db(url)
        engine = create_db_engine(url)
        factory = make_session_factory(engine)
        registry = build_registry()
        with session_scope(factory) as s:
            sync_jobs_to_db(registry, s)

        # 日志收集器与线程池是进程级共享资源：生命周期与应用一致
        with structured_logging("api"), collecting_logs() as collector:
            pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="api-run")
            _app.state.session_factory = factory
            _app.state.registry = registry
            _app.state.log_collector = collector
            _app.state.executor_pool = pool
            _app.state.queue = queue if queue is not None else _build_metrics_queue()
            _app.state.alert_rules = rules
            try:
                yield
            finally:
                pool.shutdown()
                engine.dispose()

    app = FastAPI(title="Task Platform API", version="0.1.0", lifespan=lifespan)
    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(executions.router)
    app.include_router(metrics.router)
    app.include_router(metrics.prometheus_router)
    app.include_router(alerts.router)
    app.include_router(workflows.router)
    return app

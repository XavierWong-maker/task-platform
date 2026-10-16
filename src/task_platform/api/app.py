"""FastAPI 应用工厂

启动：uvicorn task_platform.api.app:create_app --factory --reload
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI

from task_platform.api.errors import register_error_handlers
from task_platform.api.routers import executions, health, jobs
from task_platform.bootstrap import build_registry, sync_jobs_to_db
from task_platform.infrastructure.db.migrate import upgrade_db
from task_platform.infrastructure.db.session import (
    create_db_engine,
    get_database_url,
    make_session_factory,
    session_scope,
)
from task_platform.infrastructure.logging.collector import collecting_logs


def create_app(database_url: str | None = None) -> FastAPI:
    url = database_url or get_database_url()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        upgrade_db(url)
        engine = create_db_engine(url)
        factory = make_session_factory(engine)
        registry = build_registry()
        with session_scope(factory) as s:
            sync_jobs_to_db(registry, s)

        # 日志收集器与线程池是进程级共享资源：生命周期与应用一致
        with collecting_logs() as collector:
            pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="api-run")
            _app.state.session_factory = factory
            _app.state.registry = registry
            _app.state.log_collector = collector
            _app.state.executor_pool = pool
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
    return app

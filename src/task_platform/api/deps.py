"""API 依赖：把 app.state 里的共享资源以 Depends 方式提供给路由"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

from fastapi import Depends, Request
from sqlalchemy.orm import Session, sessionmaker

from task_platform.infrastructure.db.session import session_scope
from task_platform.infrastructure.logging.collector import LogCollector
from task_platform.registry import TaskRegistry


def get_session_factory(request: Request) -> sessionmaker[Session]:
    factory: sessionmaker[Session] = request.app.state.session_factory
    return factory


def get_session(factory: sessionmaker[Session] = Depends(get_session_factory)) -> Iterator[Session]:
    """每个请求一个事务：正常结束提交，异常回滚（复用 session_scope）"""
    with session_scope(factory) as session:
        yield session


def get_registry(request: Request) -> TaskRegistry:
    registry: TaskRegistry = request.app.state.registry
    return registry


def get_executor_pool(request: Request) -> ThreadPoolExecutor:
    pool: ThreadPoolExecutor = request.app.state.executor_pool
    return pool


def get_log_collector(request: Request) -> LogCollector:
    collector: LogCollector = request.app.state.log_collector
    return collector

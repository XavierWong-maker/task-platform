"""Engine / Session 工厂与事务边界"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from task_platform.infrastructure.db.base import Base

DEFAULT_DATABASE_URL = "sqlite:///data/task_platform.db"


def get_database_url() -> str:
    """配置优先级：环境变量 > 默认值（对应蓝图的配置原则）"""
    return os.environ.get("TASK_PLATFORM_DB_URL", DEFAULT_DATABASE_URL)


def create_db_engine(url: str | None = None, *, echo: bool = False) -> Engine:
    engine = create_engine(url or get_database_url(), echo=echo)
    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def _enable_sqlite_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
            # SQLite 默认不校验外键，必须每个连接显式开启
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


def init_db(engine: Engine) -> None:
    """按模型建表。临时方案：后续由 Alembic 迁移取代"""
    from task_platform.infrastructure.db import models  # noqa: F401  # 确保模型已注册到 metadata

    Base.metadata.create_all(engine)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    """事务边界：正常退出则提交，异常则回滚并继续抛出"""
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

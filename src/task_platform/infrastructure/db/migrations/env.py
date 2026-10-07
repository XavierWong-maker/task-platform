"""Alembic 环境

URL 优先取 config 里的 sqlalchemy.url（程序化调用时传入）
否则回退到 get_database_url()（环境变量 > 默认值，与应用保持一致）
"""

from __future__ import annotations

from logging.config import fileConfig
from typing import Any, Literal

from alembic import context
from alembic.autogenerate.api import AutogenContext

from task_platform.infrastructure.db import models  # noqa: F401  # 确保模型注册到 metadata
from task_platform.infrastructure.db.base import Base, UTCDateTime
from task_platform.infrastructure.db.session import (
    create_db_engine,
    get_database_url,
)

config = context.config
if config.config_file_name is not None:
    # disable_existing_loggers=False：不要把应用自己的 logger 关掉
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata


def _url() -> str:
    return config.get_main_option("sqlalchemy.url") or get_database_url()


def render_item(type_: str, obj: Any, autogen_context: AutogenContext) -> str | Literal[False]:
    """autogenerate 时把 UTCDateTime 渲染成 sa.DateTime()，
    让迁移脚本不依赖应用代码（底层存储本来就是 DateTime）
    """
    if type_ == "type" and isinstance(obj, UTCDateTime):
        return "sa.DateTime()"
    return False


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        render_as_batch=True,
        render_item=render_item,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_db_engine(_url())  # 复用应用的 engine 工厂：建父目录 + SQLite 外键 PRAGMA
    try:
        with engine.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                render_as_batch=True,
                render_item=render_item,
            )
            with context.begin_transaction():
                context.run_migrations()
            connection.commit()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import inspect

from task_platform.cli.main import main
from task_platform.infrastructure.db import models  # noqa: F401
from task_platform.infrastructure.db.base import Base
from task_platform.infrastructure.db.migrate import downgrade_db, upgrade_db
from task_platform.infrastructure.db.session import create_db_engine

APP_TABLES = {"jobs", "schedules", "job_executions", "job_logs", "idempotency_keys"}


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'migrate_test.db'}"


def _table_names(url: str) -> set[str]:
    engine = create_db_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_upgrade_creates_all_tables(db_url: str) -> None:
    upgrade_db(db_url)
    assert _table_names(db_url) == APP_TABLES | {"alembic_version"}


def test_upgrade_is_idempotent(db_url: str) -> None:
    upgrade_db(db_url)
    upgrade_db(db_url)  # 已在 head，再升级一次应是空操作而不是报"表已存在"
    assert _table_names(db_url) == APP_TABLES | {"alembic_version"}


def test_downgrade_to_base_drops_app_tables(db_url: str) -> None:
    upgrade_db(db_url)
    downgrade_db(db_url)
    assert _table_names(db_url) == {"alembic_version"}


def test_migrated_schema_matches_orm_models(db_url: str) -> None:
    """手写迁移最大的风险是和 ORM 模型悄悄漂移：
    迁移出来的库与 Base.metadata 对比，不应有任何差异。
    （compare_type 关闭：Enum(native_enum=False) 与 VARCHAR 的类型比较容易产生
    与真实漂移无关的噪音；表、列、可空性、索引、外键、唯一约束仍会被比较）
    """
    upgrade_db(db_url)

    def include_object(
        obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
    ) -> bool:
        return not (type_ == "table" and name == "alembic_version")

    engine = create_db_engine(db_url)
    try:
        with engine.connect() as connection:
            ctx = MigrationContext.configure(
                connection, opts={"compare_type": False, "include_object": include_object}
            )
            diffs = compare_metadata(ctx, Base.metadata)
    finally:
        engine.dispose()

    assert diffs == []


def test_cli_bootstraps_schema_via_migrations(db_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", db_url)
    assert main(["list"]) == 0
    assert _table_names(db_url) == APP_TABLES | {"alembic_version"}

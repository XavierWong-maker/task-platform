"""程序化执行迁移：应用启动时调用 upgrade_db()，取代早期的 create_all"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from task_platform.infrastructure.db.session import get_database_url

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def make_alembic_config(url: str | None = None) -> Config:
    config = Config()  # 不依赖 alembic.ini，路径都从包位置推导
    config.set_main_option("script_location", str(_MIGRATIONS_DIR))
    # configparser 会对 % 做插值，URL 里若含 % 需要转义
    config.set_main_option("sqlalchemy.url", (url or get_database_url()).replace("%", "%%"))
    return config


def upgrade_db(url: str | None = None, revision: str = "head") -> None:
    command.upgrade(make_alembic_config(url), revision)


def downgrade_db(url: str | None = None, revision: str = "base") -> None:
    command.downgrade(make_alembic_config(url), revision)

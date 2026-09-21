"""SQLAlchemy 声明式基类与自定义列类型"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator


class Base(DeclarativeBase):
    """所有 ORM 模型的基类"""


class UTCDateTime(TypeDecorator[datetime]):
    """存入时要求带时区并转成 UTC；读出时还原为带 UTC 时区的 datetime。

    SQLite 不保存 tzinfo，领域层使用的是 aware datetime，
    这里统一转换，避免读回来变成 naive 导致比较报错。
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("UTCDateTime 只接受带时区的 datetime")
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC)

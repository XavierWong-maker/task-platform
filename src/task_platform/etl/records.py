"""ETL 原始记录（raw records）。

这些对象代表"刚从数据源读出来、尚未校验"的数据，所以字段允许缺失
（None）或取值非法（如未知的 status）——校验是下一步质量规则的职责，
不是在构造对象时做。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

VALID_ORDER_STATUSES: tuple[str, ...] = ("PAID", "CANCELED", "PENDING")


class DirtyKind(StrEnum):
    """DataFactory 故意注入的脏数据类型"""

    MISSING_ID = "MISSING_ID"  # 非空
    DUPLICATE_ID = "DUPLICATE_ID"  # 唯一
    BAD_QUANTITY = "BAD_QUANTITY"  # 范围：quantity > 0
    NEGATIVE_PRICE = "NEGATIVE_PRICE"  # 范围：金额 >= 0
    BAD_STATUS = "BAD_STATUS"  # 枚举
    ORPHAN_USER = "ORPHAN_USER"  # 关联
    FUTURE_TIME = "FUTURE_TIME"  # 时间
    MISSING_TIME = "MISSING_TIME"  # 非空


@dataclass(frozen=True, slots=True)
class RawUser:
    user_id: str
    region: str
    register_date: date
    member_level: str


@dataclass(frozen=True, slots=True)
class RawProduct:
    product_id: str
    category: str
    price: Decimal
    cost: Decimal
    supplier: str


@dataclass(frozen=True, slots=True)
class RawOrder:
    order_id: str | None
    user_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    discount: Decimal
    status: str
    created_at: datetime | None

"""TestDataFactory：可重复的模拟数据生成器。

- 同一个 seed + 同样参数 => 完全相同的数据（每次调用都新建独立的 Random）
- 不读取真实时间：参考时间 `now` 通过构造函数注入
- generate_orders 是 generator：按需产出，支持百万级而不占用大量内存
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from task_platform.etl.records import (
    VALID_ORDER_STATUSES,
    DirtyKind,
    RawOrder,
    RawProduct,
    RawUser,
)

DEFAULT_NOW = datetime(2026, 8, 17, tzinfo=UTC)

_REGIONS = ("NORTH", "SOUTH", "EAST", "WEST", "CENTRAL")
_MEMBER_LEVELS = ("BRONZE", "SILVER", "GOLD", "PLATINUM")
_CATEGORIES = ("ELECTRONICS", "HOME", "BOOKS", "APPAREL", "FOOD")
_SUPPLIERS = tuple(f"SUP{i:02d}" for i in range(1, 9))
_DISCOUNTS = (Decimal("0.00"), Decimal("0.05"), Decimal("0.10"), Decimal("0.20"))
_DIRTY_KINDS = tuple(DirtyKind)
_ORPHAN_USER_ID = "U999999"
_CENT = Decimal("0.01")


def _money(cents: int) -> Decimal:
    return Decimal(cents).scaleb(-2).quantize(_CENT)


class TestDataFactory:
    __test__ = False

    def __init__(
        self,
        seed: int,
        *,
        user_pool_size: int = 1000,
        product_pool_size: int = 200,
        now: datetime = DEFAULT_NOW,
    ) -> None:
        if not 0 < user_pool_size < 999_999:
            raise ValueError("user_pool_size must be in (0, 999999)")
        if product_pool_size < 1:
            raise ValueError("product_pool_size must be >= 1")
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        self._seed = seed
        self._user_pool_size = user_pool_size
        self._product_pool_size = product_pool_size
        self._now = now

    def _rng(self, domain: str) -> random.Random:
        # str 种子的哈希是确定的（不同于内置 hash()），跨进程/平台可复现
        return random.Random(f"{self._seed}:{domain}")

    # ---------------- 维度数据：用户 / 商品 ----------------

    def generate_users(self) -> Iterator[RawUser]:
        """用户池：数量固定为 user_pool_size，ID 为 U000001 ..."""
        rng = self._rng("users")
        for i in range(1, self._user_pool_size + 1):
            yield RawUser(
                user_id=f"U{i:06d}",
                region=rng.choice(_REGIONS),
                register_date=(self._now - timedelta(days=rng.randint(0, 730))).date(),
                member_level=rng.choice(_MEMBER_LEVELS),
            )

    def generate_products(self) -> Iterator[RawProduct]:
        """商品池：数量固定为 product_pool_size，ID 为 P00001 ..."""
        rng = self._rng("products")
        for i in range(1, self._product_pool_size + 1):
            price = _money(rng.randint(500, 50_000))
            cost = (price * Decimal(rng.choice(("0.4", "0.5", "0.6", "0.7")))).quantize(_CENT)
            yield RawProduct(
                product_id=f"P{i:05d}",
                category=rng.choice(_CATEGORIES),
                price=price,
                cost=cost,
                supplier=rng.choice(_SUPPLIERS),
            )

    # ---------------- 事实数据：订单 ----------------

    def generate_orders(self, count: int, *, dirty_ratio: float = 0.0) -> Iterator[RawOrder]:
        for order, _ in self.generate_labeled_orders(count, dirty_ratio=dirty_ratio):
            yield order

    def generate_labeled_orders(
        self, count: int, *, dirty_ratio: float = 0.0
    ) -> Iterator[tuple[RawOrder, DirtyKind | None]]:
        """同时返回"标准答案"：该订单被注入的脏数据类型（干净则为 None）
        主要用于测试：质量规则的检出结果可以直接与标签对照
        """
        if count < 0:
            raise ValueError("count must be >= 0")
        if not 0.0 <= dirty_ratio <= 1.0:
            raise ValueError("dirty_ratio must be in [0, 1]")

        rng = self._rng("orders")
        last_emitted_id: str | None = None

        for i in range(1, count + 1):
            order_id: str | None = f"O{i:08d}"
            user_id = f"U{rng.randint(1, self._user_pool_size):06d}"
            product_id = f"P{rng.randint(1, self._product_pool_size):05d}"
            quantity = rng.randint(1, 10)
            unit_price = _money(rng.randint(500, 50_000))
            discount = rng.choice(_DISCOUNTS)
            status = rng.choice(VALID_ORDER_STATUSES)
            created_at: datetime | None = self._now - timedelta(seconds=rng.randint(0, 30 * 86_400))

            kind: DirtyKind | None = None
            if rng.random() < dirty_ratio:
                kind = rng.choice(_DIRTY_KINDS)
                # 第一批记录之前没有可重复的 ID，退化为"缺失 ID"
                if kind is DirtyKind.DUPLICATE_ID and last_emitted_id is None:
                    kind = DirtyKind.MISSING_ID

            if kind is DirtyKind.MISSING_ID:
                order_id = None
            elif kind is DirtyKind.DUPLICATE_ID:
                order_id = last_emitted_id
            elif kind is DirtyKind.BAD_QUANTITY:
                quantity = rng.choice((0, -1, -5))
            elif kind is DirtyKind.NEGATIVE_PRICE:
                unit_price = -unit_price
            elif kind is DirtyKind.BAD_STATUS:
                status = rng.choice(("UNKNOWN", "paid", "??"))
            elif kind is DirtyKind.ORPHAN_USER:
                user_id = _ORPHAN_USER_ID
            elif kind is DirtyKind.FUTURE_TIME:
                created_at = self._now + timedelta(days=rng.randint(1, 30))
            elif kind is DirtyKind.MISSING_TIME:
                created_at = None

            if order_id is not None:
                last_emitted_id = order_id

            yield (
                RawOrder(
                    order_id=order_id,
                    user_id=user_id,
                    product_id=product_id,
                    quantity=quantity,
                    unit_price=unit_price,
                    discount=discount,
                    status=status,
                    created_at=created_at,
                ),
                kind,
            )

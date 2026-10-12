"""订单 ETL 流水线：validate -> transform -> aggregate

三个阶段都是 generator，数据逐条流过，不整批进内存。
隔离（quarantine）的去向由调用方通过 on_quarantine 回调决定。
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from task_platform.etl.quality import (
    QualityChecker,
    QualityReport,
    Violation,
    default_order_rules,
    is_quarantined,
)
from task_platform.etl.records import RawOrder, RawUser

_CENT = Decimal("0.01")
_ONE = Decimal("1")


@dataclass(frozen=True, slots=True)
class QuarantinedOrder:
    order: RawOrder
    violations: tuple[Violation, ...]


@dataclass(frozen=True, slots=True)
class CleanOrder:
    """通过校验并完成转换的订单：字段齐全，附带衍生字段 region / amount"""

    order_id: str
    user_id: str
    region: str
    order_date: date
    status: str
    amount: Decimal


@dataclass(frozen=True, slots=True)
class DailyRegionStat:
    order_date: date
    region: str
    order_count: int  # 所有通过校验的订单
    paid_count: int  # 其中状态为 PAID 的订单
    gmv: Decimal  # PAID 订单的金额合计

    @property
    def avg_order_value(self) -> Decimal:
        """客单价 = GMV / 已支付订单数；没有已支付订单时为 0"""
        if self.paid_count == 0:
            return Decimal("0.00")
        return (self.gmv / self.paid_count).quantize(_CENT, ROUND_HALF_UP)


@dataclass(frozen=True, slots=True)
class PipelineResult:
    quality: QualityReport
    stats: list[DailyRegionStat]


def validate_stream(
    orders: Iterable[RawOrder],
    checker: QualityChecker,
    on_quarantine: Callable[[QuarantinedOrder], None],
) -> Iterator[RawOrder]:
    """通过校验的订单继续往下流；有 ERROR 违规的交给 on_quarantine"""
    for order in orders:
        violations = checker.check(order)
        if is_quarantined(violations):
            on_quarantine(QuarantinedOrder(order, tuple(violations)))
        else:
            yield order


def transform(orders: Iterable[RawOrder], users: Mapping[str, RawUser]) -> Iterator[CleanOrder]:
    """计算金额、关联地区。输入必须是已通过 validate_stream 的订单"""
    for order in orders:
        if order.order_id is None or order.created_at is None:
            raise ValueError("transform expects validated orders")
        amount = (order.quantity * order.unit_price * (_ONE - order.discount)).quantize(
            _CENT, ROUND_HALF_UP
        )
        yield CleanOrder(
            order_id=order.order_id,
            user_id=order.user_id,
            region=users[order.user_id].region,
            order_date=order.created_at.date(),
            status=order.status,
            amount=amount,
        )


def aggregate(orders: Iterable[CleanOrder]) -> list[DailyRegionStat]:
    """按 (日期, 地区) 聚合；内存只与"日期数 × 地区数"有关，与订单量无关"""
    order_counts: Counter[tuple[date, str]] = Counter()
    paid_counts: Counter[tuple[date, str]] = Counter()
    gmv: defaultdict[tuple[date, str], Decimal] = defaultdict(Decimal)

    for order in orders:
        key = (order.order_date, order.region)
        order_counts[key] += 1
        if order.status == "PAID":
            paid_counts[key] += 1
            gmv[key] += order.amount

    return [
        DailyRegionStat(
            order_date=key[0],
            region=key[1],
            order_count=order_counts[key],
            paid_count=paid_counts[key],
            gmv=gmv[key],
        )
        for key in sorted(order_counts)
    ]


def run_orders_pipeline(
    orders: Iterable[RawOrder],
    users: Iterable[RawUser],
    *,
    now: datetime,
    on_quarantine: Callable[[QuarantinedOrder], None] | None = None,
) -> PipelineResult:
    """跑完整条流水线。质量报告必须在聚合消费完数据流之后再取"""
    user_map = {u.user_id: u for u in users}
    checker = QualityChecker(default_order_rules(user_map.keys(), now))

    validated = validate_stream(orders, checker, on_quarantine or (lambda _q: None))
    stats = aggregate(transform(validated, user_map))
    return PipelineResult(quality=checker.report(), stats=stats)
